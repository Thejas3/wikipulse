# WikiPulse

A real-time data pipeline that streams every edit made across Wikipedia through Kafka into a Bronze → Silver → Gold lakehouse, and serves live analytics on a public dashboard. The full plan is in the PRD; this README tracks what's built and the numbers measured so far.

## Status

| Milestone (PRD §12) | State |
|---|---|
| 1. Producer and Kafka | **Done.** 3.8 h continuous ingest, resume without gaps |
| 2. Bronze and Silver (Spark, Delta on S3) | **Done.** Kill-everything test: 0 lost, 0 duplicates |
| 3. Streaming Gold (windows, trending, spikes) | **Done.** p95 end-to-end latency ≤ 30 s (target < 60 s) |
| 4. dbt models and data quality | **Done.** `dbt build` 29/29 green, lineage docs, 2 GX suites |
| 5. Airflow and monitoring | Not started |
| 6. Public dashboard (GitHub Pages, DuckDB-WASM) | Not started; a live preview exists (below) |
| 7. Hardening and write-up | Not started |

## Measured so far

| What | Result | How |
|---|---|---|
| Live event rate | 27.7 events/sec average (all wikis) | `make audit` over 4.1 h, 409k events |
| Continuous ingest | 3.8 h, 12 dropped connections all resumed, 0 gaps over 10 s (largest 0.7 s) | `make audit` |
| Data loss, producer + Bronze + Silver SIGKILLed together | **0** missing Kafka offsets in Bronze, **0** settled edits missing from Silver | `make kill-spark`, then `make audit-lake` |
| Duplicates | **0** in Bronze (by offset) and Silver (by `meta.id`); 717 producer replays removed | `make audit-lake` |
| Recovery after restart | 30 s to the first new Bronze batch (target < 2 min) | restart to first progress log |
| Throughput headroom | 10–13.5k rows/sec Bronze catch-up, about 400× the live rate (target ≥ 5×) | catch-up batches of 50k |
| End-to-end latency (edit → visible in Gold) | p50 ≤ 25 s, **p95 ≤ 30 s**, p99 ≤ 35 s (target < 60 s p95) | `make latency`, 5-min live window |
| Gold exactly-once | 0 of 59,692 wiki-minutes differ from a Silver recount, across 2 OOM crashes mid-rebuild | `make audit-lake` |
| Late events | none dropped: Silver and Gold use no watermarks, after a watermark silently dropped ~161k out-of-order backfill edits ([incident](docs/incidents/2026-10-20-silver-late-drops.md)) | Bronze vs Silver hourly counts |
| Schema rejects on live data | 0 of 500 sampled; 6 canary heartbeats in 3.8 h (now skipped) | `contracts/` validation, DLQ |

## Architecture so far

```
Wikimedia SSE feed ─► producer ─► Kafka wiki.recentchange.raw ─► Spark bronze_job ─► bronze.recentchange (Delta)
                       │  schema check,       (+ .dlq for rejects)    every event unchanged,     │
                       │  resume ID                                   partitioned by ingest_date │
                       └─ Prometheus :9108                                                       ▼
                                                    silver.edits (Delta) ◄─ Spark silver_job: parse, Wikipedia
                                                    partitioned by event_date   edits only, dedup on meta.id
                                                                                within a 10-min watermark
Delta tables live in the S3 bucket s3a://lakehouse on SeaweedFS.
```

- **Producer → Kafka, at-least-once:** the resume point only advances after Kafka acknowledges, so crashes replay but never skip ([ADR 0001](docs/decisions/0001-kafka-between-feed-and-spark.md)).
- **Kafka → Bronze, exactly-once:** offsets live in the Spark checkpoint, and the Delta sink skips micro-batches it already committed.
- **Bronze → Silver, deduplicated:** `dropDuplicatesWithinWatermark` on `meta.id` removes producer replays, with RocksDB holding the dedup state.
- **Object store:** SeaweedFS instead of MinIO, because MinIO's images are no longer published ([ADR 0002](docs/decisions/0002-seaweedfs-instead-of-minio.md)).
- **Silver partitioning:** by `event_date` only, to avoid a small-file explosion ([ADR 0003](docs/decisions/0003-silver-partitioned-by-date-only.md)).

## Analytics layer (dbt + DuckDB)

dbt-duckdb reads the Delta tables in place (`delta_scan` over S3) and builds a star schema in `warehouse/wikipulse.duckdb`:

| Model | Grain |
|---|---|
| `fct_edits` (incremental) | one row per Wikipedia edit, with `vandalism_score` |
| `dim_page`, `dim_wiki` | one row per page / wiki |
| `dim_user` (snapshot) | one row per editor version, SCD Type 2 on `is_bot`, `is_anonymous` |
| `agg_daily_wiki`, `agg_editor_leaderboard` | wiki × day, editor × day |

**Vandalism score (0–100).** A heuristic; tuning it against a hand-labeled sample is still to do. Bot edits score 0.

| Signal | Weight |
|---|---|
| Anonymous editor (IP or temporary account) | 30 |
| Blanking: removes ≥ 500 bytes, or ≥ 80% of a page over 1,000 bytes | 30 |
| Revert war: ≥ 2 revert-like edits on the page in the last hour | 20 |
| Edit burst: the same human made ≥ 10 edits in the last 10 minutes | 20 |

Silver has no edit comments, so a "revert-like" edit is detected by size: it restores the page length from right before a different user's change.

**Data quality.** 29 dbt tests (uniqueness, plausible event times, relationships, Gold `bot + human = total`, score-equals-signals) plus Great Expectations suites for hourly volume (±50% of the 7-day same-hour mean, warn) and distributions.

```bash
cd dbt && set -a && . ../.env && set +a
../.venv/bin/dbt build --profiles-dir .          # models, snapshot, tests
../.venv/bin/dbt docs generate --profiles-dir .  # lineage docs in dbt/target/
cd ../quality/great_expectations && ../.venv/bin/python run_checks.py
```

## Run the pipeline

Needs Docker (on macOS, [Colima](https://github.com/abiosoft/colima) works). The whole stack needs about 6 GB of memory (Colima with 8 GB: `colima start --cpu 4 --memory 8`).

```bash
cp .env.example .env      # set WIKIPULSE_CONTACT and generate S3_ACCESS_KEY / S3_SECRET_KEY
make up                   # Kafka, producer, SeaweedFS, Bronze and Silver jobs
make metrics              # producer: events, reconnects, delivery errors, checkpoints, last event age
make audit                # raw topic: volume, duplicates, event-time gaps
make audit-lake           # Kafka → Bronze → Silver: lost and duplicate events
make restart-producer     # kill and restart the producer (resume test)
make kill-spark           # SIGKILL and restart the Spark app (exactly-once test)
make latency              # end-to-end latency percentiles, last 30 min
make reset-gold           # rebuild Gold from Silver after a Gold logic change
make reset-silver         # rebuild Silver and Gold from Bronze after a Silver logic change
make down                 # stop; data volumes are kept
```

## Live preview app

Until the static dashboard (Milestone 6) exists, `wikipulse/` is a FastAPI app that reads the feed directly and shows live edit stats plus trending articles from the Pageviews API. Per-minute counts are kept in a local SQLite file so the hourly chart survives restarts. The PRD's final dashboard is static and has no backend, so this app is for development only.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]" -r producer/requirements.txt
uvicorn wikipulse.app:app --reload --port 8001
```

| Variable | Default | Purpose |
|---|---|---|
| `WIKIPULSE_CONTACT` | `local development` | Contact info sent in the User-Agent. Set it before deploying. |
| `WIKIPULSE_STREAM` | `1` | `0` turns off the live stream. |
| `WIKIPULSE_TRENDING_TTL` | `1800` | Seconds to cache trending results per wiki. |
| `WIKIPULSE_DB` | `wikipulse.db` | SQLite file for per-minute history. |
| `WIKIPULSE_RETENTION_DAYS` | `30` | Days of per-minute history to keep. |

## Tests

```bash
make test             # preview app + producer unit tests (local venv)
make test-streaming   # PySpark tests on fixture events, inside the streaming image
```
