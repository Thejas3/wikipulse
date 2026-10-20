# WikiPulse

A real-time data pipeline that streams every edit made across Wikipedia through Kafka into a Bronze → Silver → Gold lakehouse, and serves live analytics on a public dashboard. The full plan is in the PRD; this README tracks what's built and the numbers measured so far.

## Status

| Milestone (PRD §12) | State |
|---|---|
| 1. Producer and Kafka | **Built.** Resume test passed; 1-hour ingest run in progress |
| 2. Bronze and Silver (Spark, Delta on MinIO) | Not started |
| 3. Streaming Gold (windows, trending, spikes) | Not started |
| 4. dbt models and data quality | Not started |
| 5. Airflow and monitoring | Not started |
| 6. Public dashboard (GitHub Pages, DuckDB-WASM) | Not started; a live preview exists (below) |
| 7. Hardening and write-up | Not started |

## Measured so far

| What | Result | How |
|---|---|---|
| Live event rate | ~31 events/sec (all wikis) | `make audit` over 132 s of ingest |
| Gaps after a producer crash | 0 gaps over 10 s (largest 0.4 s) | SIGKILL, 30 s down, restart, `make audit` |
| Replayed duplicates after that crash | 435 (removed later by Silver dedup) | same |
| Schema rejects on live data | 0 of 500 sampled events | validated against `contracts/` |

## Architecture so far

```
Wikimedia SSE feed ──► producer (schema check, resume ID) ──► Kafka
                                   │                           ├─ wiki.recentchange.raw (3 partitions, keyed by wiki, 7-day retention)
                                   └─ Prometheus :9108         └─ wiki.recentchange.dlq (rejected events + reason)
```

The producer only advances its saved resume point after Kafka has acknowledged every event up to it, so a crash replays a few events but never loses any. See [ADR 0001](docs/decisions/0001-kafka-between-feed-and-spark.md).

## Run the pipeline

Needs Docker (on macOS, [Colima](https://github.com/abiosoft/colima) works: `colima start --cpu 4 --memory 6`).

```bash
cp .env.example .env      # set WIKIPULSE_CONTACT (Wikimedia's User-Agent policy)
make up                   # Kafka (KRaft, no ZooKeeper) + topic setup + producer
make metrics              # events, reconnects, delivery errors, checkpoints, last event age
make audit                # volume, duplicates and event-time gaps in the raw topic
make restart-producer     # kill and restart the producer (resume test)
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
make test    # preview app + producer unit tests
```
