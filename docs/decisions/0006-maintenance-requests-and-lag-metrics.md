# 6. Airflow requests Delta maintenance, and the Spark app runs it; Kafka lag comes from Spark progress

Date: 2026-10-21 · Status: accepted

## Context

**Maintenance.** PRD §8 has Airflow run `OPTIMIZE` and `VACUUM` daily. The Delta tables live on S3 (SeaweedFS), where Delta's default log store (`S3SingleDriverLogStore`) assumes **one writer process per table**. It serializes commits with in-process locks, because S3 has no atomic "create if absent" rename. Running OPTIMIZE from a second process (Spark in another container, or delta-rs in Airflow) while the streaming app writes could have both processes write the same commit file number, losing a commit and corrupting the table history. Safe multi-writer setups need DynamoDB-style locking or conditional PUTs, which this stack doesn't have.

**Lag.** PRD §8 scrapes Kafka consumer lag with a Kafka exporter. Spark's Kafka source tracks offsets in its own checkpoint and never commits them to a Kafka consumer group, so an exporter sees no consumer and no lag.

## Decision

1. **Maintenance is requested, not executed, by Airflow.** `delta_maintenance` (daily 03:00) writes `_control/maintenance/requests/<run_id>.json`. A worker thread inside the streaming Spark app, the only writer process, polls for requests, runs OPTIMIZE (Silver Z-ordered by `wiki`, per ADR 0003) and `VACUUM ... RETAIN 168 HOURS`, and writes `_control/maintenance/done/<run_id>.json` with each table's file count and size before and after. The DAG's sensor waits for that file and fails the run if the status isn't `success`.
   - Compaction only targets partitions no live query writes to (`< today` for Bronze and Silver, `< now - 2 h` for hourly Gold), so it never conflicts with streaming appends or MERGEs.
2. **Lag is measured by the pipeline itself.** Each Bronze progress event carries the latest and the processed Kafka offsets per partition. The difference is exported as `wikipulse_stream_kafka_lag_records`, and the alert divides it by the live event rate to get "seconds of data behind" (alert above 300 s, per the PRD).

## Consequences

- Airflow stays the scheduler and the place to see success, failure and history, while table correctness never depends on cross-process locking.
- Maintenance shares the Spark app's CPU. It runs at 03:00 UTC, the quietest hour, in its own FAIR pool.
- Before/after sizes from each run feed the PRD's "storage growth, measured with compaction" target.
- If the streaming app is down, maintenance waits (sensor timeout: 2 h) instead of running unsafely.

## Alternatives considered

- **delta-rs in Airflow with `AWS_S3_ALLOW_UNSAFE_RENAME`:** simplest, but it is unsafe by name with a concurrent Spark writer.
- **A second Spark job for maintenance:** the same two-writer problem, plus another 3 GB JVM.
- **Committing offsets to a consumer group so a Kafka exporter works:** Spark offers no first-class way to do that for its source, and the checkpoint would still be the source of truth.
