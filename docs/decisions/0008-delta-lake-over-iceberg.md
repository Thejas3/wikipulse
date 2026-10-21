# 8. Delta Lake rather than Apache Iceberg for the lakehouse tables

Date: 2026-10-21 · Status: accepted

## Context

The lakehouse needs ACID tables on S3-compatible storage that one Spark Structured Streaming app writes continuously and two other engines read: DuckDB (dbt models, the hourly audit, the dashboard export) and Spark itself (Silver reads Bronze; Gold reads Silver). The PRD names Delta Lake and lists swapping to Iceberg as a stretch goal.

The pipeline depends on these table features, in order of importance:

1. **Exactly-once streaming writes.** Bronze uses the Delta streaming sink, which records each micro-batch ID, so a replayed batch isn't written twice.
2. **Idempotent MERGE.** Silver's insert-only dedup and Gold's increments carry `txnAppId` / `txnVersion`, so a replayed micro-batch is skipped (ADR 0004; verified when the app was OOM-killed mid-MERGE during the incident rebuild).
3. **Streaming reads of a table** (Silver from Bronze, Gold from Silver), with offsets in the checkpoint.
4. **Readable by DuckDB** without a catalog service.
5. **Compaction and file pruning:** OPTIMIZE / Z-ORDER / VACUUM, and min/max file statistics for skipping.

## Decision

Use **Delta Lake 3.2.1** on Spark 3.5.3, with path-based tables (no metastore or catalog service).

## Consequences

- All five needs are met with built-in features. Idempotent MERGE via `txnAppId` / `txnVersion` turned out to be central: it is what makes Silver and Gold exactly-once without a watermark. Iceberg has no equivalent built-in micro-batch idempotency marker for MERGE; the pipeline would need its own dedup ledger.
- Path-based tables mean no catalog to run. Iceberg needs a catalog (Hive, REST, JDBC, or a Hadoop catalog with known limits on S3) to commit safely, which is one more service on a small VM.
- DuckDB reads Delta through its `delta` extension (delta-kernel-rs). That worked, with two caveats found along the way: an internal error when aggregating directly over `delta_scan` (staging into a temp table avoids it), and the browser build lacking time-zone support (the export writes plain UTC timestamps).
- **Costs:** Delta's single-writer assumption on S3 shaped the maintenance design (ADR 0006), and the format is less engine-neutral than Iceberg.

## Alternatives considered

- **Apache Iceberg:** stronger multi-engine story, hidden partitioning, and partition evolution, which would have made ADR 0003's partitioning trade-off easier. It loses on built-in idempotent MERGE for micro-batches, on needing a catalog service, and on DuckDB's Iceberg reader needing that catalog or a metadata path. Still a good stretch goal: the Gold MERGE would need an explicit batch ledger to stay exactly-once.
- **Plain Parquet files:** no ACID commits, no MERGE, no time travel. Exactly-once would have to be hand-built.
