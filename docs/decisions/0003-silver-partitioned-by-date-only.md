# 3. Partition silver.edits by event_date only, not by event_date and wiki

Date: 2026-10-20 · Status: accepted

## Context

PRD §6.3 partitions `silver.edits` by `event_date` and `wiki`. Silver runs a micro-batch every 10 seconds. Measured in Milestone 1: about 31 events/sec in total, of which roughly half are Wikipedia `edit`/`new` events spread across hundreds of language wikis.

Partitioning by wiki means each micro-batch writes one file per wiki that had an edit in those 10 seconds. That works out to dozens of files per batch, most holding one to three rows: hundreds of thousands of tiny files a day. The PRD's own risk table lists small-file explosion from 10-second triggers.

## Decision

Partition `silver.edits` by `event_date` only. Queries that filter by wiki rely on Delta's per-file min/max statistics (data skipping). The daily `delta_maintenance` DAG (Milestone 5) runs `OPTIMIZE ... ZORDER BY (wiki)` so each compacted file covers a narrow range of wikis.

## Consequences

- About one file per micro-batch instead of dozens, and compaction can bring a day down to a handful of large files.
- A query for one wiki still skips most files after Z-ordering, without a directory per wiki.
- Bronze keeps the PRD's `ingest_date` partitioning unchanged.
- Revisit if a single wiki ever needs to be rewritten or deleted on its own, which partition-level operations would make cheaper.
