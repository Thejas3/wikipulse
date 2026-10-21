# 4. Watermark length, and incremental MERGE for streaming Gold

Date: 2026-10-20 · Status: accepted; decision 1 superseded the same day (see below)

## Context

**How late do events arrive?** Measured from Bronze over a 15-minute live window (36,041 events; `kafka_timestamp - meta.dt`):

| | p50 | p95 | p99 | p99.9 | max |
|---|---|---|---|---|---|
| All events | 0.1 s | 1.0 s | 1.3 s | 2.1 s | 3 s |
| Wikipedia edits | 0.4 s | 1.0 s | 1.5 s | 2.3 s | 3 s |

After an outage, the producer replays hours of history (16 h after one network outage here). This ADR originally assumed replays arrive in event-time order. **They don't:** during that backfill, 31–42% of events were more than 10 minutes behind an event published before them in the same Kafka partition, by up to 7.9 h.

Duplicates come from producer replays after a restart: at most `CHECKPOINT_EVERY` = 500 events, about 18 s of event time.

**Gold has three requirements:** results visible within 60 s at p95 (so windows can't wait until they close), exactly-once counts across crashes, and per-event end-to-end latency measurement (PRD §8).

## Decision

1. ~~**Silver dedup watermark: 10 minutes.**~~ **Superseded.** The watermark silently dropped about 120k out-of-order backfill events ([incident report](../incidents/2026-10-20-silver-late-drops.md)). Silver now deduplicates by insert-only MERGE on `edit_id` with the same idempotent-write markers as Gold, so no event is dropped for being late. The pipeline uses no watermarks; for a source whose disorder during recovery is unbounded, completeness matters more than bounded state.

2. **Gold is computed by incremental MERGE, not a stateful streaming aggregation.** Each Silver micro-batch is grouped into event-time windows (1-minute tumbling; 5-minute sliding every 1 minute) with `F.window`, then MERGEd into Gold as increments. Every MERGE carries the micro-batch ID as a Delta idempotent-write marker (`txnAppId` + `txnVersion`), so a batch replayed after a crash is skipped. This was verified on Delta 3.2.1, and Gold actually crashed mid-MERGE (OOM) during development and recovered.

## Consequences

- **No Gold watermark, so nothing is dropped:** a late event increments its own older window. Windows are visible (and updated) after the first micro-batch, instead of after the window plus a watermark delay.
- **Per-event latency is measurable:** the batch still holds individual events, so `commit time - meta.dt` is bucketed into `gold.latency_histogram` for every event.
- **Unique editors can't be summed,** so trending windows keep a sorted, de-duplicated `editors` array, merged with `array_union` semantics.
- **Costs:** each micro-batch does 3–4 Delta commits, and MERGE needs a `window_start` lower bound in its condition so Delta skips old files by their statistics. Small files are compacted by Delta auto-compaction, and daily by `OPTIMIZE` (Milestone 5).

## Alternatives considered

- **Stateful aggregation with a watermark, in append mode:** windows only appear after they close plus the watermark delay (≥ 6 min for trending), which breaks the 60 s target.
- **The same, in update mode via foreachBatch:** low latency and idempotent upserts, but the batch only holds aggregates, so per-event latency can't be measured, and late events beyond the watermark are dropped.
- **A shorter Silver watermark (1–2 min):** less state, but it leaves little margin if the producer's checkpoint interval is raised.
