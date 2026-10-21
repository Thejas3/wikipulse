# Incident: Silver silently dropped about 6 hours of edits

Date: 2026-10-20 · Severity: data loss (recovered) · Status: fixed

## Summary

`silver.edits` was missing every Wikipedia edit with an event time between 06:00 and 11:00 UTC, and about 40% of 12:00–15:00, roughly 120,000 edits. Bronze had all of them. Silver's watermarked deduplication dropped them as "late". Silver and Gold were rebuilt from Bronze with a new deduplication method that never drops late events.

## Detection

Building the first data-quality check for Milestone 4, the hourly-volume suite saw fewer completed hours than expected. Counting edits per hour at each layer showed the gap:

| hour (UTC) | Bronze Wikipedia edits | Silver |
|---|---|---|
| 05 | 15,802 | 15,759 |
| 06 | 18,115 | 2,930 |
| 07–10 | 18,658–20,406 each | 0 |
| 11 | 20,466 | 33 |
| 12–13 | ~20,000 each | ~11,800 each |

The Kafka gap audit (`producer/audit.py`) showed no gap over 1.1 s across 21 hours, so the loss was between Bronze and Silver. The Silver audit (`make audit-lake`) would have caught it, but it wasn't rerun after the outage.

## Root cause

1. The laptop running the stack slept and lost network for about 16 hours, waking intermittently. On each reconnect the producer resumed from its checkpoint, and Wikimedia replayed the missed events.
2. **The replay arrived far out of event-time order.** Within a single Kafka partition, in the order the producer published, 31–42% of that day's events were more than 10 minutes older than an event published before them; the worst were 7.9 hours behind. A likely cause is that EventStreams interleaves its two datacenter topics when resuming from a saved event ID.
3. Silver deduplicated with `dropDuplicatesWithinWatermark` and a 10-minute event-time watermark. Spark drops rows whose event time is behind the watermark, silently from the job's point of view, so every event more than 10 minutes behind the newest one seen was discarded.
4. ADR 0004 had assumed that "replays arrive in event-time order". The live-traffic measurements (p99.9 lateness 2.3 s) were right, but they didn't cover backfills.

## Fix

- **Silver deduplicates by insert-only MERGE** on `edit_id` against the Silver table itself, bounded by the batch's dates and event-time range so Delta reads only overlapping files. Nothing is dropped for being late; replays and duplicates are skipped. Each MERGE carries the micro-batch ID as a Delta idempotent-write marker, so a crash replay doesn't insert twice.
- Silver and Gold were rebuilt from Bronze (`make reset-silver`). Bronze, the append-only replay source, had every event, which is exactly the case the PRD keeps it for.
- A new unit test covers an event 8 hours older than the newest one, plus a replayed micro-batch.

## Follow-ups

- [x] Correct ADR 0004.
- [ ] Run `make audit-lake` on a schedule (Milestone 5, Airflow), and alert when `missing_settled > 0`.
- [ ] Add a dbt test comparing hourly counts between Bronze and Silver.

## Lessons

- Watermarks trade completeness for bounded state. That's only safe when the source's disorder is bounded and measured, *including during recovery*, not just on live traffic.
- Audits only help if they run continuously. A reconciliation that exists but runs by hand is not monitoring.
