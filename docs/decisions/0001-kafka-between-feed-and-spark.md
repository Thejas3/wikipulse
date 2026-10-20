# 1. Put Kafka between the feed and Spark

Date: 2026-10-19 · Status: accepted

## Context

The Wikimedia `recentchange` feed is a single SSE connection. Spark Structured Streaming has no built-in SSE source, and anything reading the feed directly loses events whenever it restarts, unless it can resume from exactly where it stopped.

Measured on 2026-10-19 (Milestone 1, `producer/audit.py`): about **31 events/sec** across all wikis (roughly 1,800 per minute), with bursts from bulk Commons uploads and Wikidata bots.

## Decision

A small Python producer reads the feed and publishes each event to the Kafka topic `wiki.recentchange.raw`, keyed by `wiki`. Spark reads from Kafka, not from the feed.

- **Delivery is at-least-once.** The producer saves the SSE `Last-Event-ID` to disk only after Kafka has acknowledged every event up to it (`flush()`, then an atomic rename), with `acks=all` and `enable.idempotence=true`. A crash can replay the events since the last checkpoint, but can never skip any. Silver removes replays by deduplicating on `meta.id`.
- **Invalid events aren't dropped.** Events that fail `contracts/recentchange.schema.json` go to `wiki.recentchange.dlq` with the reason attached.
- **3 partitions.** At 31 events/sec one partition is enough, but 3 leave room for 3 Spark tasks in parallel during a replay, and keying by wiki keeps each wiki's events in order.
- **7-day retention** on the raw topic, so Bronze can be rebuilt from Kafka after a bug.

## Consequences

- Spark can be stopped, upgraded or replayed without touching ingestion. Kafka absorbs the backlog.
- Resume verified: producer killed with SIGKILL, down 30 s, restarted. The audit found **0 event-time gaps over 10 s** (largest 0.4 s) and **435 replayed duplicates**, the expected cost of at-least-once.
- Costs: one more service to run (Kafka at about 1 GB of memory), and duplicates that every consumer must tolerate until Silver.

## Alternatives considered

- **Spark reads the feed directly** through a custom source: less infrastructure, but no replay, and a Spark restart loses data.
- **Write to files, then Spark reads the files:** simple, but no backpressure, and small files pile up.
- **Exactly-once from the producer** with Kafka transactions: the SSE checkpoint and the Kafka commit can't share one transaction, so this would add complexity without removing the need for deduplication.
