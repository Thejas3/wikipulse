"""Prometheus metrics for the streaming pipeline (scraped at :METRICS_PORT/metrics).

Spark's Kafka source tracks offsets in its checkpoint, not in a Kafka consumer group, so a
Kafka exporter can't see Bronze's lag. Instead each progress event carries the latest and
the processed offsets per partition, and lag is their difference.
"""

import json
import os
import time

from prometheus_client import Counter, Gauge, Histogram, start_http_server

BATCH_SECONDS = Gauge("wikipulse_stream_batch_duration_seconds", "Last micro-batch duration", ["query"])
INPUT_ROWS = Counter("wikipulse_stream_input_rows_total", "Rows read by each query", ["query"])
PROCESSED_RATE = Gauge("wikipulse_stream_processed_rows_per_second", "Rows/s in the last batch", ["query"])
LAST_PROGRESS = Gauge("wikipulse_stream_last_progress_timestamp_seconds", "When the last batch finished", ["query"])
KAFKA_LAG = Gauge("wikipulse_stream_kafka_lag_records", "Kafka records not yet in Bronze", ["partition"])
E2E_LATENCY = Histogram(
    "wikipulse_e2e_latency_seconds", "Edit made (meta.dt) -> visible in Gold",
    buckets=[5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 70, 80, 90, 120, 300, 600, 1800, 3600],
)
MAINTENANCE_RUNS = Counter("wikipulse_maintenance_runs_total", "Delta maintenance runs", ["result"])


def serve() -> None:
    start_http_server(int(os.getenv("METRICS_PORT", "9109")))


def record_progress(progress) -> None:
    query = progress.name or "unknown"
    BATCH_SECONDS.labels(query).set((progress.durationMs.get("triggerExecution") or 0) / 1000)
    INPUT_ROWS.labels(query).inc(progress.numInputRows or 0)
    PROCESSED_RATE.labels(query).set(progress.processedRowsPerSecond or 0)
    LAST_PROGRESS.labels(query).set(time.time())
    if query == "bronze" and progress.sources:
        source = progress.sources[0]
        try:
            latest = json.loads(source.latestOffset or "{}")
            done = json.loads(source.endOffset or "{}")
        except (TypeError, ValueError):
            return
        for topic, partitions in latest.items():
            for partition, offset in partitions.items():
                KAFKA_LAG.labels(partition).set(max(0, offset - done.get(topic, {}).get(partition, offset)))


def record_latency(histogram_rows) -> None:
    """histogram_rows: (le_seconds, events) pairs from Gold; each event is observed at its bucket bound."""
    for le, events in histogram_rows:
        value = le if le != float("inf") else 7200.0
        for _ in range(int(events)):
            E2E_LATENCY.observe(value)
