"""Prometheus metrics for the producer (scraped at :METRICS_PORT/metrics)."""

import time

from prometheus_client import Counter, Gauge, start_http_server

EVENTS = Counter("wikipulse_producer_events_total", "Events read from the feed", ["result"])
RECONNECTS = Counter("wikipulse_producer_reconnects_total", "Stream reconnects after an error")
DELIVERY_ERRORS = Counter("wikipulse_producer_delivery_errors_total", "Kafka delivery failures")
CHECKPOINTS = Counter("wikipulse_producer_checkpoints_total", "Resume points written to disk")
LAST_EVENT_AGE = Gauge(
    "wikipulse_producer_last_event_age_seconds", "Seconds since the newest valid event was made"
)

_last_event_ts: float | None = None


def record_event_time(ts: float) -> None:
    global _last_event_ts
    _last_event_ts = ts


LAST_EVENT_AGE.set_function(lambda: time.time() - _last_event_ts if _last_event_ts else float("nan"))


def serve(port: int) -> None:
    start_http_server(port)
