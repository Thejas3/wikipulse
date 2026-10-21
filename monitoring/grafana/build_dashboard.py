"""Generate dashboards/wikipulse.json (the provisioned Grafana dashboard).

    python build_dashboard.py
"""

import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "prometheus"}


def panel(pid, title, kind, targets, x, y, w=12, h=8, unit=None, **extra):
    p = {
        "id": pid, "title": title, "type": kind, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [{"refId": chr(65 + i), "expr": expr, "legendFormat": legend, "datasource": DS}
                    for i, (expr, legend) in enumerate(targets)],
        "fieldConfig": {"defaults": {"unit": unit} if unit else {}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}},
    }
    p.update(extra)
    return p


LATENCY = 'histogram_quantile({q}, sum by (le) (rate(wikipulse_e2e_latency_seconds_bucket[10m])))'

panels = [
    panel(1, "Events/sec from the feed", "timeseries", [
        ('sum(rate(wikipulse_producer_events_total{result="valid"}[1m]))', "valid"),
        ('sum(rate(wikipulse_producer_events_total{result="dlq"}[1m]))', "dead-lettered"),
    ], 0, 0, unit="ops"),
    panel(2, "Newest event age", "stat", [("wikipulse_producer_last_event_age_seconds", "age")], 12, 0, w=6, unit="s"),
    panel(3, "Bronze lag (seconds of data)", "stat", [(
        'sum(wikipulse_stream_kafka_lag_records) / clamp_min(sum(rate(wikipulse_producer_events_total{result="valid"}[5m])), 1)',
        "lag")], 18, 0, w=6, unit="s"),
    panel(4, "End-to-end latency (edit → Gold)", "timeseries", [
        (LATENCY.format(q=0.5), "p50"), (LATENCY.format(q=0.95), "p95"), (LATENCY.format(q=0.99), "p99"),
    ], 0, 8, unit="s"),
    panel(5, "Micro-batch duration", "timeseries", [
        ("wikipulse_stream_batch_duration_seconds", "{{query}}")], 12, 8, unit="s"),
    panel(6, "Rows/sec per query", "timeseries", [
        ("sum by (query) (rate(wikipulse_stream_input_rows_total[2m]))", "{{query}}")], 0, 16, unit="ops"),
    panel(7, "Kafka lag (records) per partition", "timeseries", [
        ("wikipulse_stream_kafka_lag_records", "partition {{partition}}")], 12, 16),
    panel(8, "Producer reconnects & checkpoints (1h)", "stat", [
        ("increase(wikipulse_producer_reconnects_total[1h])", "reconnects"),
        ("increase(wikipulse_producer_checkpoints_total[1h])", "checkpoints"),
    ], 0, 24, w=8),
    panel(9, "Delta maintenance runs (7d)", "stat", [
        ("sum by (result) (increase(wikipulse_maintenance_runs_total[7d]))", "{{result}}")], 8, 24, w=8),
    {
        "id": 10, "title": "Alerts", "type": "alertlist", "gridPos": {"x": 16, "y": 24, "w": 8, "h": 8},
        "options": {"alertListOptions": {}, "stateFilter": {"firing": True, "pending": True},
                    "datasource": "Prometheus", "alertInstanceLabelFilter": "", "viewMode": "list",
                    "showInstances": True, "maxItems": 20, "sortOrder": 1},
    },
]

dashboard = {
    "uid": "wikipulse", "title": "WikiPulse pipeline", "schemaVersion": 39, "version": 1,
    "time": {"from": "now-6h", "to": "now"}, "refresh": "30s", "tags": ["wikipulse"],
    "panels": panels,
}

out = Path(__file__).parent / "dashboards" / "wikipulse.json"
out.write_text(json.dumps(dashboard, indent=2) + "\n")
print(f"wrote {out} ({len(panels)} panels)")
