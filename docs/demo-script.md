# 2-minute demo video: script and shot list

Record at 1080p with the stack running (`make up`) and a few hours of data in the lake.
Times are cumulative.

| Time | Show | Say (roughly) |
|---|---|---|
| 0:00 | Live dashboard, https://thejas3.github.io/wikipulse/ | "WikiPulse streams every edit made to Wikipedia, about 30 a second, and shows what the world is editing right now. This page is static, on GitHub Pages: no backend." |
| 0:10 | Scroll: edits/minute chart, Trending, Spikes, vandalism table; click a "view diff" link | "Trending pages over the last 15 minutes, sudden bursts flagged by z-score against each page's own 24-hour baseline, and edits a heuristic thinks are vandalism, linked to the diff." |
| 0:25 | Explore with SQL: run the example query | "The same Parquet snapshots can be queried in the browser with DuckDB-WASM." |
| 0:35 | `docs/architecture.md` diagram | "Under it: a producer reading Wikimedia's live stream into Kafka, Spark Structured Streaming into Delta Lake Bronze, Silver and Gold, then dbt and DuckDB every 15 minutes, orchestrated by Airflow." |
| 0:50 | Terminal: `make kill-spark`, then `make audit-quick` showing `"status": "ok"` | "I kill the producer and Spark mid-stream. Every hop is either at-least-once with dedup or exactly-once, so the audit still finds zero lost and zero duplicate events." |
| 1:10 | `docs/incidents/2026-10-20-silver-late-drops.md` | "The audits caught a real bug: after a 16-hour outage, the stream replayed hours out of order, and a 10-minute watermark silently dropped 161,000 edits. I replaced it with an idempotent MERGE and rebuilt from Bronze." |
| 1:30 | Grafana "WikiPulse pipeline": latency, lag, alerts | "Prometheus and Grafana track lag, batch time and end-to-end latency: about 35 seconds median from an edit happening to it being in Gold." |
| 1:45 | Airflow grid: dbt_build → export_dashboard | "Every 15 minutes dbt rebuilds the star schema and runs 29 tests. Only if they pass does the export publish to the dashboard." |
| 1:55 | README "Measured so far" table | "Every number in the README is measured, and each design decision has an ADR. Code's on GitHub." |
