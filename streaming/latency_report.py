"""End-to-end latency (edit made -> visible in Gold) percentiles from gold.latency_histogram.

    docker compose run --rm spark-tools python latency_report.py [minutes]   # default 30

Percentiles are bucket upper bounds, so they slightly overstate the true value.
"""

import json
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F

from common import Config, build_spark


def percentile(buckets: list[tuple[float, int]], q: float) -> float | None:
    total = sum(n for _, n in buckets)
    if not total:
        return None
    running = 0
    for upper, n in sorted(buckets):
        running += n
        if running >= q * total:
            return upper
    return buckets[-1][0]


def main() -> None:
    minutes = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    config = Config()
    spark = build_spark("wikipulse-latency", config)
    spark.sparkContext.setLogLevel("ERROR")
    since = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=minutes)
    rows = (
        spark.read.format("delta").load(config.table("gold/latency_histogram"))
        .where(F.col("measured_at") >= F.lit(since))
        .groupBy("le_seconds").agg(F.sum("events").alias("events")).collect()
    )
    buckets = [(r.le_seconds, r.events) for r in rows]
    print(json.dumps({
        "window_minutes": minutes,
        "events": sum(n for _, n in buckets),
        "p50_seconds": percentile(buckets, 0.50),
        "p95_seconds": percentile(buckets, 0.95),
        "p99_seconds": percentile(buckets, 0.99),
        "histogram": {str(u): n for u, n in sorted(buckets)},
    }, indent=2))


if __name__ == "__main__":
    main()
