"""Run Bronze, Silver and Gold as three streaming queries in one Spark application.

One JVM instead of three: on a 4-CPU VM, three separate drivers spent most of their CPU on
per-process overhead (PRD §9 budgets a single Spark driver at 3 GB). Each query keeps its own
checkpoint, so the queries can still be restarted or replayed independently, and each job
file can still run standalone (`python bronze_job.py`).
"""

import logging
import os
import time

import bronze_job
import gold_job
import silver_job
from common import Config, ProgressLogger, build_spark, ensure_bucket, setup_logging

log = logging.getLogger("pipeline")


def in_pool(spark, pool: str, start, config: Config):
    """Start a query in its own FAIR-scheduler pool so a slow Gold batch can't starve Bronze."""
    if pool != "bronze":  # listeners are per session; Bronze's session gets one in main()
        spark.streams.addListener(ProgressLogger())
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
    try:
        return start(spark, config)
    finally:
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)


def main() -> None:
    setup_logging()
    config = Config()
    ensure_bucket(config)
    spark = build_spark("wikipulse-streaming", config)
    spark.streams.addListener(ProgressLogger())

    # Silver and Gold set session-level Delta idempotency confs (txnAppId/txnVersion) around
    # each MERGE. Separate sessions (same SparkContext) keep those confs out of each other's
    # commits and out of Bronze's, where they could make Delta skip a write.
    queries = [in_pool(spark, "bronze", bronze_job.start, config),
               in_pool(spark.newSession(), "silver", silver_job.start, config),
               in_pool(spark.newSession(), "gold", gold_job.start, config)]

    # awaitAnyTermination() only watches one session's queries, and Gold has its own session,
    # so poll all three: the first one to stop ends the process.
    try:
        while all(q.isActive for q in queries):
            time.sleep(5)
        for q in queries:
            if not q.isActive and q.exception():
                log.error("query %s failed: %s", q.name, q.exception())
    finally:
        log.error("query stopped: %s", [q.name for q in queries if not q.isActive])
        # Hard exit so Docker restarts the container: a plain exit would hang on py4j's
        # non-daemon callback-server thread, leaving the other queries running headless.
        os._exit(1)


if __name__ == "__main__":
    main()
