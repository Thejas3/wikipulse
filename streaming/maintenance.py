"""Delta maintenance (OPTIMIZE + VACUUM), requested by Airflow and run inside the Spark app.

Delta's S3 log store assumes one writer process per table. Running OPTIMIZE from Airflow in
a second process (delta-rs, another Spark) could race the streaming writers on the commit
log. Instead the delta_maintenance DAG writes a request object, this thread (inside the
only writer process) runs it, and writes a result object the DAG waits for (ADR 0006):

    _control/maintenance/requests/<id>.json   written by Airflow
    _control/maintenance/done/<id>.json       written here: status, per-table sizes before/after

Compaction only touches partitions no live query writes to (older than today / 2 hours), so it
never conflicts with the streaming appends and MERGEs.
"""

import json
import logging
import threading
import time
import traceback
from datetime import UTC, datetime

import boto3
from delta.tables import DeltaTable

import metrics
from common import Config

log = logging.getLogger("maintenance")

REQUESTS = "_control/maintenance/requests/"
DONE = "_control/maintenance/done/"
VACUUM_HOURS = 168  # PRD §8: 7-day retention

# (table, OPTIMIZE predicate on partitions no streaming query writes to, Z-order columns)
TABLES = [
    ("bronze/recentchange", "ingest_date < current_date()", []),
    ("silver/edits", "event_date < current_date()", ["wiki"]),  # ADR 0003
    ("gold/edits_per_minute", "window_hour < current_timestamp() - INTERVAL 2 HOURS", []),
    ("gold/trending_pages", "window_hour < current_timestamp() - INTERVAL 2 HOURS", []),
    ("gold/spikes", None, []),
    ("gold/latency_histogram", None, []),
]


def table_size(spark, path: str) -> dict:
    d = DeltaTable.forPath(spark, path).detail().select("numFiles", "sizeInBytes").collect()[0]
    return {"files": d.numFiles, "bytes": d.sizeInBytes}


def run_maintenance(spark, config: Config) -> dict:
    results = {}
    for name, predicate, zorder in TABLES:
        path = config.table(name)
        if not DeltaTable.isDeltaTable(spark, path):
            continue
        started = time.monotonic()
        before = table_size(spark, path)
        sql = f"OPTIMIZE delta.`{path}`"
        if predicate:
            sql += f" WHERE {predicate}"
        if zorder:
            sql += f" ZORDER BY ({', '.join(zorder)})"
        spark.sql(sql).collect()
        after_optimize = table_size(spark, path)
        spark.sql(f"VACUUM delta.`{path}` RETAIN {VACUUM_HOURS} HOURS").collect()
        results[name] = {"before": before, "after_optimize": after_optimize,
                         "after_vacuum": table_size(spark, path),
                         "seconds": round(time.monotonic() - started, 1)}
        log.info("maintained %s: %s", name, json.dumps(results[name]))
    return results


class MaintenanceWorker(threading.Thread):
    def __init__(self, spark, config: Config, poll_seconds: int = 60):
        super().__init__(name="delta-maintenance", daemon=True)
        self.spark, self.config, self.poll = spark, config, poll_seconds
        self.s3 = boto3.client("s3", endpoint_url=config.s3_endpoint, aws_access_key_id=config.s3_access_key,
                               aws_secret_access_key=config.s3_secret_key, region_name="us-east-1")

    def run(self) -> None:
        self.spark.sparkContext.setLocalProperty("spark.scheduler.pool", "maintenance")
        while True:
            try:
                self._poll_once()
            except Exception:
                log.exception("maintenance poll failed")
            time.sleep(self.poll)

    def _poll_once(self) -> None:
        listing = self.s3.list_objects_v2(Bucket=self.config.bucket, Prefix=REQUESTS)
        for obj in sorted(listing.get("Contents", []), key=lambda o: o["Key"]):
            request_id = obj["Key"][len(REQUESTS):].removesuffix(".json")
            log.info("running maintenance request %s", request_id)
            started = datetime.now(UTC).isoformat(timespec="seconds")
            try:
                result = {"status": "success", "tables": run_maintenance(self.spark, self.config)}
                metrics.MAINTENANCE_RUNS.labels("success").inc()
            except Exception as exc:
                result = {"status": "failed", "error": str(exc), "trace": traceback.format_exc()[-2000:]}
                metrics.MAINTENANCE_RUNS.labels("failed").inc()
                log.exception("maintenance request %s failed", request_id)
            result.update(request_id=request_id, started_at=started,
                          finished_at=datetime.now(UTC).isoformat(timespec="seconds"))
            self.s3.put_object(Bucket=self.config.bucket, Key=f"{DONE}{request_id}.json",
                               Body=json.dumps(result, indent=2).encode())
            self.s3.delete_object(Bucket=self.config.bucket, Key=obj["Key"])
