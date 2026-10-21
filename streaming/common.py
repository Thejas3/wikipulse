"""Shared Spark session, storage paths and helpers for the streaming jobs."""

import json
import logging
import os
import time
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener

from packages import PACKAGES

log = logging.getLogger("streaming")



@dataclass(frozen=True)
class Config:
    bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    raw_topic: str = os.getenv("TOPIC_RAW", "wiki.recentchange.raw")
    s3_endpoint: str = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
    s3_access_key: str = os.getenv("S3_ACCESS_KEY", "")
    s3_secret_key: str = os.getenv("S3_SECRET_KEY", "")
    bucket: str = os.getenv("LAKE_BUCKET", "lakehouse")
    trigger: str = os.getenv("TRIGGER_INTERVAL", "10 seconds")  # Bronze (PRD §6.2)
    # Silver and Gold start the next batch as soon as the previous one finishes; with no
    # new upstream commits a batch is a no-op, so this cuts trigger waits, not adds commits.
    downstream_trigger: str = os.getenv("DOWNSTREAM_TRIGGER_INTERVAL", "0 seconds")

    def table(self, name: str) -> str:
        return f"s3a://{self.bucket}/{name}"

    def checkpoint(self, job: str) -> str:
        return f"s3a://{self.bucket}/_checkpoints/{job}"


def build_spark(app_name: str, config: Config | None = None, s3: bool = True) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[3]"))
        .config("spark.jars.packages", PACKAGES)
        .config("spark.jars.ivy", os.getenv("SPARK_IVY_DIR", "/opt/ivy"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.driver.extraJavaOptions", os.getenv("SPARK_DRIVER_JAVA_OPTS", ""))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Both settings are frozen into a query's checkpoint on its first run.
        .config("spark.sql.shuffle.partitions", "4")  # default 200 is far too many for ~30 events/sec
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .config("spark.sql.session.timeZone", "UTC")
        # Delta rebuilds table state several times per micro-batch; its default of 50 tasks per
        # rebuild made scheduling overhead, not data, the main cost on a 3-core driver.
        .config("spark.databricks.delta.snapshotPartitions", "4")
        # No query is stateful any more (ADR 0004); this keeps any future stateful query from
        # running back-to-back no-data batches under the 0 s trigger, which burned ~1 core.
        .config("spark.sql.streaming.noDataMicroBatches.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")  # progress bars flood container logs
        .config("spark.scheduler.mode", "FAIR")  # queries in one app share cores fairly (pipeline.py)
    )
    if s3 and config:
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", config.s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", config.s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", config.s3_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(config.s3_endpoint.startswith("https")).lower())
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_bucket(config: Config, attempts: int = 30) -> None:
    """Create the lake bucket if missing, waiting for the object store to come up."""
    import boto3
    from botocore.exceptions import ClientError, EndpointConnectionError

    s3 = boto3.client("s3", endpoint_url=config.s3_endpoint, aws_access_key_id=config.s3_access_key,
                      aws_secret_access_key=config.s3_secret_key, region_name="us-east-1")
    for attempt in range(attempts):
        try:
            existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
            if config.bucket not in existing:
                s3.create_bucket(Bucket=config.bucket)
                log.info("created bucket %s", config.bucket)
            return
        except (ClientError, EndpointConnectionError) as exc:
            log.info("object store not ready (%s), retry %d", exc, attempt + 1)
            time.sleep(2)
    raise RuntimeError(f"object store at {config.s3_endpoint} never became ready")


def prepare_checkpoint(spark: SparkSession, path: str) -> str:
    """Put a `.keep` file in the checkpoint's offsets/ and commits/ folders.

    SeaweedFS's S3 gateway deletes empty folders automatically, but Spark creates commits/
    empty and lists it before the first commit, so a long first micro-batch failed with
    FileNotFoundException. Spark only treats numbered files as batches and never deletes
    other files, so the marker keeps the folders in place for good (ADR 0002).
    """
    jvm = spark.sparkContext._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    for folder in ("offsets", "commits"):
        keep = jvm.org.apache.hadoop.fs.Path(f"{path}/{folder}/.keep")
        if not fs.exists(keep):
            fs.create(keep, True).close()
    return path


def wait_for_delta_table(spark: SparkSession, path: str, poll: float = 10.0) -> None:
    """Block until another job has created the Delta table at `path`."""
    from delta.tables import DeltaTable

    while not DeltaTable.isDeltaTable(spark, path):
        log.info("waiting for Delta table at %s", path)
        time.sleep(poll)


class ProgressLogger(StreamingQueryListener):
    """One JSON log line per micro-batch; the PRD's Prometheus export builds on this in Milestone 5."""

    def onQueryStarted(self, event):
        log.info("query started: %s", event.name)

    def onQueryProgress(self, event):
        p = event.progress
        state = p.stateOperators[0] if p.stateOperators else None
        log.info("progress %s", json.dumps({
            "query": p.name,
            "batch": p.batchId,
            "input_rows": p.numInputRows,
            "rows_per_sec": round(p.processedRowsPerSecond or 0, 1),
            "batch_ms": p.durationMs.get("triggerExecution"),
            # Where the batch time went: offsets, planning, the write itself, commit logs.
            "phases_ms": {k: v for k, v in p.durationMs.items() if k != "triggerExecution" and v},
            "state_rows": state.numRowsTotal if state else None,
            "dropped_late": state.numRowsDroppedByWatermark if state else None,
        }))

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        log.warning("query terminated: %s %s", event.id, event.exception or "")


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
