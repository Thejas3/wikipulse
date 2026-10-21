"""Delta bronze.recentchange -> Delta silver.edits (parsed, filtered to Wikipedia edits, deduplicated).

Deduplication is an insert-only MERGE on edit_id against Silver itself, not a watermarked
dropDuplicates: the source can deliver replayed events hours out of event-time order, and
a watermark silently drops anything older than it (docs/incidents/2026-10-20-silver-late-drops.md).
Each MERGE carries the micro-batch ID as a Delta idempotent-write marker, so a batch replayed
after a crash is skipped instead of inserted twice.
"""

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import (Config, ProgressLogger, build_spark, ensure_bucket, prepare_checkpoint, setup_logging,
                    wait_for_delta_table)
from transforms import parse_bronze, to_silver

log = logging.getLogger("silver")


class Silver:
    def __init__(self, spark: SparkSession, path: str):
        self.spark = spark
        self.path = path

    def ensure_table(self, schema_source: DataFrame) -> None:
        if not DeltaTable.isDeltaTable(self.spark, self.path):
            # Partitioned by date only, not date and wiki; see docs/decisions/0003.
            schema_source.limit(0).write.format("delta").partitionBy("event_date").save(self.path)
            log.info("created %s", self.path)

    def process(self, bronze_batch: DataFrame, batch_id: int) -> None:
        edits = to_silver(parse_bronze(bronze_batch)).dropDuplicates(["edit_id"]).persist()
        try:
            bounds = edits.agg(F.min("event_time").alias("lo"), F.max("event_time").alias("hi"),
                               F.collect_set("event_date").alias("dates")).collect()[0]
            if bounds.lo is None:
                return
            dates = ", ".join(f"date'{d}'" for d in sorted(bounds.dates))
            # A replayed event has the same edit_id, date and event time as the original. The
            # literal date list prunes partitions and the event_time range lets Delta skip
            # files by their min/max stats, so only overlapping files are read.
            condition = (
                "t.edit_id = s.edit_id AND t.event_date = s.event_date AND t.event_time = s.event_time"
                f" AND t.event_date IN ({dates})"
                f" AND t.event_time BETWEEN timestamp'{bounds.lo}' AND timestamp'{bounds.hi}'"
            )
            conf = self.spark.conf
            conf.set("spark.databricks.delta.write.txnAppId", "wikipulse-silver")
            conf.set("spark.databricks.delta.write.txnVersion", str(batch_id))
            try:
                (DeltaTable.forPath(self.spark, self.path).alias("t")
                 .merge(edits.alias("s"), condition)
                 .whenNotMatchedInsertAll()
                 .execute())
            finally:
                conf.unset("spark.databricks.delta.write.txnAppId")
                conf.unset("spark.databricks.delta.write.txnVersion")
        finally:
            edits.unpersist()


def start(spark: SparkSession, config: Config):
    bronze_path = config.table("bronze/recentchange")
    wait_for_delta_table(spark, bronze_path)

    silver = Silver(spark, config.table("silver/edits"))
    silver.ensure_table(to_silver(parse_bronze(spark.read.format("delta").load(bronze_path))))
    query = (
        spark.readStream.format("delta").option("maxFilesPerTrigger", 200).load(bronze_path)
        .writeStream.queryName("silver")
        .foreachBatch(silver.process)
        .option("checkpointLocation", prepare_checkpoint(spark, config.checkpoint("silver")))
        .trigger(processingTime=config.downstream_trigger)
        .start()
    )
    log.info("silver running -> %s", config.table("silver/edits"))
    return query


def main() -> None:
    setup_logging()
    config = Config()
    ensure_bucket(config)
    spark = build_spark("wikipulse-silver", config)
    spark.streams.addListener(ProgressLogger())
    start(spark, config).awaitTermination()


if __name__ == "__main__":
    main()
