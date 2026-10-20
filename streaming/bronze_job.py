"""Kafka wiki.recentchange.raw -> Delta bronze.recentchange (append-only, every event unchanged).

Exactly-once into Delta: Kafka offsets are tracked in the query checkpoint, and the Delta
sink records each micro-batch ID, so a batch replayed after a crash is not written twice.
"""

import logging

from common import Config, ProgressLogger, build_spark, ensure_bucket, setup_logging
from transforms import bronze_columns

log = logging.getLogger("bronze")


def main() -> None:
    setup_logging()
    config = Config()
    ensure_bucket(config)
    spark = build_spark("wikipulse-bronze", config)
    spark.streams.addListener(ProgressLogger())

    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap)
        .option("subscribe", config.raw_topic)
        .option("startingOffsets", "earliest")  # first run only; afterwards the checkpoint decides
        .option("maxOffsetsPerTrigger", 50_000)  # bound catch-up batches after downtime
        .load()
    )
    query = (
        bronze_columns(kafka).writeStream.format("delta")
        .queryName("bronze")
        .outputMode("append")
        .partitionBy("ingest_date")
        .option("checkpointLocation", config.checkpoint("bronze"))
        .trigger(processingTime=config.trigger)
        .start(config.table("bronze/recentchange"))
    )
    log.info("bronze running -> %s", config.table("bronze/recentchange"))
    query.awaitTermination()


if __name__ == "__main__":
    main()
