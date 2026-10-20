"""Delta bronze.recentchange -> Delta silver.edits (parsed, filtered to Wikipedia edits, deduplicated)."""

import logging

from common import Config, ProgressLogger, build_spark, ensure_bucket, setup_logging, wait_for_delta_table
from transforms import deduplicate, parse_bronze, to_silver

log = logging.getLogger("silver")


def main() -> None:
    setup_logging()
    config = Config()
    ensure_bucket(config)
    spark = build_spark("wikipulse-silver", config)
    spark.streams.addListener(ProgressLogger())

    bronze_path = config.table("bronze/recentchange")
    wait_for_delta_table(spark, bronze_path)

    bronze = spark.readStream.format("delta").option("maxFilesPerTrigger", 200).load(bronze_path)
    query = (
        deduplicate(to_silver(parse_bronze(bronze))).writeStream.format("delta")
        .queryName("silver")
        .outputMode("append")
        # Partitioned by date only, not date and wiki; see docs/decisions/0003.
        .partitionBy("event_date")
        .option("checkpointLocation", config.checkpoint("silver"))
        .trigger(processingTime=config.trigger)
        .start(config.table("silver/edits"))
    )
    log.info("silver running -> %s", config.table("silver/edits"))
    query.awaitTermination()


if __name__ == "__main__":
    main()
