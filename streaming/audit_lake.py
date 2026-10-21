"""Offset and dedup audit for the Milestone 2 exit test: 0 lost and 0 duplicate events.

Kafka -> Bronze: every Kafka offset from the earliest retained one up to Bronze's newest
must appear in Bronze exactly once.
Bronze -> Silver: every Wikipedia edit in Bronze that has had time to flow through must
appear in Silver exactly once (by meta.id).

    docker compose run --rm spark-tools python audit_lake.py
"""

import json
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F

from common import Config, build_spark
from transforms import parse_bronze, to_silver

SETTLE = timedelta(minutes=2)  # Silver may still be processing rows newer than this


def main() -> None:
    config = Config()
    spark = build_spark("wikipulse-audit", config)
    spark.sparkContext.setLogLevel("ERROR")
    # Full-table aggregations over millions of rows: more, smaller shuffle partitions spill to
    # disk instead of holding a quarter of the table per task (the streaming default is 4).
    spark.conf.set("spark.sql.shuffle.partitions", "32")
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    kafka = (
        spark.read.format("kafka").option("kafka.bootstrap.servers", config.bootstrap)
        .option("subscribe", config.raw_topic)
        .option("startingOffsets", "earliest").option("endingOffsets", "latest").load()
        .select("partition", "offset")  # values aren't needed
        .groupBy("partition").agg(F.min("offset").alias("kafka_min"), F.max("offset").alias("kafka_max"))
    )
    # Pin one Delta version per table, so every count below sees the same snapshot even
    # though both jobs keep writing while the audit runs. Silver is pinned first: anything it
    # has, the (later) Bronze snapshot must have too.
    silver = snapshot(spark, config.table("silver/edits"))
    bronze = snapshot(spark, config.table("bronze/recentchange"))
    per_partition = (
        bronze.groupBy(F.col("kafka_partition").alias("partition"))
        .agg(F.count("*").alias("rows"), F.countDistinct("kafka_offset").alias("distinct_offsets"),
             F.min("kafka_offset").alias("bronze_min"), F.max("kafka_offset").alias("bronze_max"))
        .join(kafka, "partition", "full")
        .orderBy("partition")
        .collect()
    )
    partitions = []
    for r in per_partition:
        partitions.append({
            "partition": r.partition,
            "bronze_rows": r.rows,
            "duplicates": r.rows - r.distinct_offsets,
            "missing_from_start": r.bronze_min - r.kafka_min,
            "missing_in_range": (r.bronze_max - r.bronze_min + 1) - r.distinct_offsets,
            "not_yet_ingested": r.kafka_max - r.bronze_max,
        })

    silver_rows = silver.count()
    silver_distinct = silver.select("edit_id").distinct().count()

    expected = (
        to_silver(parse_bronze(bronze)).groupBy("edit_id")
        .agg(F.min("bronze_ingested_at").alias("first_ingested"))
    )
    missing = expected.join(silver.select("edit_id"), "edit_id", "left_anti")
    cutoff = now - SETTLE
    missing_total = missing.count()
    missing_settled = missing.where(F.col("first_ingested") < F.lit(cutoff)).count()
    missing_in_flight = missing_total - missing_settled
    expected_edits = expected.count()

    report = {
        "audited_at": now.isoformat(timespec="seconds") + "Z",
        "bronze": {
            "rows": sum(p["bronze_rows"] for p in partitions),
            "lost": sum(p["missing_from_start"] + p["missing_in_range"] for p in partitions),
            "duplicates": sum(p["duplicates"] for p in partitions),
            "partitions": partitions,
        },
        "silver": {
            "rows": silver_rows,
            "duplicates": silver_rows - silver_distinct,
            "expected_edits": expected_edits,
            "missing_settled": missing_settled,
            "missing_in_flight": missing_in_flight,
            "reconciles": expected_edits == silver_distinct + missing_total,
            "replays_removed": bronze_replays(bronze),
        },
        "gold": gold_check(spark, config, silver, cutoff),
    }
    print(json.dumps(report, indent=2))


def gold_check(spark, config, silver, cutoff) -> dict:
    """gold_edits_per_minute must equal a fresh recount of Silver for every settled (wiki, minute).

    Gold adds each micro-batch as increments, so a replayed batch counted twice, or a lost
    one, shows up here as a mismatch.
    """
    from delta.tables import DeltaTable

    path = config.table("gold/edits_per_minute")
    if not DeltaTable.isDeltaTable(spark, path):
        return {"status": "no gold table yet"}
    gold = snapshot(spark, path).where(F.col("window_start") < F.lit(cutoff))
    recount = (
        silver.groupBy(F.window("event_time", "1 minute").start.alias("window_start"), "wiki")
        .agg(F.count("*").alias("silver_count"))
        .where(F.col("window_start") < F.lit(cutoff))
    )
    # Gold may trail Silver, and events a few seconds late still land in the previous minute,
    # so only compare minutes at least 2 minutes older than Gold's newest one.
    newest = gold.agg(F.max("window_start")).collect()[0][0]
    if newest is None:
        return {"status": "gold empty"}
    bound = F.lit(newest - SETTLE)
    compared = (
        recount.where(F.col("window_start") < bound)
        .join(gold.where(F.col("window_start") < bound), ["window_start", "wiki"], "full")
        .select(F.coalesce("silver_count", F.lit(0)).alias("s"), F.coalesce("edit_count", F.lit(0)).alias("g"))
    )
    r = compared.agg(
        F.count("*").alias("cells"),
        F.sum((F.col("s") != F.col("g")).cast("int")).alias("mismatched"),
        F.sum(F.col("s")).alias("silver_total"), F.sum(F.col("g")).alias("gold_total"),
    ).collect()[0]
    return {"wiki_minutes_compared": r.cells, "mismatched": r.mismatched,
            "silver_edits": r.silver_total, "gold_edits": r.gold_total}


def snapshot(spark, path: str):
    from delta.tables import DeltaTable

    version = DeltaTable.forPath(spark, path).history(1).collect()[0].version
    return spark.read.format("delta").option("versionAsOf", version).load(path)


def bronze_replays(bronze) -> int:
    """Producer replays: Bronze rows whose meta.id already appeared (all event types)."""
    ids = bronze.select(F.get_json_object("raw_json", "$.meta.id").alias("id"))
    return ids.count() - ids.distinct().count()


if __name__ == "__main__":
    main()
