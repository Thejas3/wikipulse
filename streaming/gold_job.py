"""Delta silver.edits -> streaming Gold: per-minute counts, trending pages, spikes, latency.

Each Silver micro-batch is aggregated by event-time window and MERGEd into the Gold tables
as increments. Every MERGE carries the micro-batch ID as a Delta idempotent-write marker
(txnAppId/txnVersion), so a batch replayed after a crash is skipped instead of counted twice.
Late events simply increment their own (older) window; nothing is dropped. See ADR 0004.
"""

import json
import logging
import math
import time
from datetime import datetime, timedelta, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import Config, build_spark, ensure_bucket, prepare_checkpoint, setup_logging, wait_for_delta_table

log = logging.getLogger("gold")

# Spike rule (PRD §6.4, plus the editor floor from ADR 0005).
SPIKE_MIN_EDITS = 5
SPIKE_MIN_EDITORS = 2
SPIKE_Z = 3.0
BASELINE_HOURS = 24
BASELINE_MIN_BUCKETS = 12  # need at least 1 hour of history before flagging anything
STD_FLOOR = 1.0  # a page with no history has std 0; avoid dividing by zero

# Upper bounds (seconds) of the latency histogram buckets; the last one catches everything.
LATENCY_FLUSH_EVERY = 6  # Gold batches between latency-histogram commits
LATENCY_BUCKETS = [1, 2, 3, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 70, 80, 90, 120, 300, 600, 1800, 3600, 21600, math.inf]


# ---- Pure aggregations (unit-tested) ----

def per_minute_counts(silver: DataFrame) -> DataFrame:
    w = F.window("event_time", "1 minute")
    return silver.groupBy(w.start.alias("window_start"), "wiki").agg(
        F.count("*").alias("edit_count"),
        F.sum((~F.col("is_bot")).cast("long")).alias("human_count"),
        F.sum(F.col("is_bot").cast("long")).alias("bot_count"),
        F.sum(F.greatest("byte_delta", F.lit(0))).alias("bytes_added"),
        F.max("event_time").alias("last_event_time"),
    ).withColumn("window_hour", F.date_trunc("hour", "window_start"))


def trending_counts(silver: DataFrame) -> DataFrame:
    """Article edits per page in 5-minute windows that slide every minute."""
    w = F.window("event_time", "5 minutes", "1 minute")
    return silver.where("is_article").groupBy(
        w.start.alias("window_start"), w.end.alias("window_end"), "wiki", "domain", "title",
    ).agg(
        F.count("*").alias("edit_count"),
        F.sum((~F.col("is_bot")).cast("long")).alias("human_count"),
        F.array_sort(F.collect_set("user_name")).alias("editors"),
    ).withColumn("window_hour", F.date_trunc("hour", "window_start"))


def score_spikes(candidates: DataFrame, buckets: DataFrame, history_start: datetime | None) -> DataFrame:
    """z-score each candidate window against its page's tumbling 5-minute buckets in the prior 24 h.

    `buckets` holds only buckets that had edits; every other bucket in the baseline counts
    as zero, so mean = sum / N and variance = sum_sq / N - mean^2, where N is the number of
    5-minute buckets in the baseline period (shorter while history is still building up).
    """
    joined = candidates.alias("c").join(
        buckets.alias("b"),
        (F.col("b.wiki") == F.col("c.wiki")) & (F.col("b.title") == F.col("c.title"))
        & (F.col("b.window_start") <= F.col("c.window_start") - F.expr("INTERVAL 5 MINUTES"))
        & (F.col("b.window_start") >= F.col("c.window_start") - F.expr(f"INTERVAL {BASELINE_HOURS} HOURS")),
        "left",
    )
    baseline_start = F.col("c.window_start") - F.expr(f"INTERVAL {BASELINE_HOURS} HOURS")
    if history_start is not None:
        baseline_start = F.greatest(baseline_start, F.lit(history_start))
    n = F.floor((F.col("c.window_start").cast("long") - baseline_start.cast("long")) / 300)
    agg = joined.groupBy("c.window_start", "c.window_end", "c.wiki", "c.domain", "c.title",
                         "c.edit_count", "c.unique_editors").agg(
        F.coalesce(F.sum("b.edit_count"), F.lit(0)).alias("s"),
        F.coalesce(F.sum(F.col("b.edit_count") * F.col("b.edit_count")), F.lit(0)).alias("ss"),
        F.first(n).alias("n"),
    )
    mean = F.col("s") / F.col("n")
    std = F.greatest(F.sqrt(F.greatest(F.col("ss") / F.col("n") - mean * mean, F.lit(0.0))), F.lit(STD_FLOOR))
    return (
        agg.where(F.col("n") >= BASELINE_MIN_BUCKETS)
        .select("window_start", "window_end", "wiki", "domain", "title",
                F.col("edit_count").alias("window_count"), "unique_editors",
                mean.alias("baseline_mean"), std.alias("baseline_std"),
                ((F.col("edit_count") - mean) / std).alias("z_score"), "n")
        .withColumnRenamed("n", "baseline_buckets")
        .where(F.col("z_score") > SPIKE_Z)
    )


def latency_histogram(silver: DataFrame, visible_at: datetime) -> DataFrame:
    """End-to-end latency per event (Gold commit time - meta.dt), bucketed."""
    latency = F.lit(visible_at).cast("double") - F.col("event_time").cast("double")
    bucket = F.lit(LATENCY_BUCKETS[-1])
    for upper in reversed(LATENCY_BUCKETS[:-1]):
        bucket = F.when(latency <= upper, F.lit(float(upper))).otherwise(bucket)
    return silver.groupBy(bucket.alias("le_seconds")).agg(F.count("*").alias("events"))


# ---- Delta writes ----

class Gold:
    def __init__(self, spark: SparkSession, config: Config):
        self.spark = spark
        self.paths = {name: config.table(f"gold/{name}") for name in
                      ("edits_per_minute", "trending_pages", "spikes", "latency_histogram")}
        self.history_start: datetime | None = None
        # Latency rows are buffered and written once a minute: a Delta commit per batch added
        # 2-4 s to every Gold batch. A crash loses at most the buffered measurements, not data.
        self.latency_buffer: list[tuple] = []
        self.compacted_hours: set[datetime] = set()

    def ensure_tables(self) -> None:
        """Create the Gold tables (empty) so MERGEs always have a target."""
        schemas = {
            "edits_per_minute": "window_start timestamp, wiki string, edit_count long, human_count long, "
                                "bot_count long, bytes_added long, last_event_time timestamp, "
                                "updated_at timestamp, window_hour timestamp",
            "trending_pages": "window_start timestamp, window_end timestamp, wiki string, domain string, "
                              "title string, edit_count long, human_count long, editors array<string>, "
                              "unique_editors int, updated_at timestamp, window_hour timestamp",
            "spikes": "window_start timestamp, window_end timestamp, wiki string, domain string, title string, "
                      "window_count long, unique_editors int, baseline_mean double, baseline_std double, "
                      "z_score double, baseline_buckets long, detected_at timestamp, updated_at timestamp",
            "latency_histogram": "batch_id long, measured_at timestamp, le_seconds double, events long",
        }
        # Hourly partitions: each MERGE reads and rewrites only the current hour (ADR 0004).
        partitions = {"edits_per_minute": ["window_hour"], "trending_pages": ["window_hour"]}
        for name, schema in schemas.items():
            path = self.paths[name]
            if DeltaTable.isDeltaTable(self.spark, path):
                continue
            (self.spark.createDataFrame([], schema).write.format("delta")
             .partitionBy(*partitions.get(name, []))
             .save(path))  # compaction runs daily (OPTIMIZE in Milestone 5), not inline per batch
            log.info("created %s", path)

    def _idempotent(self, table: str, batch_id: int):
        conf = self.spark.conf
        conf.set("spark.databricks.delta.write.txnAppId", f"wikipulse-gold-{table}")
        conf.set("spark.databricks.delta.write.txnVersion", str(batch_id))

    def _clear_idempotent(self):
        self.spark.conf.unset("spark.databricks.delta.write.txnAppId")
        self.spark.conf.unset("spark.databricks.delta.write.txnVersion")

    def _merge(self, table: str, source: DataFrame, keys: list[str], updates: dict, batch_id: int,
               min_window: datetime) -> None:
        # Literal bounds on the target let Delta prune hour partitions and skip files by their
        # min/max stats, instead of scanning the whole table.
        bounds = [f"t.window_start >= timestamp'{min_window:%Y-%m-%d %H:%M:%S}'"]
        if table != "spikes":
            bounds.append(f"t.window_hour >= timestamp'{min_window:%Y-%m-%d %H}:00:00'")
        condition = " AND ".join([f"t.{k} = s.{k}" for k in keys] + bounds)
        self._idempotent(table, batch_id)
        try:
            (DeltaTable.forPath(self.spark, self.paths[table]).alias("t")
             .merge(source.alias("s"), condition)
             .whenMatchedUpdate(set=updates)
             .whenNotMatchedInsertAll()
             .execute())
        finally:
            self._clear_idempotent()

    def process(self, batch: DataFrame, batch_id: int) -> None:
        batch = batch.persist()
        timings, t = {}, time.monotonic()

        def lap(step: str) -> None:
            nonlocal t
            timings[step] = round(time.monotonic() - t, 2)
            t = time.monotonic()

        try:
            stats = batch.agg(F.count("*").alias("n"), F.min("event_time").alias("t0")).collect()[0]
            if not stats.n:
                return
            min_window = stats.t0.replace(second=0, microsecond=0)
            earliest_slide = min_window - timedelta(minutes=4)  # 5-min windows sliding by 1 min
            now = F.current_timestamp()
            lap("read")

            self._merge("edits_per_minute", per_minute_counts(batch).withColumn("updated_at", now),
                        ["window_hour", "window_start", "wiki"], {
                            "edit_count": "t.edit_count + s.edit_count",
                            "human_count": "t.human_count + s.human_count",
                            "bot_count": "t.bot_count + s.bot_count",
                            "bytes_added": "t.bytes_added + s.bytes_added",
                            "last_event_time": "greatest(t.last_event_time, s.last_event_time)",
                            "updated_at": "s.updated_at",
                        }, batch_id, min_window)
            lap("merge_per_minute")

            trending = (trending_counts(batch).withColumn("unique_editors", F.size("editors"))
                        .withColumn("updated_at", now))
            self._merge("trending_pages", trending, ["window_hour", "window_start", "wiki", "title"], {
                "edit_count": "t.edit_count + s.edit_count",
                "human_count": "t.human_count + s.human_count",
                "editors": "array_sort(array_distinct(concat(t.editors, s.editors)))",
                "unique_editors": "size(array_distinct(concat(t.editors, s.editors)))",
                "updated_at": "s.updated_at",
            }, batch_id, earliest_slide)
            lap("merge_trending")

            self._detect_spikes(trending, batch_id, earliest_slide)
            lap("spikes")

            visible_at = datetime.now(timezone.utc).replace(tzinfo=None)
            latency_rows = [(r.le_seconds, r.events) for r in latency_histogram(batch, visible_at).collect()]
            self.latency_buffer.extend((batch_id, visible_at, le, n) for le, n in latency_rows)
            try:
                import metrics
                metrics.record_latency(latency_rows)
            except Exception:
                log.exception("failed to record latency metrics")
            if batch_id % LATENCY_FLUSH_EVERY == 0:
                self._flush_latency(batch_id)
            lap("latency")
            self._compact_closed_hour()
            lap("compact")
            log.info("batch timings %s", json.dumps({"batch": batch_id, "rows": stats.n, **timings}))
        finally:
            batch.unpersist()

    def _compact_closed_hour(self) -> None:
        """OPTIMIZE the hourly partition that closed 2 hours ago, once.

        Every MERGE adds files to the current hour, so an hour ends with hundreds of small files,
        and the spike baseline scans 25 hours of them. Daily OPTIMIZE (ADR 0006) is too slow
        for that, so Gold compacts each hour once no MERGE can touch it. It runs in Gold's own
        thread, between batches, so it never races Gold's writes.
        """
        hour = (datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=2)).replace(
            minute=0, second=0, microsecond=0)
        if hour in self.compacted_hours:
            return
        for table in ("edits_per_minute", "trending_pages"):
            self.spark.sql(f"OPTIMIZE delta.`{self.paths[table]}` "
                           f"WHERE window_hour = timestamp'{hour:%Y-%m-%d %H}:00:00'").collect()
        self.compacted_hours.add(hour)
        log.info("compacted gold hour %s", hour)

    def _flush_latency(self, batch_id: int) -> None:
        if not self.latency_buffer:
            return
        rows = self.spark.createDataFrame(
            self.latency_buffer, "batch_id long, measured_at timestamp, le_seconds double, events long")
        (rows.write.format("delta").mode("append")
         .option("txnAppId", "wikipulse-gold-latency").option("txnVersion", batch_id)
         .save(self.paths["latency_histogram"]))
        self.latency_buffer = []

    def _detect_spikes(self, touched: DataFrame, batch_id: int, earliest: datetime) -> None:
        trending = self.spark.read.format("delta").load(self.paths["trending_pages"])
        if self.history_start is None:
            self.history_start = trending.agg(F.min("window_start")).collect()[0][0]
        keys = touched.select("window_start", "wiki", "title").distinct()
        candidates = (
            trending.where((F.col("window_hour") >= F.date_trunc("hour", F.lit(earliest)))
                           & (F.col("window_start") >= F.lit(earliest)))
            .join(keys, ["window_start", "wiki", "title"])
            .where((F.col("edit_count") >= SPIKE_MIN_EDITS) & (F.col("unique_editors") >= SPIKE_MIN_EDITORS))
        ).persist()
        try:
            if candidates.isEmpty():
                return
            pages = candidates.select("wiki", "title").distinct()
            buckets = (
                trending.where((F.minute("window_start") % 5 == 0)
                               & (F.col("window_hour") >= F.date_trunc("hour", F.lit(earliest) - F.expr(f"INTERVAL {BASELINE_HOURS + 1} HOURS"))))
                .join(F.broadcast(pages), ["wiki", "title"])
                .select("wiki", "title", "window_start", "edit_count")
            )
            spikes = score_spikes(candidates, buckets, self.history_start).withColumn(
                "detected_at", F.current_timestamp()).withColumn("updated_at", F.current_timestamp())
            self._merge("spikes", spikes, ["window_start", "wiki", "title"], {
                "window_count": "s.window_count", "unique_editors": "s.unique_editors",
                "baseline_mean": "s.baseline_mean", "baseline_std": "s.baseline_std",
                "z_score": "s.z_score", "baseline_buckets": "s.baseline_buckets",
                "updated_at": "s.updated_at",  # detected_at keeps the first detection time
            }, batch_id, earliest)
        finally:
            candidates.unpersist()


def start(spark: SparkSession, config: Config):
    silver_path = config.table("silver/edits")
    wait_for_delta_table(spark, silver_path)
    gold = Gold(spark, config)
    gold.ensure_tables()
    query = (
        spark.readStream.format("delta").option("maxFilesPerTrigger", 5000).load(silver_path)  # cost is per batch, not per row
        .writeStream.queryName("gold")
        .foreachBatch(gold.process)
        .option("checkpointLocation", prepare_checkpoint(spark, config.checkpoint("gold")))
        .trigger(processingTime=config.downstream_trigger)
        .start()
    )
    log.info("gold running -> %s", config.table("gold/"))
    return query


def main() -> None:
    from common import ProgressLogger

    setup_logging()
    config = Config()
    ensure_bucket(config)
    spark = build_spark("wikipulse-gold", config)
    spark.streams.addListener(ProgressLogger())
    start(spark, config).awaitTermination()


if __name__ == "__main__":
    main()
