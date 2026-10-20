"""Bronze and Silver transformations as plain DataFrame functions, shared by jobs and tests."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

# The fields Silver uses (PRD §3). Everything else stays available in bronze.raw_json.
EVENT_SCHEMA = StructType([
    StructField("meta", StructType([
        StructField("id", StringType()),
        StructField("dt", StringType()),
        StructField("domain", StringType()),
        StructField("uri", StringType()),
    ])),
    StructField("type", StringType()),
    StructField("wiki", StringType()),
    StructField("namespace", LongType()),
    StructField("title", StringType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),
    StructField("minor", BooleanType()),
    StructField("timestamp", LongType()),
    StructField("length", StructType([StructField("old", LongType()), StructField("new", LongType())])),
    StructField("revision", StructType([StructField("old", LongType()), StructField("new", LongType())])),
])

WIKIPEDIA_DOMAIN = r"^[a-z0-9-]+\.wikipedia\.org$"
IPV4 = r"^\d{1,3}(\.\d{1,3}){3}$"
IPV6 = r"^[0-9A-Fa-f]{0,4}(:[0-9A-Fa-f]{0,4}){2,7}$"
TEMP_ACCOUNT = r"^~\d{4}-\d+(-\d+)?$"  # Wikimedia temporary accounts, which replace IP editing

WATERMARK = "10 minutes"


def bronze_columns(kafka: DataFrame) -> DataFrame:
    """Kafka records -> bronze.recentchange rows: the event unchanged, plus where it came from."""
    ingested_at = F.current_timestamp()
    return kafka.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        ingested_at.alias("ingested_at"),
        F.to_date(ingested_at).alias("ingest_date"),
    )


def parse_bronze(bronze: DataFrame) -> DataFrame:
    """Parse raw_json into typed columns and derive event time (meta.dt, else the unix timestamp)."""
    e = F.from_json("raw_json", EVENT_SCHEMA).alias("e")
    return bronze.select(e, "kafka_partition", "kafka_offset", "ingested_at").select(
        "e.*",
        "kafka_partition",
        "kafka_offset",
        "ingested_at",
        F.coalesce(F.to_timestamp("e.meta.dt"), F.timestamp_seconds("e.timestamp")).alias("event_time"),
    )


def to_silver(parsed: DataFrame) -> DataFrame:
    """Keep edits and page creations on Wikipedia, and derive Silver's columns (PRD §6.3)."""
    domain = F.col("meta.domain")
    user = F.col("user")
    return (
        parsed.where(F.col("type").isin("edit", "new") & domain.rlike(WIKIPEDIA_DOMAIN)
                     & F.col("meta.id").isNotNull())
        .select(
            F.col("meta.id").alias("edit_id"),
            "event_time",
            F.to_date("event_time").alias("event_date"),
            "wiki",
            domain.alias("domain"),
            F.split(domain, r"\.").getItem(0).alias("language"),
            "namespace",
            (F.col("namespace") == 0).alias("is_article"),
            "title",
            user.alias("user_name"),
            (user.rlike(IPV4) | user.rlike(IPV6) | user.rlike(TEMP_ACCOUNT)).alias("is_anonymous"),
            F.coalesce("bot", F.lit(False)).alias("is_bot"),
            F.coalesce("minor", F.lit(False)).alias("is_minor"),
            F.col("type").alias("edit_type"),
            (F.coalesce("length.new", F.lit(0)) - F.coalesce("length.old", F.lit(0))).alias("byte_delta"),
            F.col("length.old").alias("length_old"),
            F.col("length.new").alias("length_new"),
            F.col("revision.old").alias("revision_old"),
            F.col("revision.new").alias("revision_new"),
            F.col("meta.uri").alias("page_url"),
            "kafka_partition",
            "kafka_offset",
            F.col("ingested_at").alias("bronze_ingested_at"),
            F.current_timestamp().alias("silver_processed_at"),
        )
    )


def deduplicate(silver: DataFrame) -> DataFrame:
    """Drop replayed events (same meta.id) seen within the watermark; state older than it is evicted."""
    return silver.withWatermark("event_time", WATERMARK).dropDuplicatesWithinWatermark(["edit_id"])
