"""PySpark unit tests on fixture events (run inside the streaming image: `make test-streaming`)."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Row  # noqa: E402

from common import build_spark  # noqa: E402
from transforms import deduplicate, parse_bronze, to_silver  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = build_spark("tests", s3=False)
    yield session
    session.stop()


def event(id="e1", **overrides) -> dict:
    data = {
        "meta": {"id": id, "dt": "2026-10-19T20:00:00Z", "domain": "en.wikipedia.org",
                 "uri": "https://en.wikipedia.org/wiki/Python"},
        "type": "edit", "wiki": "enwiki", "namespace": 0, "title": "Python", "user": "Alice",
        "bot": False, "minor": True, "timestamp": 1792440000,
        "length": {"old": 100, "new": 150}, "revision": {"old": 1, "new": 2},
    }
    data.update(overrides)
    return data


def bronze_df(spark, events):
    return spark.createDataFrame([
        Row(raw_json=json.dumps(e) if isinstance(e, dict) else e, kafka_partition=0, kafka_offset=i,
            ingested_at=None)
        for i, e in enumerate(events)
    ], "raw_json string, kafka_partition int, kafka_offset long, ingested_at timestamp")


def silver_rows(spark, events):
    return {r.edit_id: r for r in to_silver(parse_bronze(bronze_df(spark, events))).collect()}


def test_parses_and_derives_columns(spark):
    row = silver_rows(spark, [event()])["e1"]
    assert row.byte_delta == 50
    assert row.language == "en"
    assert row.is_article and row.is_minor and not row.is_bot and not row.is_anonymous
    assert str(row.event_time) == "2026-10-19 20:00:00"
    assert str(row.event_date) == "2026-10-19"


def test_keeps_only_wikipedia_edits_and_page_creations(spark):
    rows = silver_rows(spark, [
        event("edit"),
        event("new", type="new", length={"old": None, "new": 300}),
        event("log", type="log"),
        event("categorize", type="categorize"),
        event("commons", meta={"id": "commons", "dt": "2026-10-19T20:00:00Z", "domain": "commons.wikimedia.org"}),
        event("wikidata", meta={"id": "wikidata", "dt": "2026-10-19T20:00:00Z", "domain": "www.wikidata.org"}),
    ])
    assert set(rows) == {"edit", "new"}
    assert rows["new"].byte_delta == 300  # new page: no old length


@pytest.mark.parametrize("user,anonymous", [
    ("203.0.113.7", True),
    ("2001:db8::1", True),
    ("2001:0db8:85a3:0000:0000:8a2e:0370:7334", True),
    ("~2026-12345-67", True),  # temporary account
    ("Alice", False),
    ("User 1.2.3", False),
])
def test_anonymous_editor_detection(spark, user, anonymous):
    assert silver_rows(spark, [event(user=user)])["e1"].is_anonymous is anonymous


def test_event_time_falls_back_to_unix_timestamp(spark):
    e = event(meta={"id": "e1", "dt": "not a date", "domain": "en.wikipedia.org"}, timestamp=1792440060)
    assert str(silver_rows(spark, [e])["e1"].event_time) == "2026-10-19 20:01:00"


def test_malformed_json_does_not_break_the_batch(spark):
    assert set(silver_rows(spark, ["{not json", event()])) == {"e1"}


def test_streaming_dedup_across_micro_batches(spark, tmp_path):
    """A replayed event in a later micro-batch is dropped by the watermarked dedup state."""
    source, checkpoint, table = tmp_path / "src", tmp_path / "chk", tmp_path / "silver"
    source.mkdir()

    def run_batch(name, events):
        (source / f"{name}.txt").write_text("\n".join(json.dumps(e) for e in events))
        stream = (
            spark.readStream.format("text").load(str(source))
            .withColumnRenamed("value", "raw_json")
            .selectExpr("raw_json", "0 as kafka_partition", "0L as kafka_offset",
                        "current_timestamp() as ingested_at")
        )
        # A Delta sink, as in production: the second run restarts from the same checkpoint.
        query = (
            deduplicate(to_silver(parse_bronze(stream))).writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", str(checkpoint)).trigger(availableNow=True).start(str(table))
        )
        query.awaitTermination()

    run_batch("b1", [event("a"), event("b"), event("a")])  # duplicate inside one batch
    run_batch("b2", [event("a"), event("c")])  # replay of "a" after a restart
    ids = sorted(r.edit_id for r in spark.read.format("delta").load(str(table)).collect())
    assert ids == ["a", "b", "c"]
