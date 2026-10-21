import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import Config, build_spark  # noqa: E402
from gold_job import Gold, latency_histogram, per_minute_counts, score_spikes, trending_counts  # noqa: E402

SILVER = ("event_time timestamp, wiki string, domain string, title string, user_name string, "
          "is_bot boolean, is_article boolean, byte_delta long")
T = datetime(2026, 10, 20, 12, 2, 30)
W = T.replace(second=0)  # window-aligned start of the candidate window


@pytest.fixture(scope="module")
def spark():
    session = build_spark("gold-tests", s3=False)
    yield session
    session.stop()


def edit(t=T, title="Python", user="Alice", bot=False, article=True, delta=10, wiki="enwiki"):
    return (t, wiki, "en.wikipedia.org", title, user, bot, article, delta)


def test_per_minute_counts(spark):
    df = spark.createDataFrame([edit(), edit(bot=True, delta=-40), edit(t=T + timedelta(minutes=1))], SILVER)
    rows = {r.window_start.minute: r for r in per_minute_counts(df).collect()}
    assert (rows[2].edit_count, rows[2].human_count, rows[2].bot_count) == (2, 1, 1)
    assert rows[2].bytes_added == 10  # only positive deltas count as added
    assert rows[3].edit_count == 1


def test_trending_uses_five_sliding_windows_and_articles_only(spark):
    df = spark.createDataFrame([edit(), edit(user="Bob"), edit(title="Talk:Python", article=False)], SILVER)
    rows = trending_counts(df).collect()
    assert [r.window_start.strftime("%H:%M") for r in sorted(rows, key=lambda r: r.window_start)] == \
        ["11:58", "11:59", "12:00", "12:01", "12:02"]
    assert all(r.title == "Python" and r.edit_count == 2 and r.editors == ["Alice", "Bob"] for r in rows)


def candidates_df(spark, count, editors=3, start=None):
    start = start or W
    return spark.createDataFrame(
        [(start, start + timedelta(minutes=5), "enwiki", "en.wikipedia.org", "Python", count, editors)],
        "window_start timestamp, window_end timestamp, wiki string, domain string, title string, "
        "edit_count long, unique_editors int")


def buckets_df(spark, counts_by_minutes_ago):
    start = T.replace(second=0)
    return spark.createDataFrame(
        [("enwiki", "Python", start - timedelta(minutes=m), n) for m, n in counts_by_minutes_ago.items()],
        "wiki string, title string, window_start timestamp, edit_count long")


def test_spike_flagged_against_quiet_baseline(spark):
    history = W - timedelta(hours=24)
    rows = score_spikes(candidates_df(spark, 12), buckets_df(spark, {60: 1, 120: 1}), history).collect()
    assert len(rows) == 1
    spike = rows[0]
    assert spike.baseline_buckets == 288
    assert spike.baseline_mean == pytest.approx(2 / 288)
    assert spike.baseline_std == 1.0  # floored
    assert spike.z_score > 3


def test_busy_page_is_not_a_spike(spark):
    # The page usually gets ~10 edits per 5 minutes, so 12 is normal.
    busy = {m: 10 if m % 10 else 14 for m in range(5, 24 * 60, 5)}
    assert score_spikes(candidates_df(spark, 12), buckets_df(spark, busy), W - timedelta(hours=24)).count() == 0


def test_no_spikes_until_an_hour_of_history(spark):
    assert score_spikes(candidates_df(spark, 50), buckets_df(spark, {}), W - timedelta(minutes=30)).count() == 0
    assert score_spikes(candidates_df(spark, 50), buckets_df(spark, {}), W - timedelta(minutes=60)).count() == 1


def test_bucket_overlapping_the_window_is_excluded_from_baseline(spark):
    # The bucket starting 2 minutes before the window overlaps it, so it must not count.
    rows = score_spikes(candidates_df(spark, 12), buckets_df(spark, {2: 100}), W - timedelta(hours=24)).collect()
    assert rows[0].baseline_mean == 0


def test_latency_histogram_buckets(spark):
    df = spark.createDataFrame([edit(t=T - timedelta(seconds=s)) for s in (0.5, 4, 4, 70, 99999)], SILVER)
    hist = {r.le_seconds: r.events for r in latency_histogram(df, T).collect()}
    assert hist == {1.0: 1, 5.0: 2, 70.0: 1, float("inf"): 1}


def test_replayed_batch_is_not_counted_twice(spark, tmp_path):
    gold = Gold(spark, Config())
    gold.paths = {name: str(tmp_path / name) for name in gold.paths}
    gold.ensure_tables()
    batch = spark.createDataFrame([edit(), edit(user="Bob"), edit(wiki="dewiki")], SILVER)

    gold.process(batch, batch_id=0)
    gold.latency_buffer = []  # a crash drops buffered measurements; the replay re-measures
    gold.process(batch, batch_id=0)  # crash and replay of the same micro-batch
    epm = spark.read.format("delta").load(gold.paths["edits_per_minute"])
    assert epm.where("wiki = 'enwiki'").collect()[0].edit_count == 2

    later = spark.createDataFrame([edit(user="Carol")], SILVER)
    gold.process(later, batch_id=1)
    gold._flush_latency(1)
    assert epm.where("wiki = 'enwiki'").collect()[0].edit_count == 3
    trending = spark.read.format("delta").load(gold.paths["trending_pages"]).where("wiki = 'enwiki'")
    assert {r.unique_editors for r in trending.collect()} == {3}
    hist = spark.read.format("delta").load(gold.paths["latency_histogram"])
    assert hist.groupBy().sum("events").collect()[0][0] == 4  # 3 + 1 events, the replay skipped
