from wikipulse.stats import EditStats, parse_event
from wikipulse.stream import SSEParser


def event(**overrides):
    data = {
        "meta": {"uri": "https://en.wikipedia.org/wiki/Python", "domain": "en.wikipedia.org"},
        "wiki": "enwiki",
        "title": "Python",
        "user": "Alice",
        "bot": False,
        "type": "edit",
        "namespace": 0,
        "timestamp": 1_000_000,
        "length": {"old": 100, "new": 150},
    }
    data.update(overrides)
    return data


def test_parse_event_fields():
    edit = parse_event(event())
    assert edit.wiki == "enwiki"
    assert edit.delta == 50
    assert edit.url == "https://en.wikipedia.org/wiki/Python"


def test_parse_event_skips_canary():
    assert parse_event(event(meta={"domain": "canary"})) is None


def test_parse_event_handles_missing_length():
    assert parse_event(event(length=None, type="log")).delta == 0


def test_snapshot_aggregates():
    stats = EditStats()
    now = 1_000_000
    stats.started = now - 3600
    for i in range(3):
        stats.add(parse_event(event(timestamp=now - i)))
    stats.add(parse_event(event(title="Bot page", bot=True, wiki="dewiki", timestamp=now)))
    stats.add(parse_event(event(title="User:X", namespace=2, timestamp=now)))
    stats.add(parse_event(event(
        title="Q42", wiki="wikidatawiki", timestamp=now,
        meta={"uri": "https://www.wikidata.org/wiki/Q42", "domain": "www.wikidata.org"},
    )))

    snap = stats.snapshot(now=now)
    assert snap["total"] == 6
    assert snap["edits_per_minute"] == 6
    assert snap["bot_share"] == round(1 / 6, 3)
    assert snap["active_wikis"] == 3
    assert snap["top_articles"][0] == {
        "wiki": "enwiki", "title": "Python", "edits": 3, "url": "https://en.wikipedia.org/wiki/Python",
    }
    titles = {a["title"] for a in snap["top_articles"]}
    assert "User:X" not in titles  # non-article namespace
    assert "Q42" not in titles  # Wikidata item, not a Wikipedia article
    assert len(snap["per_minute"]) == 60
    assert sum(p["human"] + p["bot"] for p in snap["per_minute"]) == 6


def test_per_minute_starts_at_server_start():
    stats = EditStats()
    stats.started = 1_000_000 - 150  # started 2.5 minutes ago, mid-minute
    snap = stats.snapshot(now=1_000_000)
    minutes = snap["per_minute"]
    assert len(minutes) == 3
    assert [m["partial"] for m in minutes] == [True, False, True]


def test_snapshot_prunes_old_edits():
    stats = EditStats(window=300)
    stats.add(parse_event(event(timestamp=1_000_000)))
    snap = stats.snapshot(now=1_000_000 + 301)
    assert snap["edits_per_minute"] == 0
    assert snap["top_articles"] == []
    assert snap["total"] == 1


def test_sse_parser():
    parser = SSEParser()
    lines = [":ok", "", "event: message", "id: abc", 'data: {"a":', "data: 1}", ""]
    out = [d for d in map(parser.feed, lines) if d is not None]
    assert out == ['{"a":\n1}']
    assert parser.last_id == "abc"
