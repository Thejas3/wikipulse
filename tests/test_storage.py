from wikipulse.stats import EditStats
from wikipulse.storage import MinuteStore


def test_save_load_and_prune(tmp_path):
    store = MinuteStore(tmp_path / "test.db")
    store.save([(100, 5, 1), (101, 7, 2)])
    store.save([(101, 9, 3)])  # later flush of the same minute replaces it
    assert store.load(since_minute=0) == {100: [5, 1], 101: [9, 3]}
    assert store.load(since_minute=101) == {101: [9, 3]}
    store.prune(before_minute=101)
    assert store.load(since_minute=0) == {101: [9, 3]}


def test_complete_minutes_come_from_run_coverage(tmp_path):
    store = MinuteStore(tmp_path / "test.db")
    run = store.start_run(started=6030)  # mid-minute 100
    store.touch_run(run, now=6250)  # stopped mid-minute 104
    assert store.complete_minutes(since_minute=0) == {101, 102, 103}
    assert store.complete_minutes(since_minute=102) == {102, 103}


def test_take_dirty_returns_changed_minutes_once():
    stats = EditStats()

    class E:  # minimal stand-in for an Edit
        def __init__(self, ts, bot=False):
            self.ts, self.bot = ts, bot

    stats.started = 6000
    stats.add(E(6000))
    stats.add(E(6001, bot=True))
    stats.add(E(6060))
    assert stats.take_dirty() == [(100, 1, 1), (101, 1, 0)]
    assert stats.take_dirty() == []

    stats.add(E(5990))  # late event from before the run started
    assert stats.take_dirty() == []
    assert stats.total == 4  # still counted overall


def test_history_from_earlier_run_is_shown_with_gaps_left_out():
    now = 1_000_000  # minute 16666
    stats = EditStats(
        history={16620: [10, 5], 16621: [12, 6], 16622: [3, 1]},
        history_complete={16620, 16621},
    )
    stats.started = now - 150  # this run began in minute 16664, mid-minute

    points = {p["minute"] // 60: p for p in stats.snapshot(now=now)["per_minute"]}
    assert points[16620]["partial"] is False
    assert points[16622]["partial"] is True  # previous run stopped during this minute
    assert 16630 not in points  # server was down: unknown, not zero
    assert points[16664]["partial"] is True  # this run started mid-minute
    assert points[16665] == {"minute": 16665 * 60, "human": 0, "bot": 0, "partial": False}
    assert points[16666]["partial"] is True  # in progress


def test_restart_round_trip(tmp_path):
    store = MinuteStore(tmp_path / "test.db")
    first = EditStats()
    first.started = 6000

    class E:
        def __init__(self, ts):
            self.ts, self.bot = ts, False

    for ts in (6000, 6010, 6070):
        first.add(E(ts))
    store.save(first.take_dirty())

    second = EditStats(history=store.load(since_minute=0))
    second.started = 6072
    second.add(E(6075))  # same minute keeps accumulating after a quick restart
    assert second.take_dirty() == [(101, 2, 0)]
