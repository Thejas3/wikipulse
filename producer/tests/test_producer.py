import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from audit import summarize  # noqa: E402
from producer import Checkpoint, Config, Producer, Router, load_validator  # noqa: E402

SCHEMA = Path(__file__).resolve().parents[2] / "contracts" / "recentchange.schema.json"


def event(**overrides) -> dict:
    data = {
        "meta": {"id": "a1", "dt": "2026-10-19T20:00:00Z", "domain": "en.wikipedia.org"},
        "type": "edit", "wiki": "enwiki", "namespace": 0, "title": "Python",
        "user": "Alice", "bot": False, "minor": False, "timestamp": 1792440000,
        "length": {"old": 100, "new": 150}, "revision": {"old": 1, "new": 2},
    }
    data.update(overrides)
    return data


class FakeKafka:
    def __init__(self, fail=False, unacked=0):
        self.sent, self.fail, self.unacked, self.flushes = [], fail, unacked, 0

    def produce(self, topic, value, key=None, on_delivery=None):
        self.sent.append((topic, key, value))
        if on_delivery:
            on_delivery("broker down" if self.fail else None, None)

    def poll(self, _timeout):
        return 0

    def flush(self, _timeout):
        self.flushes += 1
        return self.unacked


@pytest.fixture
def validator():
    return load_validator(SCHEMA)


def test_valid_event_goes_to_raw_keyed_by_wiki(validator):
    kafka = FakeKafka()
    assert Router(kafka, validator, "raw", "dlq").route(json.dumps(event()))
    topic, key, _ = kafka.sent[0]
    assert (topic, key) == ("raw", "enwiki")


@pytest.mark.parametrize("bad", [
    "not json",
    json.dumps(event(meta={"id": "x", "domain": "en.wikipedia.org"})),  # no meta.dt
    json.dumps(event(type="mystery")),
    json.dumps(event(timestamp="yesterday")),
])
def test_invalid_event_goes_to_dlq_with_reason(validator, bad):
    kafka = FakeKafka()
    assert not Router(kafka, validator, "raw", "dlq").route(bad)
    topic, _, value = kafka.sent[0]
    assert topic == "dlq"
    payload = json.loads(value)
    assert payload["raw"] == bad and payload["error"]


def test_checkpoint_round_trip(tmp_path):
    cp = Checkpoint(tmp_path / "state" / "last_event_id")
    assert cp.load() is None
    cp.save('[{"topic":"x","offset":5}]')
    assert cp.load() == '[{"topic":"x","offset":5}]'


def make_producer(tmp_path, validator, kafka, every=2):
    config = Config(checkpoint_every=every, checkpoint_path=str(tmp_path / "cp"))
    return Producer(config, kafka, Checkpoint(config.checkpoint_path), validator)


def test_checkpoint_written_only_after_kafka_acks(tmp_path, validator):
    kafka = FakeKafka()
    producer = make_producer(tmp_path, validator, kafka)
    producer.handle("id-1", json.dumps(event()))
    assert producer.checkpoint.load() is None  # not yet: below checkpoint_every
    producer.handle("id-2", json.dumps(event()))
    assert kafka.flushes == 1
    assert producer.checkpoint.load() == "id-2"


def test_checkpoint_not_advanced_when_kafka_has_unacked_events(tmp_path, validator):
    producer = make_producer(tmp_path, validator, FakeKafka(unacked=3))
    producer.handle("id-1", json.dumps(event()))
    with pytest.raises(RuntimeError):
        producer.handle("id-2", json.dumps(event()))
    assert producer.checkpoint.load() is None


def test_checkpoint_not_advanced_on_delivery_error(tmp_path, validator):
    producer = make_producer(tmp_path, validator, FakeKafka(fail=True), every=1)
    with pytest.raises(RuntimeError):
        producer.handle("id-1", json.dumps(event()))
    assert producer.checkpoint.load() is None


def test_resumes_from_saved_checkpoint(tmp_path, validator):
    Checkpoint(tmp_path / "cp").save("id-41")
    assert make_producer(tmp_path, validator, FakeKafka()).last_id == "id-41"


def test_audit_counts_duplicates_and_gaps():
    def ev(i, dt):
        return {"meta": {"id": f"e{i}", "dt": dt}}
    events = [ev(1, "2026-10-19T20:00:00Z"), ev(2, "2026-10-19T20:00:01Z"),
              ev(2, "2026-10-19T20:00:01Z"), ev(3, "2026-10-19T20:00:31Z")]
    report = summarize(events)
    assert report["duplicates"] == 1
    assert report["largest_gaps"][0]["seconds"] == 30.0
    assert report["gaps_over_threshold"] == 1
