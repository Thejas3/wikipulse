"""Read the whole raw topic and report volume, duplicates, and event-time gaps.

Used for the Milestone 1 exit checks: measure the live rate, and show that a producer
restart resumes without gaps (replayed duplicates are expected; holes are not).

    python audit.py [--bootstrap 127.0.0.1:29092] [--topic wiki.recentchange.raw]
"""

import argparse
import json
from collections import Counter
from datetime import datetime

from confluent_kafka import Consumer, TopicPartition


def read_topic(bootstrap: str, topic: str):
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "wikipulse-audit",  # offsets are assigned explicitly, never committed
        "enable.auto.commit": False,
    })
    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    ends = {}
    assignment = []
    for p in partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        if high > low:
            ends[p] = high
            assignment.append(TopicPartition(topic, p, low))
    consumer.assign(assignment)
    while ends:
        msg = consumer.poll(5)
        if msg is None:
            break
        if msg.error():
            raise RuntimeError(msg.error())
        yield json.loads(msg.value())
        if msg.offset() + 1 >= ends.get(msg.partition(), 0):
            ends.pop(msg.partition(), None)
    consumer.close()


def summarize(events: list[dict], gap_threshold: float = 10.0) -> dict:
    ids = Counter(e["meta"]["id"] for e in events)
    times = sorted(datetime.fromisoformat(e["meta"]["dt"].replace("Z", "+00:00")).timestamp()
                   for e in events)
    gaps = sorted(((b - a, a) for a, b in zip(times, times[1:])), reverse=True)
    span = times[-1] - times[0] if len(times) > 1 else 0
    return {
        "events": len(events),
        "distinct": len(ids),
        "duplicates": sum(n - 1 for n in ids.values()),
        "span_seconds": round(span),
        "events_per_second": round(len(ids) / span, 1) if span else None,
        "largest_gaps": [
            {"seconds": round(g, 1), "after": datetime.fromtimestamp(t).isoformat(timespec="seconds")}
            for g, t in gaps[:3]
        ],
        "gaps_over_threshold": sum(g > gap_threshold for g, _ in gaps),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="127.0.0.1:29092")
    parser.add_argument("--topic", default="wiki.recentchange.raw")
    args = parser.parse_args()
    events = list(read_topic(args.bootstrap, args.topic))
    print(json.dumps(summarize(events), indent=2))


if __name__ == "__main__":
    main()
