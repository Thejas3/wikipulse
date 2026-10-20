"""Wikimedia recentchange SSE feed -> Kafka.

Delivery is at-least-once: the resume point (the SSE Last-Event-ID) is only written to
disk after Kafka has acknowledged every event up to it, so a crash can replay a few
events but never skip any. Silver deduplicates replays on meta.id.
"""

import json
import logging
import os
import signal
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx
from httpx_sse import SSEError, connect_sse
from jsonschema import Draft202012Validator, FormatChecker

import metrics

log = logging.getLogger("producer")
ROOT = Path(__file__).resolve().parent


@dataclass(frozen=True)
class Config:
    stream_url: str = os.getenv("STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
    bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
    raw_topic: str = os.getenv("TOPIC_RAW", "wiki.recentchange.raw")
    dlq_topic: str = os.getenv("TOPIC_DLQ", "wiki.recentchange.dlq")
    checkpoint_path: str = os.getenv("CHECKPOINT_PATH", str(ROOT / ".state" / "last_event_id"))
    checkpoint_every: int = int(os.getenv("CHECKPOINT_EVERY", "500"))
    schema_path: str = os.getenv("SCHEMA_PATH", str(ROOT.parent / "contracts" / "recentchange.schema.json"))
    metrics_port: int = int(os.getenv("METRICS_PORT", "9108"))
    contact: str = os.getenv("WIKIPULSE_CONTACT", "local development")

    @property
    def user_agent(self) -> str:
        return f"WikiPulse-producer/0.1 ({self.contact})"


class Checkpoint:
    """The last SSE event ID whose event (and every earlier one) Kafka has acknowledged."""

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def load(self) -> str | None:
        try:
            value = self.path.read_text().strip()
        except FileNotFoundError:
            return None
        return value or None

    def save(self, event_id: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(event_id)
        tmp.replace(self.path)  # atomic: a crash leaves the old or the new value, never half


def load_validator(schema_path: str | Path) -> Draft202012Validator:
    schema = json.loads(Path(schema_path).read_text())
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


@dataclass
class Router:
    """Validates one SSE message and publishes it to the raw topic or the dead-letter topic."""

    kafka: object  # confluent_kafka.Producer, or a fake with produce()/poll()/flush()
    validator: Draft202012Validator
    raw_topic: str
    dlq_topic: str
    delivery_errors: list = field(default_factory=list)

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.delivery_errors.append(err)
            metrics.DELIVERY_ERRORS.inc()

    def route(self, data: str) -> bool:
        """Returns True if the event was valid."""
        try:
            event = json.loads(data)
            errors = sorted(self.validator.iter_errors(event), key=lambda e: list(e.path))
        except json.JSONDecodeError as exc:
            event, errors = None, [exc]

        if errors:
            reason = "; ".join(str(getattr(e, "message", e)) for e in errors[:3])
            self.kafka.produce(
                self.dlq_topic,
                value=json.dumps({"error": reason, "raw": data, "rejected_at": time.time()}),
                on_delivery=self._on_delivery,
            )
            metrics.EVENTS.labels(result="dlq").inc()
            log.debug("rejected event: %s", reason)
        else:
            self.kafka.produce(
                self.raw_topic,
                key=event["wiki"],  # one wiki's events stay ordered in one partition
                value=data,
                on_delivery=self._on_delivery,
            )
            metrics.EVENTS.labels(result="valid").inc()
            metrics.record_event_time(event["timestamp"])
        self.kafka.poll(0)  # serve delivery callbacks
        return not errors


class Producer:
    def __init__(self, config: Config, kafka, checkpoint: Checkpoint, validator: Draft202012Validator):
        self.config = config
        self.kafka = kafka
        self.checkpoint = checkpoint
        self.router = Router(kafka, validator, config.raw_topic, config.dlq_topic)
        self.last_id: str | None = checkpoint.load()
        self.since_checkpoint = 0
        self.stopping = False

    def commit(self) -> None:
        """Wait for Kafka to acknowledge everything sent, then advance the resume point."""
        remaining = self.kafka.flush(30)
        if remaining or self.router.delivery_errors:
            errors, self.router.delivery_errors = self.router.delivery_errors, []
            raise RuntimeError(f"Kafka did not acknowledge {remaining} events; errors: {errors[:3]}")
        if self.last_id:
            self.checkpoint.save(self.last_id)
            metrics.CHECKPOINTS.inc()
        self.since_checkpoint = 0

    def handle(self, event_id: str | None, data: str) -> None:
        if not data:
            return
        self.router.route(data)
        if event_id:
            self.last_id = event_id
        self.since_checkpoint += 1
        if self.since_checkpoint >= self.config.checkpoint_every:
            self.commit()

    def run(self) -> None:
        backoff = 1.0
        while not self.stopping:
            headers = {"User-Agent": self.config.user_agent}
            if self.last_id:
                headers["Last-Event-ID"] = self.last_id
            try:
                timeout = httpx.Timeout(10.0, read=60.0)
                with httpx.Client(timeout=timeout) as client:
                    with connect_sse(client, "GET", self.config.stream_url, headers=headers) as source:
                        source.response.raise_for_status()
                        log.info("connected (resume from %s)", "checkpoint" if self.last_id else "now")
                        backoff = 1.0
                        for sse in source.iter_sse():
                            self.handle(sse.id or None, sse.data)
                            if self.stopping:
                                break
            except (httpx.HTTPError, SSEError) as exc:
                metrics.RECONNECTS.inc()
                log.warning("stream error (%s); reconnecting in %.0fs", exc, backoff)
                self._sleep(backoff)
                backoff = min(backoff * 2, 60.0)
        self.commit()
        log.info("stopped cleanly; checkpoint saved")

    def _sleep(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while not self.stopping and time.monotonic() < deadline:
            time.sleep(0.2)


def main() -> None:
    from confluent_kafka import Producer as KafkaProducer

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config()
    kafka = KafkaProducer({
        "bootstrap.servers": config.bootstrap,
        "acks": "all",
        "enable.idempotence": True,  # broker drops retried duplicates
        "compression.type": "zstd",
        "linger.ms": 50,
        "client.id": "wikipulse-producer",
    })
    producer = Producer(config, kafka, Checkpoint(config.checkpoint_path), load_validator(config.schema_path))

    def stop(signum, _frame):
        log.info("received signal %s, stopping", signum)
        producer.stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    metrics.serve(config.metrics_port)
    log.info("metrics on :%d, publishing to %s", config.metrics_port, config.bootstrap)
    producer.run()


if __name__ == "__main__":
    main()
