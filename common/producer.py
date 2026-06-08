import json
import logging
import os
import signal
import sys
import time
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import KafkaError

LOGGER = logging.getLogger(__name__)

DEFAULT_BROKER = "192.168.2.110:9092"
DEFAULT_RETRIES = 5
DEFAULT_BACKOFF_SECONDS = 1
DEFAULT_LOG_LEVEL = "INFO"


class GracefulShutdown:
    stopped = False

    @classmethod
    def install(cls):
        signal.signal(signal.SIGINT, cls._handle)
        signal.signal(signal.SIGTERM, cls._handle)

    @classmethod
    def _handle(cls, signum, frame):
        LOGGER.info("received signal %s, stopping", signum)
        cls.stopped = True


def configure_logging(level: str | None = None) -> None:
    if level is None:
        level = os.getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL)
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def get_env(key: str, default: str | None = None) -> str:
    value = os.getenv(key, default)
    if value is None:
        raise KeyError(f"Required environment variable '{key}' is not set")
    return value


def build_producer() -> KafkaProducer:
    brokers = os.getenv("KAFKA_BROKER", DEFAULT_BROKER)
    LOGGER.debug("creating Kafka producer for brokers=%s", brokers)
    return KafkaProducer(
        bootstrap_servers=brokers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=int(os.getenv("KAFKA_PRODUCER_RETRIES", DEFAULT_RETRIES)),
        linger_ms=int(os.getenv("KAFKA_PRODUCER_LINGER_MS", "0")),
        acks=os.getenv("KAFKA_PRODUCER_ACKS", "all"),
    )


def build_event(
    event_type: str,
    source: str,
    metrics: dict,
    tags: dict | None = None,
    host: str | None = None,
    timestamp: str | None = None,
) -> dict:
    if tags is None:
        tags = {}
    if timestamp is None:
        timestamp = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    event = {
        "event_type": event_type,
        "timestamp": timestamp,
        "source": source,
        "metrics": metrics,
        "tags": tags,
    }
    if host is not None:
        event["host"] = host
    return event


def send_with_retry(producer: KafkaProducer, topic: str, event: dict) -> None:
    max_retries = int(os.getenv("KAFKA_SEND_MAX_RETRIES", DEFAULT_RETRIES))
    backoff_seconds = int(os.getenv("KAFKA_SEND_BACKOFF_SECONDS", DEFAULT_BACKOFF_SECONDS))
    attempt = 0

    while attempt <= max_retries:
        try:
            producer.send(topic, event)
            producer.flush()
            LOGGER.debug("sent event to %s", topic)
            return
        except KafkaError as exc:
            attempt += 1
            if attempt > max_retries:
                LOGGER.exception("failed to send event after %d attempts", attempt)
                raise
            sleep_time = backoff_seconds * (2 ** (attempt - 1))
            LOGGER.warning(
                "Kafka send failed (attempt %d/%d): %s; retrying in %s seconds",
                attempt,
                max_retries,
                exc,
                sleep_time,
            )
            time.sleep(sleep_time)


def shutdown_producer(producer: KafkaProducer) -> None:
    try:
        producer.flush()
        producer.close()
        LOGGER.info("Kafka producer shut down")
    except Exception:
        LOGGER.exception("error shutting down Kafka producer")


if __name__ == "__main__":
    configure_logging()
    GracefulShutdown.install()
    LOGGER.info("common producer module loaded")
    sys.exit(0)
