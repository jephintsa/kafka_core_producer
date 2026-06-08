import os
import socket
import time
import psutil
import logging
import datetime

from common.producer import (
    GracefulShutdown,
    build_event,
    build_producer,
    configure_logging,
    send_with_retry,
    shutdown_producer,
)


TOPIC = os.getenv("KAFKA_TOPIC", "system.disk.metrics")
SAMPLE_INTERVAL = int(os.getenv("DISK_METRICS_INTERVAL", "5"))
HOST = os.getenv("HOSTNAME", socket.gethostname())
ENVIRONMENT = os.getenv("ENVIRONMENT", "lab")
NODE_NAME = os.getenv("NODE_NAME", HOST)

LOGGER = logging.getLogger(__name__)


def collect_disk():
    disk = psutil.disk_usage("/")
    io = psutil.disk_io_counters()

    return {
        "disk": {
            "total_gb": disk.total / (1024**3),
            "used_gb": disk.used / (1024**3),
            "free_gb": disk.free / (1024**3),
            "used_percent": disk.percent,
        },
        "io": {
            "read_bytes": io.read_bytes if io else 0,
            "write_bytes": io.write_bytes if io else 0,
            "read_count": io.read_count if io else 0,
            "write_count": io.write_count if io else 0,
        },
    }


def run():
    configure_logging()
    GracefulShutdown.install()
    producer = build_producer()

    LOGGER.info("disk metrics producer started")

    while not GracefulShutdown.stopped:
        try:
            metrics = collect_disk()
            event = build_event(
                event_type="system.disk.metrics",
                source="psutil",
                metrics=metrics,
                host=HOST,
                tags={
                    "env": ENVIRONMENT,
                    "node": NODE_NAME,
                },
            )
            send_with_retry(producer, TOPIC, event)
            LOGGER.info("sent disk metrics")
        except Exception:
            LOGGER.exception("failed sending disk metrics")
        time.sleep(SAMPLE_INTERVAL)

    shutdown_producer(producer)


if __name__ == "__main__":
    run()
