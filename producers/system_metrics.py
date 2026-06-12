import os
import socket
import time
import psutil
import logging

from common.producer import (
    GracefulShutdown,
    build_event,
    build_producer,
    configure_logging,
    send_with_retry,
    shutdown_producer,
)


TOPIC = os.getenv("KAFKA_TOPIC", "system.metrics")
SAMPLE_INTERVAL = int(os.getenv("SYSTEM_METRICS_INTERVAL", "2"))
HOST = os.getenv("HOSTNAME", socket.gethostname())
ENVIRONMENT = os.getenv("ENVIRONMENT", "lab")
NODE_NAME = os.getenv("NODE_NAME", HOST)

LOGGER = logging.getLogger(__name__)


def collect_metrics():
    cpu_per_core = psutil.cpu_percent(interval=1, percpu=True)
    memory = psutil.virtual_memory()
    load = psutil.getloadavg()

    return {
        "cpu_total_percent": sum(cpu_per_core) / len(cpu_per_core),
        "cpu_per_core": cpu_per_core,
        "memory_used_percent": memory.percent,
        "memory_available_mb": memory.available / (1024 * 1024),
        "memory_total_mb": memory.total / (1024 * 1024),
        "load_1m": load[0],
        "load_5m": load[1],
        "load_15m": load[2],
    }


def run():
    configure_logging()
    GracefulShutdown.install()
    producer = build_producer()

    LOGGER.info("system metrics producer started")

    while not GracefulShutdown.stopped:
        try:
            metrics = collect_metrics()
            event = build_event(
                event_type="system.metrics",
                source="psutil",
                metrics=metrics,
                host=HOST,
                tags={
                    "env": ENVIRONMENT,
                    "node": NODE_NAME,
                },
            )
            send_with_retry(producer, TOPIC, event)
            LOGGER.info("sent system metrics")
        except Exception:
            LOGGER.exception("failed sending system metrics")
        time.sleep(SAMPLE_INTERVAL)

    shutdown_producer(producer)


if __name__ == "__main__":
    run()
