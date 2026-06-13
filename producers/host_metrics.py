import os
import socket
import time
import psutil
import logging
from datetime import datetime
from typing import Dict, Any

from common.producer import (
    GracefulShutdown,
    build_event,
    build_producer,
    configure_logging,
    send_with_retry,
    shutdown_producer,
)


TOPIC = os.getenv("KAFKA_TOPIC", "host.metrics")
SAMPLE_INTERVAL = int(os.getenv("HOST_METRICS_INTERVAL", "10"))
HOST = os.getenv("HOSTNAME", socket.gethostname())
ENVIRONMENT = os.getenv("ENVIRONMENT", "lab")
NODE_NAME = os.getenv("NODE_NAME", HOST)

LOGGER = logging.getLogger(__name__)


def collect_host() -> Dict[str, Any]:
    boot_time = psutil.boot_time()
    uptime = time.time() - boot_time

    return {
        "uptime_seconds": uptime,
        "process_count": len(psutil.pids()),
        "cpu_cores": psutil.cpu_count(),
        "load_1m": psutil.getloadavg()[0],
        "load_5m": psutil.getloadavg()[1],
        "load_15m": psutil.getloadavg()[2],
    }


def run() -> None:
    configure_logging()
    GracefulShutdown.install()
    producer = build_producer()

    LOGGER.info("host metrics producer started")

    while not GracefulShutdown.stopped:
        try:
            metrics = collect_host()
            event = build_event(
                event_type="host.metrics",
                source="psutil",
                metrics=metrics,
                host=HOST,
                tags={
                    "env": ENVIRONMENT,
                    "node": NODE_NAME,
                },
            )
            send_with_retry(producer, TOPIC, event)
            LOGGER.info("sent host metrics")
        except Exception as e:
            LOGGER.exception("failed sending host metrics: %s", str(e))
        time.sleep(SAMPLE_INTERVAL)

    shutdown_producer(producer)


if __name__ == "__main__":
    run()
