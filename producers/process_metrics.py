import os
import socket
import time
import psutil
import logging
from typing import List, Dict, Any

from common.producer import (
    GracefulShutdown,
    build_event,
    build_producer,
    configure_logging,
    send_with_retry,
    shutdown_producer,
)


TOPIC = os.getenv("KAFKA_TOPIC", "process.metrics")
SAMPLE_INTERVAL = int(os.getenv("PROCESS_METRICS_INTERVAL", "5"))
HOST = os.getenv("HOSTNAME", socket.gethostname())
ENVIRONMENT = os.getenv("ENVIRONMENT", "lab")
NODE_NAME = os.getenv("NODE_NAME", HOST)

LOGGER = logging.getLogger(__name__)


def collect_processes() -> Dict[str, Any]:
    processes = []

    for p in psutil.process_iter(["pid", "name", "cpu_percent", "memory_percent"]):
        try:
            info = p.info
            processes.append(info)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    top_cpu = sorted(processes, key=lambda x: x["cpu_percent"], reverse=True)[:10]
    top_mem = sorted(processes, key=lambda x: x["memory_percent"], reverse=True)[:10]

    return {
        "top_cpu_processes": top_cpu,
        "top_memory_processes": top_mem,
        "process_count": len(processes),
    }


def run() -> None:
    configure_logging()
    GracefulShutdown.install()
    producer = build_producer()

    LOGGER.info("process metrics producer started")

    while not GracefulShutdown.stopped:
        try:
            metrics = collect_processes()
            event = build_event(
                event_type="process.metrics",
                source="psutil",
                metrics=metrics,
                host=HOST,
                tags={
                    "env": ENVIRONMENT,
                    "node": NODE_NAME,
                },
            )
            send_with_retry(producer, TOPIC, event)
            LOGGER.info("sent process metrics")
        except Exception as e:
            LOGGER.exception("failed sending process metrics: %s", str(e))
        time.sleep(SAMPLE_INTERVAL)

    shutdown_producer(producer)


if __name__ == "__main__":
    run()
