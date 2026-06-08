import json
import os
import socket
import time
from datetime import datetime

import docker

from common.producer import (
    GracefulShutdown,
    build_event,
    build_producer,
    configure_logging,
    send_with_retry,
    shutdown_producer,
)


KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "container.metrics")
SAMPLE_INTERVAL = int(os.getenv("CONTAINER_METRICS_INTERVAL", "5"))
HOST = os.getenv("HOSTNAME", socket.gethostname())

client = docker.from_env()


def get_container_stats(container):
    stats = container.stats(stream=False)

    cpu_delta = (
        stats["cpu_stats"]["cpu_usage"]["total_usage"]
        - stats["precpu_stats"]["cpu_usage"]["total_usage"]
    )
    system_delta = (
        stats["cpu_stats"]["system_cpu_usage"]
        - stats["precpu_stats"]["system_cpu_usage"]
    )

    cpu_percent = 0.0
    if system_delta > 0:
        cpu_percent = (
            cpu_delta
            / system_delta
            * len(stats["cpu_stats"]["cpu_usage"].get("percpu_usage", [1]))
            * 100.0
        )

    memory_usage = stats["memory_stats"].get("usage", 0)
    memory_limit = stats["memory_stats"].get("limit", 1)
    memory_percent = (memory_usage / memory_limit) * 100.0

    return {
        "container_id": container.id[:12],
        "name": container.name,
        "status": container.status,
        "cpu_percent": round(cpu_percent, 2),
        "memory_usage": memory_usage,
        "memory_limit": memory_limit,
        "memory_percent": round(memory_percent, 2),
        "network": stats.get("networks", {}),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


def run():
    configure_logging()
    GracefulShutdown.install()
    producer = build_producer()

    print("🚀 container_metrics_edit producer started")

    while not GracefulShutdown.stopped:
        containers = client.containers.list(all=True)

        for container in containers:
            try:
                metrics = get_container_stats(container)
                event = build_event(
                    event_type="container.metrics",
                    source="docker",
                    metrics={
                        "host": HOST,
                        **metrics,
                    },
                    tags={
                        "container_id": metrics["container_id"],
                        "container_name": metrics["name"],
                    },
                )
                send_with_retry(producer, KAFKA_TOPIC, event)
            except Exception as exc:
                print(f"[ERROR] {container.name}: {exc}")

        time.sleep(SAMPLE_INTERVAL)

    shutdown_producer(producer)


if __name__ == "__main__":
    run()
