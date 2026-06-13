import os
import socket
import time
from datetime import datetime, timezone
import docker
import logging
import threading
import http.server
import json
from typing import Dict, Any

from common.producer import (
    GracefulShutdown,
    build_event,
    build_producer,
    configure_logging,
    send_with_retry,
    shutdown_producer,
)


TOPIC = os.getenv("KAFKA_TOPIC", "container.metrics")
SAMPLE_INTERVAL = int(os.getenv("CONTAINER_METRICS_INTERVAL", "5"))
HOST = os.getenv("HOSTNAME", socket.gethostname())
ENVIRONMENT = os.getenv("ENVIRONMENT", "lab")
NODE_NAME = os.getenv("NODE_NAME", HOST)

client = docker.from_env()
LOGGER = logging.getLogger(__name__)

# Global variable to track the timestamp of the last successful metric send.
last_sent_time: str | None = None


def get_container_stats(container: Any) -> Dict[str, Any]:
    stats = container.stats(stream=False)

    cpu_delta = (
        stats["cpu_stats"]["cpu_usage"]["total_usage"]
        - stats["precpu_stats"]["cpu_usage"]["total_usage"]
    )
    system_delta = (
        stats["cpu_stats"].get("system_cpu_usage", 0)
        - stats["precpu_stats"].get("system_cpu_usage", 0)
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
        "timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }


def _start_health_server(port: int):
    class HealthRequestHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/health":
                status = "ok" if not GracefulShutdown.stopped else "stopping"
                response = {
                    "status": status,
                    "last_sent": last_sent_time or "unknown",
                }
                payload = json.dumps(response).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            else:
                self.send_error(404)

        def log_message(self, format: str, *args) -> None:  # pragma: no cover
            # Suppress default logging to keep console clean.
            return

    server = http.server.HTTPServer(("0.0.0.0", port), HealthRequestHandler)
    LOGGER.info("Health check server listening on %s:%d", "0.0.0.0", port)
    server.serve_forever()


def run() -> None:
    configure_logging()
    GracefulShutdown.install()
    producer = build_producer()

    # Start health check HTTP server in a daemon thread
    health_port = int(os.getenv("HEALTH_PORT", "8000"))
    threading.Thread(target=_start_health_server, args=(health_port,), daemon=True).start()

    LOGGER.info("container metrics producer started")

    while not GracefulShutdown.stopped:
        containers = client.containers.list(all=True)

        for container in containers:
            try:
                metrics = get_container_stats(container)
                event = build_event(
                    event_type="container.metrics",
                    source="docker",
                    metrics=metrics,
                    host=HOST,
                    tags={
                        "env": ENVIRONMENT,
                        "node": NODE_NAME,
                        "container_id": metrics["container_id"],
                        "container_name": metrics["name"],
                    },
                )
                send_with_retry(producer, TOPIC, event)
                # Update last sent timestamp for health check
                global last_sent_time
                last_sent_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            except Exception as e:
                LOGGER.exception(
                    "failed to collect or send metrics for container %s: %s",
                    container.name,
                    str(e)
                )

        time.sleep(SAMPLE_INTERVAL)

    shutdown_producer(producer)


if __name__ == "__main__":
    run()
