import docker
import time
import socket
from datetime import datetime
from kafka import KafkaProducer
import json

# -----------------------------
# CONFIG
# -----------------------------
KAFKA_BROKER = "192.168.2.110:9092"
TOPIC = "container.metrics"
HOST = socket.gethostname()

client = docker.from_env()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# -----------------------------
# Helpers
# -----------------------------
def get_container_stats(container):
    stats = container.stats(stream=False)

    cpu_delta = stats["cpu_stats"]["cpu_usage"]["total_usage"] - \
                stats["precpu_stats"]["cpu_usage"]["total_usage"]

    system_delta = stats["cpu_stats"]["system_cpu_usage"] - \
                   stats["precpu_stats"]["system_cpu_usage"]

    cpu_percent = 0.0
    if system_delta > 0:
        cpu_percent = (cpu_delta / system_delta) * len(
            stats["cpu_stats"]["cpu_usage"].get("percpu_usage", [1])
        ) * 100.0

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
        "network_rx": stats.get("networks", {}),
        "timestamp": datetime.utcnow().isoformat()
    }

# -----------------------------
# Producer Loop
# -----------------------------
def run():
    print("🚀 container.metrics producer started")

    while True:
        containers = client.containers.list(all=True)

        for container in containers:
            try:
                metrics = get_container_stats(container)

                event = {
                    "event_type": "container.metrics",
                    "timestamp": metrics["timestamp"],
                    "host": HOST,
                    "source": "docker",
                    "metrics": metrics,
                    "tags": {
                        "container_id": metrics["container_id"],
                        "container_name": metrics["name"]
                    }
                }

                producer.send(TOPIC, event)

            except Exception as e:
                print(f"[ERROR] {container.name}: {e}")

        producer.flush()
        time.sleep(5)


if __name__ == "__main__":
    run()
