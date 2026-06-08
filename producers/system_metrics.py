import psutil
import time
import json
import os
import yaml
from datetime import datetime
from kafka import KafkaProducer

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")
KAFKA_BROKER = "192.168.2.110:9092"
TOPIC = "system.metrics"

try:
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
        KAFKA_BROKER = cfg.get("kafka", {}).get("broker", KAFKA_BROKER)
        TOPIC = cfg.get("producers", {}).get("system.metrics", {}).get("topic", TOPIC)
except Exception:
    pass

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def collect_metrics():
    cpu_per_core = psutil.cpu_percent(interval=1, percpu=True)
    memory = psutil.virtual_memory()
    load = psutil.getloadavg()

    return {
        "event_type": "system.metrics",
        "timestamp": datetime.utcnow().isoformat(),
        "host": "dell-node-a",
        "source": "psutil",
        "metrics": {
            "cpu": {
                "total_percent": sum(cpu_per_core) / len(cpu_per_core),
                "per_core": cpu_per_core
            },
            "memory": {
                "used_percent": memory.percent,
                "available_mb": memory.available / (1024 * 1024),
                "total_mb": memory.total / (1024 * 1024)
            },
            "load": {
                "1m": load[0],
                "5m": load[1],
                "15m": load[2]
            }
        },
        "tags": {
            "env": "lab",
            "node": "dell-node-a"
        }
    }

def run():
    while True:
        try:
            event = collect_metrics()

            producer.send(TOPIC, event)
            producer.flush()

            print("sent:", event)

            time.sleep(2)

        except Exception as e:
            print("error:", e)
            time.sleep(2)

if __name__ == "__main__":
    run()
