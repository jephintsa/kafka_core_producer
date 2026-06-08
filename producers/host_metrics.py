import psutil
import time
import json
import os
import yaml
from datetime import datetime
from kafka import KafkaProducer

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")
KAFKA_BROKER = "192.168.2.110:9092"
TOPIC = "host.metrics"

try:
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
        KAFKA_BROKER = cfg.get("kafka", {}).get("broker", KAFKA_BROKER)
        TOPIC = cfg.get("producers", {}).get("host.metrics", {}).get("topic", TOPIC)
except Exception:
    pass

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def collect_host():
    boot_time = psutil.boot_time()
    uptime = time.time() - boot_time

    return {
        "event_type": "host.metrics",
        "timestamp": datetime.utcnow().isoformat(),
        "host": "dell-node-a",
        "source": "psutil",
        "metrics": {
            "uptime_seconds": uptime,
            "process_count": len(psutil.pids()),
            "cpu_cores": psutil.cpu_count(),
            "load_avg": psutil.getloadavg()
        },
        "tags": {
            "env": "lab",
            "node": "dell-node-a"
        }
    }

def run():
    while True:
        try:
            event = collect_host()

            producer.send(TOPIC, event)
            producer.flush()

            print("sent host metrics")

            time.sleep(10)

        except Exception as e:
            print("error:", e)
            time.sleep(10)

if __name__ == "__main__":
    run()
