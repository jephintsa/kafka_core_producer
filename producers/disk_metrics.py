import psutil
import time
import json
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = "192.168.2.110:9092"
TOPIC = "system.disk.metrics"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def collect_disk():
    disk = psutil.disk_usage("/")
    io = psutil.disk_io_counters()

    return {
        "event_type": "system.disk.metrics",
        "timestamp": datetime.utcnow().isoformat(),
        "host": "dell-node-a",
        "source": "psutil",
        "metrics": {
            "disk": {
                "total_gb": disk.total / (1024**3),
                "used_gb": disk.used / (1024**3),
                "free_gb": disk.free / (1024**3),
                "used_percent": disk.percent
            },
            "io": {
                "read_bytes": io.read_bytes if io else 0,
                "write_bytes": io.write_bytes if io else 0,
                "read_count": io.read_count if io else 0,
                "write_count": io.write_count if io else 0
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
            event = collect_disk()

            producer.send(TOPIC, event)
            producer.flush()

            print("sent disk:", event)

            time.sleep(5)

        except Exception as e:
            print("error:", e)
            time.sleep(5)

if __name__ == "__main__":
    run()
