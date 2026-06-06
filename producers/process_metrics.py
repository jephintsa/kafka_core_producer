import psutil
import time
import json
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = "192.168.2.110:9092"
TOPIC = "process.metrics"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def collect_processes():
    processes = []

    for p in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
        try:
            info = p.info
            processes.append(info)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    # sort for observability (top consumers)
    top_cpu = sorted(processes, key=lambda x: x['cpu_percent'], reverse=True)[:10]
    top_mem = sorted(processes, key=lambda x: x['memory_percent'], reverse=True)[:10]

    return {
        "event_type": "process.metrics",
        "timestamp": datetime.utcnow().isoformat(),
        "host": "dell-node-a",
        "source": "psutil",
        "metrics": {
            "top_cpu_processes": top_cpu,
            "top_memory_processes": top_mem,
            "process_count": len(processes)
        },
        "tags": {
            "env": "lab",
            "node": "dell-node-a"
        }
    }

def run():
    while True:
        try:
            event = collect_processes()

            producer.send(TOPIC, event)
            producer.flush()

            print("sent process metrics")

            time.sleep(5)

        except Exception as e:
            print("error:", e)
            time.sleep(5)

if __name__ == "__main__":
    run()
