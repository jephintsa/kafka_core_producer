import psutil
import time
import json
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = "192.168.2.110:9092"
TOPIC = "network.metrics"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

last = psutil.net_io_counters()

def collect_network():
    global last
    current = psutil.net_io_counters()

    sent = current.bytes_sent - last.bytes_sent
    recv = current.bytes_recv - last.bytes_recv

    last = current

    return {
        "event_type": "network.metrics",
        "timestamp": datetime.utcnow().isoformat(),
        "host": "dell-node-a",
        "source": "psutil",
        "metrics": {
            "bytes_sent_per_sec": sent,
            "bytes_recv_per_sec": recv,
            "packets_sent": current.packets_sent,
            "packets_recv": current.packets_recv,
            "errin": current.errin,
            "errout": current.errout,
            "dropin": current.dropin,
            "dropout": current.dropout
        },
        "tags": {
            "env": "lab",
            "node": "dell-node-a"
        }
    }

def run():
    while True:
        try:
            event = collect_network()

            producer.send(TOPIC, event)
            producer.flush()

            print("sent net:", event)

            time.sleep(2)

        except Exception as e:
            print("error:", e)
            time.sleep(2)

if __name__ == "__main__":
    run()
