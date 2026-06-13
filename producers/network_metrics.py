import os
import socket
import time
import psutil
import logging
from typing import Dict, Any

from common.producer import (
    GracefulShutdown,
    build_event,
    build_producer,
    configure_logging,
    send_with_retry,
    shutdown_producer,
)


TOPIC = os.getenv("KAFKA_TOPIC", "network.metrics")
SAMPLE_INTERVAL = int(os.getenv("NETWORK_METRICS_INTERVAL", "2"))
HOST = os.getenv("HOSTNAME", socket.gethostname())
ENVIRONMENT = os.getenv("ENVIRONMENT", "lab")
NODE_NAME = os.getenv("NODE_NAME", HOST)

LOGGER = logging.getLogger(__name__)

last = psutil.net_io_counters()


def collect_network() -> Dict[str, Any]:
    global last
    current = psutil.net_io_counters()

    sent = current.bytes_sent - last.bytes_sent
    recv = current.bytes_recv - last.bytes_recv
    last = current

    return {
        "bytes_sent_per_sec": sent,
        "bytes_recv_per_sec": recv,
        "packets_sent": current.packets_sent,
        "packets_recv": current.packets_recv,
        "errin": current.errin,
        "errout": current.errout,
        "dropin": current.dropin,
        "dropout": current.dropout,
    }


def run() -> None:
    configure_logging()
    GracefulShutdown.install()
    producer = build_producer()

    LOGGER.info("network metrics producer started")

    while not GracefulShutdown.stopped:
        try:
            metrics = collect_network()
            event = build_event(
                event_type="network.metrics",
                source="psutil",
                metrics=metrics,
                host=HOST,
                tags={
                    "env": ENVIRONMENT,
                    "node": NODE_NAME,
                },
            )
            send_with_retry(producer, TOPIC, event)
            LOGGER.info("sent network metrics")
        except Exception as e:
            LOGGER.exception("failed sending network to network metrics: %s", str(e))
        time.sleep(SAMPLE_INTERVAL)

    shutdown_producer(producer)


if __name__ == "__main__":
    run()
