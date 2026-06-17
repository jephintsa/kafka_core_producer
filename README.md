# Kafka Core

Kafka Core is a small Python metrics pipeline that collects host, disk, container, process, network, and system metrics on Node A and publishes them to Kafka for downstream processing on Node B.

## Documentation
- [Architecture and plan](ARCHITECTURE.md)

## Quick Start
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

export KAFKA_BROKER="192.168.2.110:9092"
python producers/host_metrics.py
```

## Main Pieces
- `common/producer.py` handles event creation, retries, logging, and shutdown.
- `producers/` contains the metric collectors.
- `tests/` covers the shared event builder.
