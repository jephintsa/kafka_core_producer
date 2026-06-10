# Kafka‑Core Project Summary

**Kafka‑Core** is a Python‑based metrics collection framework designed to monitor container and host resources for Kafka deployments. The repository contains a modular architecture that separates concerns into distinct packages:

- **`Agg/`** – Aggregation utilities (e.g., `producer.py`) that collect and aggregate metric data.
- **`common/`** – Shared helpers, including the base `__init__.py`.
- **`producers/`** – Individual producer modules for specific metrics:
  - `container_metrics_edit.py`
  - `container_metrics.py`
  - `disk_metrics.py`
  - `host_metrics.py`
  - `network_metrics.py`
  - `process_metrics.py`
  - `system_metrics.py`
- **`common/`** – (inside producers) contains shared logic for producer modules.
- **`systemd/`** – System‑d integration helpers.

## Architecture

See the high‑level design in [ARCHITECTURE.md](ARCHITECTURE.md). Key points:
1. **Metric Producers** generate raw metrics from system or container sources.
2. **Aggregator** (`Agg/producer.py`) normalizes and aggregates these metrics into a unified format.
3. **Export Layer** (not shown in the current tree) would push aggregated data to Kafka topics.

## Usage
```bash
# Install dependencies
pip install -r requirements.txt

# Run a specific producer
python producers/container_metrics.py
```
Each producer can be executed independently for testing or integrated into a larger monitoring pipeline.

## Contributing
- Follow the style guidelines in `CONTRIBUTING.md` (if present).
- Add new metrics by creating a module under `producers/` and registering it in the aggregator.

---
For detailed documentation, refer to the individual files linked above.