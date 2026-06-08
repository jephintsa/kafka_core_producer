Producer Server (Dell Node A) - Instruction Set
=============================================

Purpose
-------

This document describes the producers running on the Dell sensor node (Node A). It is a concise reference for operators and developers: what each producer does, what topic it publishes to, run instructions, dependencies, and recommended operational improvements.

Quick facts
-----------

- **Node role**: telemetry generation only (no storage, no processing)
- **Kafka broker**: 192.168.2.110:9092 (configured in each script)
- **Common tags**: env=lab, node=dell-node-a

Producers (summary)
-------------------

- `container_metrics.py`
  - Topic: `container.metrics`
  - Cadence: collects per container then sleeps 5s between cycles
  - Source: Docker Engine API (docker SDK for Python)
  - Metrics: container id/name/status, cpu_percent, memory_usage/limit/percent, network rx, timestamp
  - Notes: uses `container.stats(stream=False)` and computes cpu% using total/system delta

- `disk_metrics.py`
  - Topic: `system.disk.metrics`
  - Cadence: 5s
  - Source: `psutil`
  - Metrics: disk total/used/free (GB), used_percent, disk io counters (read/write bytes, ops)

- `host_metrics.py`
  - Topic: `host.metrics`
  - Cadence: 10s
  - Source: `psutil`
  - Metrics: uptime_seconds, process_count, cpu_cores, load_avg

- `network_metrics.py`
  - Topic: `network.metrics`
  - Cadence: 2s
  - Source: `psutil`
  - Metrics: bytes_sent_per_sec, bytes_recv_per_sec, packets, errors, drops
  - Notes: computes delta from last `psutil.net_io_counters()` snapshot

- `process_metrics.py`
  - Topic: `process.metrics`
  - Cadence: 5s
  - Source: `psutil`
  - Metrics: top CPU consumers, top memory consumers, total process count

- `system_metrics.py`
  - Topic: `system.metrics`
  - Cadence: 2s (note: calls `psutil.cpu_percent(interval=1)` which blocks ~1s)
  - Source: `psutil`
  - Metrics: cpu total/per-core, memory used/available, load 1/5/15

How events are structured
------------------------

Each producer emits a JSON event with a structure like:

{
  "event_type": "<topic>",
  "timestamp": "<ISO UTC>",
  "host": "dell-node-a",
  "source": "psutil|docker",
  "metrics": { ... },
  "tags": { ... }
}

Dependencies
------------

- Python 3.8+ recommended
- kafka-python (`pip install kafka-python`)
- psutil (`pip install psutil`)
- docker (for container producer) (`pip install docker`)

Install example
---------------

Create a virtualenv and install deps:

```bash
python3 -m venv /opt/telemetry/venv
source /opt/telemetry/venv/bin/activate
pip install --upgrade pip
pip install kafka-python psutil docker
```

Run (manual)
------------

From the repo root on the Dell node:

```bash
cd /path/to/kafka-core
python3 producers/host_metrics.py
python3 producers/system_metrics.py
# etc. or run each producer in its own terminal/session
```

Recommended: run each producer as a managed service (systemd) or inside Docker so they auto-restart.

Systemd service example
-----------------------

Create `/etc/systemd/system/producer-<name>.service` with contents:

```ini
[Unit]
Description=Telemetry producer - host metrics
After=network.target docker.service

[Service]
User=youruser
WorkingDirectory=/path/to/kafka-core
ExecStart=/opt/telemetry/venv/bin/python3 producers/host_metrics.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Operational checklist
---------------------

- Verify Kafka connectivity: broker reachable on `192.168.2.110:9092` and topics exist when consumers start
- Ensure Docker daemon is running for `container_metrics.py`
- Ensure Python virtualenv and packages present for all producers
- Ensure node hostname/tagging matches expectations (`dell-node-a` used in scripts)

Troubleshooting
---------------

- If `producer.send` errors or blocks: check network connectivity, broker address, and broker logs.
- If Docker-related errors: verify Docker socket permissions and that Docker CLI works for the service user.
- If `psutil` fails to collect metrics: ensure correct permissions (some metrics require root) or run with sudo/system service.
- High CPU from producers: reduce cadences (increase sleep), avoid calling blocking `psutil.cpu_percent(interval=1)` in tight loops.

Improvements and recommendations
--------------------------------

- Centralize configuration: move `KAFKA_BROKER`, `HOST`, common tags and cadences into a single `config.yaml` consumed by all producers.
- Add retry/backoff and error metrics (emit local health events on a health topic).
- Use batching or compression for higher throughput and lower network overhead.
- Add TLS/SASL for Kafka authentication and encryption in production.
- Add a lightweight schema registry or implicit schema version field to event payloads to ensure consumers can evolve safely.
- Containerize producers for easier deployment and dependency control.
- Add unit tests for collectors (mock psutil/docker) and CI checks.

Notes about progression
----------------------

This repository contains working producers for host, disk, container, network, process, and system metrics. Consumers, storage, and ML layers are not yet implemented — the producers are currently configured to publish directly to the broker at 192.168.2.110:9092.

File location
-------------

This instruction set is saved here: producers/INSTRUCTIONS.md

End
