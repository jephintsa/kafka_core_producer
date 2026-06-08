1. High-level project overview

This system is a real-time data engineering + observability platform built around a strict event-driven architecture:

All telemetry is collected as events
All events flow through Kafka
All processing happens in consumers (never in producers)
Storage and ML are downstream consumers
Visualization (Grafana) reads from processed storage
AI layer sits on top of processed + streaming data for explanations

Core idea:

Kafka is the single data backbone. Everything else plugs into it.

2. Two-server architecture
Node A — Dell Server (Data Source Node)

From your config and scripts, this is the sensor + telemetry generator node.

Hardware role
Older Intel i5-3450 system
8 GB RAM
Multiple HDDs
Acts as a lightweight edge/host monitoring machine
Responsibilities

This node runs producers only:

System metrics collection (CPU, disk, host stats)
Docker container monitoring (27 containers)
Network telemetry (planned / partial)
Home Assistant integration (planned / partial)
Log generation
Active producers (from your files)
1. Host metrics producer

File: host_metrics.py

Publishes to:

host.metrics

Collects:

uptime
process count
CPU cores
load average
2. Disk metrics producer

File: disk_metrics.py

Publishes to:

system.disk.metrics

Collects:

disk usage (total/used/free)
disk I/O stats (read/write bytes, ops)
3. Container metrics producer

File: container_metrics.py

Publishes to:

container.metrics

Collects (via Docker API):

CPU usage per container
memory usage + percentage
container status
network RX stats (partial)
Important rule enforced here

Node A does not:

store data
run ML
run Kafka
perform aggregation

It only:

collects → formats → sends to Kafka

3. Node B — MacBook Air Ubuntu (Core Platform Node)

From config:

Intel i5-5350U (2C/4T)
8 GB RAM
SSD-based system
More modern + stable Linux environment

This is the central brain of the system.

Responsibilities

Node B runs:

1. Kafka broker (central event hub)

This is the most important component.

All producers (Node A and future nodes) connect here:

Kafka Broker: 192.168.2.110:9092

This IP appears in all your producers:

container_metrics.py
disk_metrics.py
host_metrics.py

So Node B is the single ingestion point.

2. Consumer layer (planned / partial)

This includes:

ETL consumer → cleans + normalizes data
Analytics consumer → rolling averages, spikes
ML consumer → anomaly detection (Isolation Forest)
AI consumer → natural language interface

These consume Kafka topics like:

host.metrics
system.disk.metrics
container.metrics
3. Storage layer (planned)

Likely:

PostgreSQL → structured data, features
InfluxDB → time-series metrics

Important rule:

Only consumers write to storage, never producers.

4. AI + visualization layer
Grafana dashboards (system health, containers, anomalies)
AI assistant (LLM via Ollama)
answers questions like:
“Why is CPU high?”
“Which container is unstable?”
4. Data flow (end-to-end)
Step 1 — Data generation (Node A)

Example:

psutil → host_metrics.py
docker API → container_metrics.py
psutil → disk_metrics.py
Step 2 — Event formatting

Each script converts raw metrics into structured JSON:

Example:

{
  "event_type": "container.metrics",
  "timestamp": "...",
  "host": "dell-node-a",
  "metrics": {...}
}
Step 3 — Kafka ingestion (Node B)

All producers send to:

192.168.2.110:9092

Topics:

host.metrics
system.disk.metrics
container.metrics

Kafka acts as:

buffering + decoupling layer

Step 4 — Consumers (Node B)

Consumers independently process streams:

ETL Consumer
cleans data
normalizes schema
writes to DB
Analytics Consumer
rolling averages
spike detection
ML Consumer
Isolation Forest anomaly detection
predictions
AI Consumer
builds context
queries Kafka + DB
sends to LLM
returns explanation
Step 5 — Storage layer
Postgres / InfluxDB store processed data
raw Kafka data is not queried directly
Step 6 — Visualization + AI
Grafana reads from DB
AI layer interprets:
trends
anomalies
root causes
5. Kafka topics currently defined

From your system design + code:

Active topics
host.metrics
system.disk.metrics
container.metrics
Planned (from architecture)
system.metrics
network.metrics
home.events
logs.system
6. What is currently implemented vs planned
Implemented (working core)

✔ Kafka broker on Node B
✔ Producers on Node A
✔ Real-time container metrics
✔ Disk metrics
✔ Host metrics
✔ Event streaming pipeline (A → Kafka → B broker)

Not yet implemented

❌ Consumers (ETL / ML / AI)
❌ Storage layer (Postgres/InfluxDB)
❌ Grafana dashboards
❌ Network + Home Assistant ingestion
❌ ML anomaly detection
❌ AI assistant layer

7. Key architectural insight

This system is correctly structured as:

Event-driven pipeline
Node A (Producers)
    ↓
Kafka (Node B)
    ↓
Consumers (ETL / ML / AI)
    ↓
Storage + Grafana + AI UI
Critical design strength

You already enforce:

strict producer isolation
Kafka as single transport layer
stateless ingestion
decoupled processing

This is exactly how production observability systems (Datadog-like architecture) are built.

8. Bottlenecks / risks (based on current setup)
Single Kafka broker → potential single point of failure
Node B is both Kafka + compute → resource contention risk
HDD-based Node A may introduce occasional disk latency (if extended logging is added)
No schema registry yet → risk of inconsistent event formats
No consumer lag monitoring yet
9. Summary
Node A (Dell Server)

Role: Telemetry generator

collects system/container/disk metrics
sends JSON events to Kafka
no storage, no processing
Node B (MacBook Ubuntu)

Role: Central data platform

runs Kafka broker (192.168.2.110:9092)
receives all streams
will run consumers, ML, storage, Grafana, AI layer