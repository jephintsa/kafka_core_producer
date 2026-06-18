# Metrics Overview

This document describes the Kafka events produced by the **kafka‑core** metric collectors and explains each field that appears in the payload.  All events share a common envelope:

| Field | Type | Description |
|-------|------|-------------|
| `event_type` | string | Logical name of the event (e.g., `container.metrics`). |
| `source` | string | The library or tool that collected the data (`docker`, `psutil`). |
| `metrics` | object | Dictionary containing metric key/value pairs specific to the event type. |
| `host` | string | Hostname of the machine that produced the metrics. |
| `tags` | object | Key/value tags for filtering (e.g., `env`, `node`). |

Below is a table for each event type and its metric keys.

## Container Metrics (`container.metrics`)

| Metric key | Type | Description |
|------------|------|-------------|
| `container_id` | string | Short 12‑character Docker container ID. |
| `name` | string | Container name. |
| `status` | string | Current status (e.g., `running`). |
| `cpu_percent` | float | CPU usage percentage of the container, rounded to two decimals. |
| `memory_usage` | int | Memory used in bytes. |
| `memory_limit` | int | Memory limit set for the container in bytes. |
| `memory_percent` | float | Percentage of memory used relative to the limit. |
| `network` | object | Raw network stats from Docker (`rx_bytes`, `tx_bytes`, etc.). |
| `timestamp` | string (ISO‑8601 UTC) | Time when metrics were collected. |

## System Disk Metrics (`system.disk.metrics`)

| Metric key | Type | Description |
|------------|------|-------------|
| `disk_total_gb` | float | Total disk capacity in gigabytes. |
| `disk_used_gb` | float | Used space in gigabytes. |
| `disk_free_gb` | float | Free space in gigabytes. |
| `disk_used_percent` | float | Percentage of disk used. |
| `io_read_bytes` | int | Bytes read since boot. |
| `io_write_bytes` | int | Bytes written since boot. |
| `io_read_count` | int | Number of read operations since boot. |
| `io_write_count` | int | Number of write operations since boot. |

## Host Metrics (`host.metrics`)

| Metric key | Type | Description |
|------------|------|-------------|
| `uptime_seconds` | float | Seconds since the host last booted. |
| `process_count` | int | Total number of processes currently running. |
| `cpu_cores` | int | Number of logical CPU cores. |
| `load_1m`, `load_5m`, `load_15m` | float | 1‑, 5‑, and 15‑minute load averages. |

## Network Metrics (`network.metrics`)

| Metric key | Type | Description |
|------------|------|-------------|
| `bytes_sent_per_sec` | int | Bytes sent in the last sample interval. |
| `bytes_recv_per_sec` | int | Bytes received in the last sample interval. |
| `packets_sent`, `packets_recv` | int | Total packets sent/received since boot. |
| `errin`, `errout` | int | Input/output errors. |
| `dropin`, `dropout` | int | Packets dropped on input/output. |

## Process Metrics (`process.metrics`)

| Metric key | Type | Description |
|------------|------|-------------|
| `top_cpu_processes` | array of objects | Top 10 processes by CPU usage; each object contains `pid`, `name`, `cpu_percent`, `memory_percent`. |
| `top_memory_processes` | array of objects | Top 10 processes by memory usage. |
| `process_count` | int | Total number of processes enumerated. |

## System Metrics (`system.metrics`)

| Metric key | Type | Description |
|------------|------|-------------|
| `cpu_total_percent` | float | Average CPU utilization across all cores. |
| `cpu_per_core` | array of floats | Individual core percentages. |
| `memory_used_percent` | float | Percentage of RAM used. |
| `memory_available_mb`, `memory_total_mb` | int | Available and total memory in megabytes. |
| `load_1m`, `load_5m`, `load_15m` | float | Load averages. |

---

### Tags
All events include the following tags:

- **env** – Deployment environment (e.g., `lab`).
- **node** – Node name, usually the hostname.
- **container_id**, **container_name** – For container metrics only.

These tags enable filtering and aggregation in downstream consumers such as Grafana or custom dashboards.

---

For more detailed information on how each producer collects data, see the source code under `producers/`.