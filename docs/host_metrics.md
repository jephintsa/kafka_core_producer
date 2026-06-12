# Host Metrics Schema

| Key | Type | Description |
|-----|------|-------------|
| `uptime_seconds` | float | Seconds since the host booted.
| `process_count` | int | Number of running processes.
| `cpu_cores` | int | Physical CPU core count.
| `load_avg` | array[float] | 1‑, 5‑, and 15‑minute load averages.

All values are placed under the `metrics` field in the Kafka event.