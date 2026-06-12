# System Metrics Schema

| Key | Type | Description |
|-----|------|-------------|
| `cpu.total_percent` | float | Overall CPU usage percentage.
| `cpu.per_core` | array[float] | Per‑core CPU percentages.
| `memory.used_percent` | float | Memory used as a percentage of total.
| `memory.available_mb` | float | Available memory in megabytes.
| `memory.total_mb` | float | Total memory in megabytes.
| `load.1m` | float | 1‑minute load average.
| `load.5m` | float | 5‑minute load average.
| `load.15m` | float | 15‑minute load average.

All fields are part of the `metrics` JSON object in the Kafka event.