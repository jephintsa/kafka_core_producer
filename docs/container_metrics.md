# Container Metrics Schema

| Key | Type | Description |
|-----|------|-------------|
| `container_id` | string | Short Docker container ID.
| `name` | string | Container name.
| `status` | string | Current status (e.g., running, exited).
| `cpu_percent` | float | CPU usage percentage.
| `memory_usage` | int | Memory used in bytes.
| `memory_limit` | int | Memory limit in bytes.
| `memory_percent` | float | Memory usage as a percentage of the limit.
| `network` | object | Network statistics per interface.
| `timestamp` | string | ISO‑8601 UTC timestamp when metrics were collected.

These fields are nested inside the `metrics` JSON object in the Kafka event.