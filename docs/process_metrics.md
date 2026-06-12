# Process Metrics Schema

| Key | Type | Description |
|-----|------|-------------|
| `top_cpu_processes` | array[object] | Top 10 processes by CPU usage. Each object contains `pid`, `name`, `cpu_percent`, and `memory_percent`.
| `top_memory_processes` | array[object] | Top 10 processes by memory usage. Same fields as above.
| `process_count` | int | Total number of processes on the host.

The arrays are nested inside the `metrics` JSON object in the Kafka event.