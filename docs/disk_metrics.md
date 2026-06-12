# Disk Metrics Schema

| Key | Type | Description |
|-----|------|-------------|
| `disk.total_gb` | float | Total disk space in gigabytes.
| `disk.used_gb` | float | Used disk space in gigabytes.
| `disk.free_gb` | float | Free disk space in gigabytes.
| `disk.used_percent` | float | Percentage of disk used.
| `io.read_bytes` | int | Total bytes read since boot.
| `io.write_bytes` | int | Total bytes written since boot.
| `io.read_count` | int | Number of read operations.
| `io.write_count` | int | Number of write operations.

The metrics are sent as a JSON object under the `metrics` field in the Kafka event.