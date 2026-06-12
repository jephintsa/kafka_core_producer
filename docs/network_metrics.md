# Network Metrics Schema

| Key | Type | Description |
|-----|------|-------------|
| `bytes_sent_per_sec` | int | Bytes sent per second.
| `bytes_recv_per_sec` | int | Bytes received per second.
| `packets_sent` | int | Total packets sent.
| `packets_recv` | int | Total packets received.
| `errin` | int | Input errors.
| `errout` | int | Output errors.
| `dropin` | int | Input drops.
| `dropout` | int | Output drops.

These metrics are included in the Kafka event’s `metrics` JSON object.