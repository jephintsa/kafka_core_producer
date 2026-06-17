# Node A Detailed Documentation

This document explains Node A in full detail: what it is, why it exists, how each producer works, how the shared event pipeline is assembled, how the containerized deployment behaves, and how the whole system can be recreated from scratch if the code were lost.

The goal is not only to describe the current implementation, but to preserve the intent behind it so that a new person with no prior knowledge of Kafka, Python, psutil, Docker, or the repository could rebuild the same system with confidence.

## 1. What Node A Is

Node A is the data-collection side of the system. It runs on the Dell server and gathers machine and workload metrics locally from the operating system and from Docker. It then sends those metrics to Kafka so that Node B can consume, store, analyze, visualize, and eventually enrich them.

In practical terms, Node A is the source of truth for live telemetry. It does not store the data long-term and it does not perform downstream analytics. Its job is to observe the local machine, package the observations into a stable event format, and publish them reliably.

### What Node A collects

Node A currently produces six streams of metrics:

1. Host metrics
2. Disk metrics
3. Container metrics
4. Process metrics
5. Network metrics
6. System metrics

Each stream is handled by its own producer module under the [producers](../producers) directory.

### Why Node A is separate from Node B

The separation matters because the machine collecting metrics is not the same machine that processes them. This reduces coupling and makes the system more realistic and scalable:

- Node A focuses on observation and publishing.
- Node B focuses on ingestion, storage, dashboards, and future AI/analytics work.

That separation also makes it easier to reason about failures. If Node B goes down, Node A can continue collecting and retrying. If Node A goes down, Node B simply stops receiving new events.

## 2. Repository Map for Node A

The most important files for Node A are listed below.

| File | Purpose | Why it exists |
|---|---|---|
| [common/producer.py](../common/producer.py) | Shared event builder, Kafka producer factory, retry logic, logging setup, and shutdown handling | Keeps all producers consistent and avoids duplicate code |
| [producers/host_metrics.py](../producers/host_metrics.py) | Collects host-level system data such as uptime and load | Reports the general health of the machine |
| [producers/disk_metrics.py](../producers/disk_metrics.py) | Collects disk usage and disk I/O counters | Tracks storage pressure and disk activity |
| [producers/container_metrics.py](../producers/container_metrics.py) | Collects Docker container stats and exposes `/health` | Observes container workloads and provides health visibility |
| [producers/process_metrics.py](../producers/process_metrics.py) | Collects the top CPU and memory processes | Identifies the noisiest processes on the machine |
| [producers/network_metrics.py](../producers/network_metrics.py) | Collects network traffic and error counters | Captures interface activity and network quality signals |
| [producers/system_metrics.py](../producers/system_metrics.py) | Collects CPU, memory, and load data | Provides a higher-level machine overview |
| [docker/Dockerfile](../docker/Dockerfile) | Builds the runtime image used for producers | Makes the system portable and reproducible |
| [docker/docker-compose.yml](../docker/docker-compose.yml) | Runs Kafka, Zookeeper, and all six producers | Defines the complete local or lab deployment |
| [run_producer.sh](../run_producer.sh) | Starts one producer module inside the container | Gives each container a simple and consistent entrypoint |
| [requirements.txt](../requirements.txt) | Python package dependencies | Records the exact libraries needed to run Node A |
| [tests/test_producer.py](../tests/test_producer.py) | Tests the shared event builder | Verifies that the event envelope is built correctly |
| [conftest.py](../conftest.py) | Adds the repo root to Python import path during tests | Lets pytest import `common` reliably from the project root |

## 3. The End-to-End Flow

At a high level, the flow is:

1. A producer wakes up on a fixed interval.
2. It gathers data from the machine or Docker.
3. It places the data into a standard event envelope.
4. It sends the event to Kafka.
5. If sending fails, it retries with exponential backoff.
6. When the process is stopped, it flushes and closes the Kafka connection cleanly.

The key design principle is that every producer follows the same lifecycle even though each one collects different data.

## 4. The Shared Event Model

All Node A producers use the common event builder in [common/producer.py](../common/producer.py). That builder creates one standard JSON document shape for every metric event.

### Event envelope

Each event contains:

- `version`
- `event_type`
- `timestamp`
- `source`
- `metrics`
- `tags`
- `host` when available

### What each field means

#### `version`

This is the contract version. In the current implementation it is always `1`.

Why it exists:

- It allows future changes without breaking old consumers.
- If the format changes in a way that is not backward compatible, a new version can be introduced.

#### `event_type`

This is the event name, aligned to the Kafka topic or producer purpose. Examples include `host.metrics` and `system.metrics`.

Why it exists:

- It tells downstream consumers what kind of event they are reading.
- It keeps routing deterministic.
- It makes the topic name and event name easy to match.

#### `timestamp`

This is the moment the event was created, in UTC, formatted as ISO-8601 and ending in `Z`.

Why it exists:

- It allows consumers to order events consistently.
- It avoids confusion about time zones.
- It is safe for time-series storage and analysis.

#### `source`

This identifies the data source library or subsystem, such as `psutil` or `docker`.

Why it exists:

- It shows where the metrics came from.
- It helps debugging and provenance tracking.

#### `metrics`

This is the payload that holds the actual measured values.

Why it exists:

- It keeps the collected data separate from the common envelope.
- It allows each producer to store its own specialized measurements.

#### `tags`

These are metadata labels such as environment and node name.

Why it exists:

- It makes the events easier to filter, group, and partition.
- It allows consumers to identify where the data came from.

#### `host`

This is an optional host identifier. It is included when known.

Why it exists:

- It gives downstream systems a stable machine identifier.
- It is useful when the node hostname differs from other node naming conventions.

### Versioning rules

The current documentation and code imply the following rules:

- Adding a new optional field is allowed if it does not break existing consumers.
- Removing or renaming an existing top-level field should require a new contract version.
- The event type should remain aligned with the topic name so routing stays predictable.
- The shared builder is the canonical source for the event shape.

### Why the envelope is stable

The same envelope format is used for all producers because downstream systems should not need special handling for each metric source. A stable envelope means consumers can normalize, store, and route events with the same logic regardless of whether the payload describes disk, process, network, or container activity.

## 5. Shared Producer Helper in Detail

The file [common/producer.py](../common/producer.py) is the center of the Node A runtime model. It defines the reusable behavior that every producer depends on.

### 5.1 `configure_logging()`

This function sets up Python logging.

What it does:

- Reads `LOG_LEVEL` from the environment, or falls back to `INFO`.
- Configures the root logger.
- Uses a timestamped log format.

Why it exists:

- It makes logs readable and consistent across all producer processes.
- It allows the verbosity to be changed without code edits.

### 5.2 `get_env()`

This helper reads a required environment variable and raises an error if it is missing.

Why it exists:

- It provides a safe way to enforce required configuration.
- It makes missing environment values fail early and clearly.

### 5.3 `build_producer()`

This function constructs the Kafka producer client.

Behavior:

- Reads `KAFKA_BROKER` from the environment.
- Falls back to `192.168.2.110:9092` if no broker is provided.
- Allows multiple brokers through comma-separated values.
- Serializes payloads as JSON encoded in UTF-8.
- Uses environment-driven retry and acknowledgment settings.

Relevant settings:

- `KAFKA_PRODUCER_RETRIES` defaults to `5`
- `KAFKA_PRODUCER_LINGER_MS` defaults to `0`
- `KAFKA_PRODUCER_ACKS` defaults to `all`

Why it exists:

- It standardizes the Kafka client across all producers.
- It ensures all events are serialized the same way.
- It keeps broker setup and messaging behavior in one place.

### 5.4 `build_event()`

This function creates the common event dictionary.

What it does:

- Adds the contract version.
- Adds the event type.
- Creates an automatic UTC timestamp if none is provided.
- Inserts the source, metrics, and tags.
- Adds the host if supplied.

Why it exists:

- It prevents every producer from re-implementing the same event formatting logic.
- It ensures the output shape stays consistent.

Timestamp behavior:

- The timestamp is generated with UTC time.
- Microseconds are removed.
- The timestamp is normalized to a `Z` suffix.

This matters because it gives downstream systems a clean and predictable time representation.

### 5.5 `send_with_retry()`

This function sends one event to Kafka and retries if Kafka returns an error.

How it works:

1. It tries to send the event.
2. It flushes the producer immediately after sending.
3. If Kafka raises a `KafkaError`, it waits and retries.
4. The wait time grows exponentially with each failure.
5. If the maximum retry count is exceeded, it raises the error.

Configuration:

- `KAFKA_SEND_MAX_RETRIES` defaults to `5`
- `KAFKA_SEND_BACKOFF_SECONDS` defaults to `1`

Why it exists:

- Kafka may be temporarily unavailable.
- Short interruptions should not cause permanent data loss.
- Exponential backoff avoids hammering the broker when it is unhealthy.

### 5.6 `shutdown_producer()`

This function flushes any remaining buffered records and then closes the Kafka producer.

Why it exists:

- It reduces the chance of losing the last events during shutdown.
- It allows the process to exit cleanly.

### 5.7 `GracefulShutdown`

This class installs signal handlers for `SIGINT` and `SIGTERM`.

Behavior:

- When a termination signal arrives, the class sets a shared `stopped` flag to `True`.
- The producer loops check that flag on each iteration.

Why it exists:

- It gives the process a controlled way to stop.
- It prevents abrupt termination from leaving the Kafka client open.

### 5.8 Direct execution of `common/producer.py`

The module can be run directly. In that case it only configures logging, installs signal handlers, logs that the module loaded, and exits.

That behavior is a lightweight sanity check and not a real service entrypoint.

## 6. Producer-by-Producer Documentation

Each producer has the same broad structure:

1. Read configuration from environment variables.
2. Configure logging and graceful shutdown.
3. Create a Kafka producer.
4. Loop until stopped.
5. Collect metrics.
6. Build an event.
7. Send the event with retry handling.
8. Sleep for the configured interval.
9. Shutdown cleanly when stopped.

The differences are in the data source and the exact metrics collected.

### 6.1 Host Metrics Producer

File: [producers/host_metrics.py](../producers/host_metrics.py)

#### Purpose

The host metrics producer gives a broad picture of the machine itself. It answers questions like:

- How long has the machine been running?
- How many processes exist on the machine?
- How many CPU cores are available?
- What is the current system load?

#### Data source

It uses `psutil`, a Python library that exposes operating-system statistics.

#### Collected metrics

The function `collect_host()` returns:

- `uptime_seconds`
- `process_count`
- `cpu_cores`
- `load_1m`
- `load_5m`
- `load_15m`

#### How each field is computed

- Uptime is calculated by subtracting the system boot time from the current wall-clock time.
- Process count is computed from the number of PIDs known to the OS.
- CPU cores come from `psutil.cpu_count()`.
- Load averages are taken from the system load average over 1, 5, and 15 minutes.

#### Event settings

- Default topic: `host.metrics`
- Event type: `host.metrics`
- Source: `psutil`
- Default interval: 10 seconds

#### Tags and identity

The producer adds:

- `env` from `ENVIRONMENT`, default `lab`
- `node` from `NODE_NAME`, default hostname
- `host` from `HOSTNAME` or the machine hostname

#### Why it exists

This producer is the broad health summary for the server. It provides context for all the more specific collectors because a disk or process metric is easier to interpret when you know the machine’s general state.

### 6.2 Disk Metrics Producer

File: [producers/disk_metrics.py](../producers/disk_metrics.py)

#### Purpose

The disk metrics producer tracks storage usage and disk activity.

#### Data source

It uses `psutil` to inspect the filesystem and disk I/O counters.

#### Collected metrics

The function `collect_disk()` returns:

- `disk_total_gb`
- `disk_used_gb`
- `disk_free_gb`
- `disk_used_percent`
- `io_read_bytes`
- `io_write_bytes`
- `io_read_count`
- `io_write_count`

#### How each field is computed

- Storage size values are read from the root filesystem `/` and converted from bytes to gigabytes.
- Used percentage comes from the filesystem usage summary.
- I/O counters come from the OS disk I/O statistics.
- If disk I/O counters are unavailable, the code safely uses `0`.

#### Event settings

- Default topic: `system.disk.metrics`
- Event type: `system.disk.metrics`
- Source: `psutil`
- Default interval: 5 seconds

#### Why it exists

Disk pressure is often one of the first indicators of a machine becoming unhealthy. A nearly full disk or rapidly increasing I/O can explain slow performance or service interruptions.

### 6.3 Container Metrics Producer

File: [producers/container_metrics.py](../producers/container_metrics.py)

#### Purpose

The container metrics producer observes Docker containers running on the Node A machine. This is important when the server hosts multiple containers and you want to understand their resource usage and runtime status.

#### Data source

It uses the Docker Python SDK and connects to the local Docker daemon via the environment created by `docker.from_env()`.

#### Docker access

The container producer depends on the Docker socket being available. In the containerized deployment, the host Docker socket is mounted into the producer container so the producer can query the local Docker engine.

#### Collected metrics

The function `get_container_stats()` returns:

- `container_id`
- `name`
- `status`
- `cpu_percent`
- `memory_usage`
- `memory_limit`
- `memory_percent`
- `network`
- `timestamp`

#### How the metrics are calculated

##### CPU percentage

The producer reads current and previous container CPU stats and calculates CPU usage from the delta between them.

This is why it uses both `cpu_stats` and `precpu_stats`.

##### Memory percentage

It divides current memory usage by the memory limit and converts the result into a percent.

##### Network data

The raw network statistics are passed through from Docker as a nested object.

##### Timestamp

The container-specific stats include their own UTC timestamp in addition to the standard event timestamp added by the shared event builder.

#### Event settings

- Default topic: `container.metrics`
- Event type: `container.metrics`
- Source: `docker`
- Default interval: 5 seconds

#### Health endpoint

This producer is the only Node A producer that exposes an HTTP health endpoint in the current code.

Behavior:

- It starts an HTTP server on `0.0.0.0`.
- The port comes from `HEALTH_PORT`, defaulting to `8000`.
- A GET request to `/health` returns JSON.
- The response includes:
	- `status`: `ok` or `stopping`
	- `last_sent`: timestamp of the last successful metric send or `unknown`

Why it exists:

- It gives orchestration systems a simple way to verify that the producer is alive and recently able to send data.
- It reflects both process liveness and metric publication success.

#### Why it exists

Container metrics make it possible to see how individual workloads behave on the server without needing to log into each workload separately.

### 6.4 Process Metrics Producer

File: [producers/process_metrics.py](../producers/process_metrics.py)

#### Purpose

The process metrics producer shows which processes are consuming the most CPU and memory.

#### Data source

It uses `psutil.process_iter()` to inspect every visible process.

#### Collected metrics

The function `collect_processes()` returns:

- `top_cpu_processes`
- `top_memory_processes`
- `process_count`

#### How the metrics are calculated

- The code iterates over processes and keeps a small information dictionary for each one.
- It sorts the processes by CPU percent and keeps the top 10.
- It sorts the processes by memory percent and keeps the top 10.
- It counts the total number of successfully collected processes.

#### Error handling

Some processes can disappear between discovery and inspection, or can deny access. The code explicitly ignores `NoSuchProcess` and `AccessDenied` so one bad process does not break the whole sample.

#### Event settings

- Default topic: `process.metrics`
- Event type: `process.metrics`
- Source: `psutil`
- Default interval: 5 seconds

#### Why it exists

This producer helps identify the processes that matter most when the machine is under pressure. It is especially useful for spotting runaway applications or unexpected CPU and memory use.

### 6.5 Network Metrics Producer

File: [producers/network_metrics.py](../producers/network_metrics.py)

#### Purpose

The network metrics producer tracks network activity on the machine.

#### Data source

It uses `psutil.net_io_counters()`.

#### Collected metrics

The function `collect_network()` returns:

- `bytes_sent_per_sec`
- `bytes_recv_per_sec`
- `packets_sent`
- `packets_recv`
- `errin`
- `errout`
- `dropin`
- `dropout`

#### Important implementation detail

The code stores a previous counter snapshot in a module-level variable called `last` and subtracts the previous bytes sent and received from the current values.

That means the first two byte fields are actually the difference since the last sample interval, not a true normalized per-second rate.

The field names say `per_sec`, but the code does not divide by the elapsed interval. This is an important detail to preserve in the documentation because it reflects the current implementation exactly.

#### Event settings

- Default topic: `network.metrics`
- Event type: `network.metrics`
- Source: `psutil`
- Default interval: 2 seconds

#### Why it exists

Network traffic and network errors can reveal system behavior that is not obvious from CPU or memory alone. A machine may look healthy while suffering packet loss, drops, or unusual traffic spikes.

### 6.6 System Metrics Producer

File: [producers/system_metrics.py](../producers/system_metrics.py)

#### Purpose

The system metrics producer gives a compact overview of the machine’s core resource state: CPU, memory, and load.

#### Data source

It uses `psutil`.

#### Collected metrics

The function `collect_metrics()` returns:

- `cpu_total_percent`
- `cpu_per_core`
- `memory_used_percent`
- `memory_available_mb`
- `memory_total_mb`
- `load_1m`
- `load_5m`
- `load_15m`

#### How the metrics are calculated

- CPU usage is sampled per core.
- The total CPU percentage is computed as the average of the per-core percentages.
- Memory values are read from `virtual_memory()` and converted from bytes to megabytes where needed.
- Load averages are taken from the system load average values.

#### Important implementation detail

The CPU percentage call uses `interval=1`, which means the function waits about one second while measuring CPU usage.

That means this collector is intentionally slower than a pure instantaneous read. The one-second sampling window improves the reliability of CPU measurements but makes the collection step blocking.

#### Event settings

- Default topic: `system.metrics`
- Event type: `system.metrics`
- Source: `psutil`
- Default interval: 2 seconds

#### Why it exists

This producer acts as the machine summary event stream. It is useful when a consumer wants a single broad signal without inspecting the more specialized collectors.

## 7. Runtime Configuration and Environment Variables

The producers are intentionally configured through environment variables instead of hard-coded values. That makes them easier to run in Docker, on a physical machine, or in future orchestration systems.

### Common environment variables

| Variable | Default | Used by | Meaning |
|---|---|---|---|
| `KAFKA_BROKER` | `192.168.2.110:9092` | all producers | Kafka broker address or comma-separated broker list |
| `KAFKA_TOPIC` | producer-specific in code, overridden in compose | all producers | Destination Kafka topic name |
| `LOG_LEVEL` | `INFO` | all producers | Logging verbosity |
| `ENVIRONMENT` | `lab` | all producers | Logical environment label |
| `NODE_NAME` | hostname | all producers | Human-readable node label |
| `HOSTNAME` | system hostname | all producers | Host identifier used in the event |
| `KAFKA_SEND_MAX_RETRIES` | `5` | shared helper | Retry limit for failed sends |
| `KAFKA_SEND_BACKOFF_SECONDS` | `1` | shared helper | Starting delay for retry backoff |
| `KAFKA_PRODUCER_RETRIES` | `5` | shared helper | Kafka client retry setting |
| `KAFKA_PRODUCER_LINGER_MS` | `0` | shared helper | Kafka client batching delay |
| `KAFKA_PRODUCER_ACKS` | `all` | shared helper | Kafka acknowledgment policy |

### Producer-specific interval variables

| Variable | Default | Producer |
|---|---|---|
| `HOST_METRICS_INTERVAL` | `10` | Host metrics |
| `DISK_METRICS_INTERVAL` | `5` | Disk metrics |
| `CONTAINER_METRICS_INTERVAL` | `5` | Container metrics |
| `PROCESS_METRICS_INTERVAL` | `5` | Process metrics |
| `NETWORK_METRICS_INTERVAL` | `2` | Network metrics |
| `SYSTEM_METRICS_INTERVAL` | `2` | System metrics |

### Why environment variables matter

Environment variables allow the same codebase to run in multiple contexts without editing source code:

- local development
- container runtime
- docker compose deployment
- future production orchestration

## 8. Containerization

Node A is designed to run cleanly inside Docker containers.

### 8.1 Dockerfile

File: [docker/Dockerfile](../docker/Dockerfile)

#### What it does

- Starts from `python:3.10-slim`.
- Sets `/app` as the working directory.
- Adds `/app` to `PYTHONPATH`.
- Installs Python dependencies from `requirements.txt`.
- Copies `common`, `producers`, and `run_producer.sh` into the image.
- Marks the startup script as executable.
- Uses the script as the container entrypoint.

#### Why it exists

This file defines a repeatable runtime environment. Anyone with Docker can build the same image and run the same producers without manually installing Python packages on the host.

#### Why `ENTRYPOINT` is used

The entrypoint makes it possible to pass the producer module name as a command argument through docker compose. That means one image can run six different producer processes depending on the command supplied.

### 8.2 docker-compose.yml

File: [docker/docker-compose.yml](../docker/docker-compose.yml)

#### Services defined

The compose file runs:

- `zookeeper`
- `kafka`
- `container_metrics`
- `disk_metrics`
- `host_metrics`
- `network_metrics`
- `process_metrics`
- `system_metrics`

#### Kafka and Zookeeper

The Kafka stack is based on Confluent images:

- `confluentinc/cp-zookeeper:7.2.1`
- `confluentinc/cp-kafka:7.2.1`

Why they exist:

- Zookeeper coordinates Kafka in this setup.
- Kafka is the event transport layer that receives all Node A metrics.

#### Kafka networking

Kafka listens on port `9092` and advertises itself as `kafka:9092` inside the compose network.

Why it exists:

- The producers can reach Kafka by service name instead of hard-coded host IPs.
- This keeps the containerized setup self-contained.

#### Producer services

Each producer service uses the same build context and the same Dockerfile, but passes a different command:

- `container_metrics`
- `disk_metrics`
- `host_metrics`
- `network_metrics`
- `process_metrics`
- `system_metrics`

#### Important deployment detail for the container producer

The container producer mounts `/var/run/docker.sock` into the container.

Why this matters:

- The Docker SDK needs access to the Docker daemon.
- Without the socket mount, the container producer cannot inspect local containers.

#### Topic names in compose

The compose file sets topic names like `container_metrics_topic` and `disk_metrics_topic`.

That is different from the default topic names hard-coded in the producer modules, which use dotted names such as `container.metrics` and `system.metrics`.

This means compose overrides the defaults through environment variables.

That distinction is important because it shows that the code is environment-driven and can target different topic naming schemes depending on deployment.

### 8.3 run_producer.sh

File: [run_producer.sh](../run_producer.sh)

#### What it does

The script expects a module name, for example `container_metrics`.

It then:

1. Validates that an argument was provided.
2. Adds `.py` if the name does not already include it.
3. Verifies that the producer module exists.
4. Sets default Kafka topic and interval values if they are not already defined.
5. Prints the module name and topic.
6. Executes Python with unbuffered output.

#### Why it exists

The script is a simple, reusable launcher. It lets one Docker image run multiple producer modules by switching the command argument.

#### Signal handling note

The script uses `exec` so the Python process replaces the shell process.

Why that matters:

- Signals such as SIGTERM reach the Python application directly.
- This makes graceful shutdown behavior work correctly in containers.

## 9. Testing

The current test file is [tests/test_producer.py](../tests/test_producer.py).

### What is tested

The tests focus on `build_event()` from [common/producer.py](../common/producer.py).

They verify that:

- the contract version is present and equals `1`
- the event type is stored correctly
- the source is stored correctly
- the metrics payload is embedded correctly
- tags are preserved
- the host field is included when provided
- a timestamp is generated automatically
- the timestamp ends in `Z`
- optional fields can be omitted
- the tags field still exists when no tags are provided

### Why the tests are small

These tests protect the shared envelope because every producer depends on it. If the event builder changes unexpectedly, every downstream stream could be affected.

### Test import setup

The file [conftest.py](../conftest.py) adds the repository root to `sys.path` so pytest can import `common.producer` from the project root.

This is necessary because Python import resolution does not automatically treat the repository root as an importable package directory during test execution.

### Known local test requirement

Repository memory notes indicate that local tests need `PYTHONPATH=.` or equivalent path setup so imports like `common.producer` resolve correctly.

That detail is important if the repository is recreated later.

## 10. Python Dependencies

File: [requirements.txt](../requirements.txt)

### Runtime libraries

- `psutil` provides host, disk, process, network, and memory statistics.
- `docker` provides access to the local Docker daemon and container stats.
- `requests` and `requests-unixsocket` support HTTP-related interactions and Docker socket-related use cases.
- `urllib3<2.0` pins a compatible version range for the HTTP stack.
- `kafka-python` provides Kafka producer support.

### Test libraries

- `pytest`
- `pytest-asyncio`

### Why dependency pinning matters

Exact versions reduce the chance of behavior changes across environments. This is especially important for systems like this one that depend on Kafka client semantics, Docker API access, and psutil metric calculations.

## 11. File-by-File Behavioral Summary

This section is a compact reference for reconstruction.

### [common/producer.py](../common/producer.py)

- Defines `CONTRACT_VERSION = 1`.
- Creates Kafka producers.
- Builds the event envelope.
- Retries Kafka sends.
- Handles logging and shutdown.
- Installs signal handling for clean exits.

### [producers/host_metrics.py](../producers/host_metrics.py)

- Collects uptime, process count, CPU cores, and load averages.
- Sends `host.metrics` events.
- Runs every 10 seconds by default.

### [producers/disk_metrics.py](../producers/disk_metrics.py)

- Collects filesystem usage and disk I/O counters.
- Sends `system.disk.metrics` events.
- Runs every 5 seconds by default.

### [producers/container_metrics.py](../producers/container_metrics.py)

- Collects Docker container CPU, memory, network, and status data.
- Sends `container.metrics` events.
- Runs every 5 seconds by default.
- Exposes `/health` on port 8000 by default.

### [producers/process_metrics.py](../producers/process_metrics.py)

- Collects top CPU and memory processes.
- Sends `process.metrics` events.
- Runs every 5 seconds by default.

### [producers/network_metrics.py](../producers/network_metrics.py)

- Collects interface byte deltas, packet counts, and error/drop counters.
- Sends `network.metrics` events.
- Runs every 2 seconds by default.

### [producers/system_metrics.py](../producers/system_metrics.py)

- Collects CPU, memory, and load averages.
- Sends `system.metrics` events.
- Runs every 2 seconds by default.

### [docker/Dockerfile](../docker/Dockerfile)

- Builds the standard runtime image.
- Installs dependencies.
- Copies code.
- Uses the launcher script as entrypoint.

### [docker/docker-compose.yml](../docker/docker-compose.yml)

- Defines the Kafka stack.
- Defines one service per producer.
- Supplies broker addresses, topic names, and intervals.

### [run_producer.sh](../run_producer.sh)

- Launches a selected producer module.
- Ensures the module exists.
- Sets default environment variables.
- Uses `exec` so signals are handled correctly.

### [tests/test_producer.py](../tests/test_producer.py)

- Verifies the event builder.
- Confirms the default envelope shape.

## 12. Operational Behavior and Important Implementation Notes

These are the details that matter when someone tries to understand or recreate the system exactly.

### 12.1 Node A is collection-only

Node A does not store data permanently and does not perform consumer-side processing. It only observes and publishes.

### 12.2 Each producer owns one topic and one interval

Each module is intentionally narrow in scope. This makes failures easier to isolate and keeps each metric stream understandable.

### 12.3 Logging is standardized

All producers use the same basic logging format and log start, send, retry, and shutdown-related messages.

### 12.4 Shutdown is cooperative

The process exits by noticing a signal-set flag rather than by being killed abruptly.

### 12.5 Retry behavior is conservative

The retry policy is designed to preserve continuity during temporary Kafka outages without retrying forever.

### 12.6 Some field names reflect implementation choices

The network producer’s `bytes_sent_per_sec` and `bytes_recv_per_sec` values are deltas between samples, not true per-second normalized rates. The code should be documented exactly as it behaves.

### 12.7 Some collectors are slower by design

The system metrics producer waits one second while sampling CPU usage. That makes its collection loop slower than the others.

### 12.8 The container producer has extra responsibilities

Unlike the other producers, the container producer both collects data and exposes HTTP health status.

## 13. How to Recreate the Entire Node A System

If everything were lost, the following steps would recreate the current design.

### Step 1: Create the Python project structure

Recreate:

- `common/`
- `producers/`
- `tests/`
- `docker/`
- `docs/`

### Step 2: Create the shared helper

Implement `common/producer.py` with:

- logging setup
- Kafka producer creation
- event envelope creation
- retry logic
- shutdown handling
- signal handling

### Step 3: Create the six producer modules

Implement each module with the same lifecycle:

- load env vars
- configure logging
- install shutdown handler
- build Kafka producer
- loop until stopped
- collect metrics
- build event
- send with retry
- sleep
- shutdown

### Step 4: Use the exact data sources

- `psutil` for host, disk, process, network, and system metrics
- Docker SDK for container metrics

### Step 5: Preserve the event envelope

Every message should include:

- version
- event_type
- timestamp
- source
- metrics
- tags
- host when available

### Step 6: Preserve the container health endpoint

The container producer should run a small HTTP server exposing `/health` and returning JSON with status and last successful send time.

### Step 7: Recreate the container build

Build from `python:3.10-slim`, copy the code, install requirements, and use the launcher script as the entrypoint.

### Step 8: Recreate the compose deployment

Define Kafka, Zookeeper, and one service per producer. Mount the Docker socket for container metrics. Pass the correct broker and topic env vars.

### Step 9: Recreate the tests

At minimum, verify that `build_event()` preserves the envelope contract.

### Step 10: Recreate the documentation itself

Keep a document like this one that explains:

- what each component does
- why it exists
- how it is configured
- how it interacts with the rest of the system

## 14. Rebuild Checklist

Use this as a practical reconstruction checklist.

- [ ] Create Python virtual environment
- [ ] Install dependencies from `requirements.txt`
- [ ] Implement `common/producer.py`
- [ ] Implement `producers/host_metrics.py`
- [ ] Implement `producers/disk_metrics.py`
- [ ] Implement `producers/container_metrics.py`
- [ ] Implement `producers/process_metrics.py`
- [ ] Implement `producers/network_metrics.py`
- [ ] Implement `producers/system_metrics.py`
- [ ] Add `run_producer.sh`
- [ ] Add `docker/Dockerfile`
- [ ] Add `docker/docker-compose.yml`
- [ ] Add tests for `build_event()`
- [ ] Verify startup and shutdown behavior
- [ ] Verify Kafka publication
- [ ] Verify container health endpoint

## 15. Summary

Node A is a small but carefully structured metrics collection system. The important design choices are:

- one shared event envelope
- one shared Kafka helper
- one producer per metric domain
- environment-based configuration
- clean shutdown and retry behavior
- container support through Docker and Docker Compose

The code is intentionally simple, but the behavior is meaningful: it collects live telemetry from the Dell server, turns it into stable Kafka events, and hands those events off to Node B for all downstream processing.

This document should be sufficient to reconstruct both the code structure and the reasoning behind it.
