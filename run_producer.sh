#!/bin/bash

set -e

# Validate argument
if [ -z "$1" ]; then
    echo "Usage: $0 <module_name>"
    echo "Example: $0 container_metrics"
    exit 1
fi

MODULE_NAME="$1"

# Allow optional .py
if [[ "$MODULE_NAME" != *.py ]]; then
    MODULE_NAME="${MODULE_NAME}.py"
fi

MODULE_PATH="./producers/$MODULE_NAME"

if [ ! -f "$MODULE_PATH" ]; then
    echo "Error: Producer module not found: $MODULE_PATH"
    exit 1
fi

# Defaults (override via docker-compose env)
export KAFKA_TOPIC=${KAFKA_TOPIC:-"metrics.topic"}
export NETWORK_METRICS_INTERVAL=${NETWORK_METRICS_INTERVAL:-"2"}
export DISK_METRICS_INTERVAL=${DISK_METRICS_INTERVAL:-"2"}
export HOST_METRICS_INTERVAL=${HOST_METRICS_INTERVAL:-"2"}
export PROCESS_METRICS_INTERVAL=${PROCESS_METRICS_INTERVAL:-"2"}
export SYSTEM_METRICS_INTERVAL=${SYSTEM_METRICS_INTERVAL:-"2"}

echo "Starting producer: $MODULE_NAME"
echo "Using topic: $KAFKA_TOPIC"

# IMPORTANT: exec replaces shell (proper signal handling)
exec python -u "$MODULE_PATH"