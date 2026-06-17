#!/bin/bash
# Script to run a specific metric producer in a containerized environment

# Check if a module name was provided as an argument
if [ -z "$1" ]; then
    echo "Usage: $0 <module_name>"
    echo "Example: $0 container_metrics.py"
    exit 1
fi

MODULE_FILE="$1"

if [ ! -f "./producers/$MODULE_FILE" ]; then
    echo "Error: Producer module file not found at ./producers/$MODULE_FILE"
    exit 1
fi

# Execute the specified producer script. We pass all environment variables
# that were available in the original execution context to maintain consistency.
export KAFKA_TOPIC=${KAFKA_TOPIC:-"network.metrics"} # Use default if not set
export NETWORK_METRICS_INTERVAL=${NETWORK_METRICS_INTERVAL:-"2"}

echo "Starting producer for module: $MODULE_FILE..."
# Execute the script directly to run its main logic
python3 ./producers/$MODULE_FILE