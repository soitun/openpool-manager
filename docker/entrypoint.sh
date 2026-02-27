#!/bin/bash
set -e

# Define paths
TEMPLATE_FILE="$DAGSTER_HOME/dagster-template.yaml"
TARGET_FILE="$DAGSTER_HOME/dagster.yaml"

# Check if template exists
if [ -f "$TEMPLATE_FILE" ]; then
    # Copy template to target
    cp $TEMPLATE_FILE $TARGET_FILE

    # Replace network placeholder
    if [ ! -z "$DOCKER_NETWORK" ]; then
        sed -i "s/NETWORK_PLACEHOLDER/$DOCKER_NETWORK/g" $TARGET_FILE
        echo "Set Docker network to: $DOCKER_NETWORK"
    fi

    # Replace volume placeholder
    if [ ! -z "$DOCKER_VOLUME" ]; then
        sed -i "s/VOLUME_PLACEHOLDER/$DOCKER_VOLUME/g" $TARGET_FILE
        echo "Set Docker volume to: $DOCKER_VOLUME"
    fi
else
    echo "Warning: $TEMPLATE_FILE not found, using existing dagster.yaml"
fi

# Execute the original command
exec "$@"