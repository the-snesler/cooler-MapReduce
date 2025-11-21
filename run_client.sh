#!/bin/bash
# MapReduce Client Docker Wrapper
# Runs the client CLI in a Docker container with access to the MapReduce network and shared storage

set -e

# Check if docker is available
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed or not in PATH"
    exit 1
fi

# Check if mapreduce network exists
if ! docker network ls | grep -q mapreduce_mapreduce-net; then
    echo "Warning: MapReduce network 'mapreduce_mapreduce-net' not found"
    echo "Make sure the MapReduce system is running (./startup.sh)"
fi

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Run client in Docker container
docker run --rm -i \
    --network mapreduce_mapreduce-net \
    -v mapreduce-data:/mapreduce-data \
    -v "${SCRIPT_DIR}":/app \
    -w /app \
    -e COORDINATOR_HOST=coordinator:50051 \
    -e SHARED_VOLUME=/mapreduce-data \
    python:3.9-slim \
    bash -c "pip install -q -r client/requirements.txt && python client/client.py $*"
