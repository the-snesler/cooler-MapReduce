#!/bin/bash
# Initialize MapReduce shared storage volume

set -e

echo "Initializing MapReduce shared storage..."

# Create Docker volume
echo "Creating Docker volume 'mapreduce-data'..."
docker volume create mapreduce-data

# Initialize directory structure
echo "Setting up directory structure..."
docker run --rm -v mapreduce-data:/data busybox sh -c '
    mkdir -p /data/inputs /data/intermediate /data/outputs /data/jobs
    chmod -R 777 /data
    echo "Created directories:"
    ls -la /data
'

echo ""
echo "Storage initialization complete!"
echo ""
echo "Directory structure:"
echo "  /mapreduce-data/inputs/       - User-uploaded input files"
echo "  /mapreduce-data/intermediate/ - Map task outputs organized by partition"
echo "  /mapreduce-data/outputs/      - Final reduce task outputs"
echo "  /mapreduce-data/jobs/         - Job metadata and state files"
