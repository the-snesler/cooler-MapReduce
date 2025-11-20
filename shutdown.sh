#!/bin/bash
# Shutdown script for MapReduce infrastructure

echo "========================================"
echo "MapReduce Infrastructure Shutdown"
echo "========================================"
echo ""

# Stop and remove containers
echo "Stopping all containers..."
docker-compose down
echo ""

# Ask about volume removal
read -p "Do you want to remove the shared volume 'mapreduce-data'? (y/N): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Removing Docker volume..."
    docker volume rm mapreduce-data 2>/dev/null || echo "Volume already removed or doesn't exist"
    echo ""
fi

echo "========================================"
echo "✓ Infrastructure shutdown complete!"
echo "========================================"
echo ""
