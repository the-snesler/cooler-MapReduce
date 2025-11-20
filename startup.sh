#!/bin/bash
# Startup script for MapReduce infrastructure

set -e

echo "========================================"
echo "MapReduce Infrastructure Startup"
echo "========================================"
echo ""

# Initialize storage
echo "Step 1: Initializing shared storage..."
./init_storage.sh
echo ""

# Build Docker images
echo "Step 2: Building Docker images..."
docker-compose build
echo ""

# Start containers
echo "Step 3: Starting containers..."
docker-compose up -d
echo ""

# Wait for containers to be ready
echo "Step 4: Waiting for containers to start (10 seconds)..."
sleep 10
echo ""

# Display running containers
echo "Step 5: Verifying container status..."
docker-compose ps
echo ""

# Display resource usage
echo "Step 6: Container resource usage:"
docker stats --no-stream
echo ""

# Success message
echo "========================================"
echo "✓ Infrastructure started successfully!"
echo "========================================"
echo ""
echo "Services running:"
echo "  - Coordinator:  localhost:50051"
echo "  - 4 Workers:    worker-1, worker-2, worker-3, worker-4"
echo ""
echo "Shared volume:  mapreduce-data"
echo ""
echo "Next steps:"
echo "  - Run './test_infrastructure.sh' to validate setup"
echo "  - Run 'docker-compose logs -f' to view logs"
echo "  - Run './shutdown.sh' to stop all services"
echo ""
