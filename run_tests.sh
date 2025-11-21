#!/bin/bash
# Test runner script for MapReduce project

set -e

echo "======================================"
echo "MapReduce Test Suite"
echo "======================================"

# Check if pytest is installed
if ! command -v pytest &> /dev/null; then
    echo "pytest not found. Installing test dependencies..."
    pip install -r requirements-test.txt
fi

# Run unit tests
echo ""
echo "Running unit tests..."
echo "--------------------------------------"
pytest tests/unit/ -v --cov=worker --cov=coordinator --cov-report=term-missing -m "not integration"
UNIT_EXIT=$?

# Check if Docker is available for integration tests
if command -v docker &> /dev/null && docker ps &> /dev/null; then
    echo ""
    echo "Running integration tests..."
    echo "--------------------------------------"
    echo "Note: Integration tests require Docker containers to be running"
    echo "Starting containers if not already running..."
    docker-compose up -d

    # Wait for containers to be ready
    sleep 5

    pytest tests/integration/ -v -m integration --tb=short
    INTEGRATION_EXIT=$?
else
    echo ""
    echo "Skipping integration tests (Docker not available or not running)"
    INTEGRATION_EXIT=0
fi

# Summary
echo ""
echo "======================================"
echo "Test Summary"
echo "======================================"

if [ $UNIT_EXIT -eq 0 ]; then
    echo "✓ Unit tests: PASSED"
else
    echo "✗ Unit tests: FAILED"
fi

if [ $INTEGRATION_EXIT -eq 0 ]; then
    echo "✓ Integration tests: PASSED (or skipped)"
else
    echo "✗ Integration tests: FAILED"
fi

echo "======================================"

# Exit with error if any tests failed
if [ $UNIT_EXIT -ne 0 ] || [ $INTEGRATION_EXIT -ne 0 ]; then
    exit 1
fi

echo "All tests passed!"
exit 0
