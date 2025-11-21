# Coolest MapReduce

A simplified implementation of MapReduce for educational purposes, demonstrating distributed parallel computation on a single VM using Docker containers.

## Features

- **Distributed execution**: 4 worker containers processing tasks in parallel
- **Combiner optimization**: Local aggregation reduces network transfer and improves performance
- **Easy job submission**: Simple CLI for running custom MapReduce jobs
- **Example jobs**: Word count and inverted index implementations included
- **Performance metrics**: Detailed timing and data size measurements
- **Comprehensive testing**: Unit and integration tests with >80% coverage

## Architecture Overview

```
┌─────────┐
│ Client  │  <-- Python CLI for job submission and monitoring
└────┬────┘
     │ gRPC
     ▼
┌─────────────┐
│ Coordinator │  <-- Manages jobs, assigns tasks, tracks progress
└──────┬──────┘
       │ gRPC
       ▼
┌──────────────────────────────────┐
│ Workers (x4)                     │  <-- Execute map and reduce tasks
│ worker-1  worker-2  worker-3  worker-4 │
│ [1 CPU each]                     │
└──────┬───────────────────────────┘
       │
       ▼
┌───────────────┐
│ Shared Volume │  <-- /mapreduce-data (inputs, intermediate, outputs)
└───────────────┘
```

## Prerequisites

- **Docker** 20.10 or later
- **Docker Compose** 2.0 or later
- **Python** 3.9 or later
- **System requirements**: 4+ CPU cores, 8GB+ RAM

## Quick Start

### 1. Clone and Build

```bash
git clone <repository-url>
cd coolest-mapreduce
docker-compose build
```

This will build Docker images for the coordinator and workers.

### 2. Start Containers

```bash
docker-compose up -d
```

Verify all containers are running:

```bash
docker-compose ps
```

You should see:
- `coordinator` (running on port 50051)
- `worker-1`, `worker-2`, `worker-3`, `worker-4` (each limited to 1 CPU core)

### 3. Run Example Job

```bash
# Set up example data (downloads Shakespeare text)
./examples/setup_examples.sh

# Run word count example
./examples/run_wordcount.sh
```

The word count job will:
1. Upload Shakespeare complete works (~5MB)
2. Submit job with 8 map tasks and 4 reduce tasks
3. Poll for completion
4. Display top 20 most frequent words

Expected output:
```
the     25000
and     20000
to      15000
...
```

## Custom Jobs

### Step 1: Write Map/Reduce Functions

Create a Python file (e.g., `my_job.py`) with these three functions:

```python
def map_function(key, value):
    """
    Process input record and emit key-value pairs.

    Args:
        key: Line number (int)
        value: Line content (str)

    Yields:
        (output_key, output_value): Tuples to be shuffled
    """
    # Your map logic here
    yield (output_key, output_value)

def reduce_function(key, values):
    """
    Aggregate values for a key.

    Args:
        key: The key to process
        values: List of all values for this key

    Yields:
        (output_key, output_value): Final result
    """
    # Your reduce logic here
    yield (output_key, output_value)

def combiner_function(key, values):  # Optional
    """
    Pre-aggregate values locally (combiner optimization).

    Args:
        key: The key to process
        values: List of values from local map task

    Yields:
        (output_key, output_value): Partially aggregated result
    """
    # Usually same as reduce function
    yield (output_key, output_value)
```

### Step 2: Upload Input Data

```bash
python3 client/client.py upload-data my_data.txt /mapreduce-data/inputs/my_data.txt
```

### Step 3: Submit Job

```bash
python3 client/client.py submit-job \
  --input /mapreduce-data/inputs/my_data.txt \
  --output /mapreduce-data/outputs/my_result \
  --job-file my_job.py \
  --num-map-tasks 8 \
  --num-reduce-tasks 4 \
  --use-combiner \
  --job-id my-custom-job
```

Parameters:
- `--input`: Path to input file in shared volume
- `--output`: Output directory in shared volume
- `--job-file`: Your Python file with map/reduce functions
- `--num-map-tasks`: Number of map tasks (M) - typically 2-4x number of workers
- `--num-reduce-tasks`: Number of reduce tasks (R) - determines output partitions
- `--use-combiner`: Enable combiner optimization (optional, recommended)
- `--job-id`: Custom job identifier (optional, auto-generated if omitted)

### Step 4: Monitor Job Status

```bash
python3 client/client.py job-status my-custom-job
```

Output:
```
Job ID: my-custom-job
Status: map_phase
Progress: 50.0%
Completed tasks: 4 / 8
```

Status values:
- `pending`: Job accepted, not started
- `map_phase`: Map tasks executing
- `reduce_phase`: Reduce tasks executing
- `completed`: Job finished successfully
- `failed`: Job failed (check coordinator logs)

### Step 5: Get Results

```bash
python3 client/client.py get-results my-custom-job
```

Output:
```
Job ID: my-custom-job
Status: completed
Output: /mapreduce-data/outputs/my_result/

Performance Metrics:
  Execution time: 12.5 seconds
  Map phase: 8.2 seconds
  Reduce phase: 4.3 seconds
  Input size: 5.2 MB
  Intermediate size: 2.1 MB
  Output size: 0.8 MB
  Combiner reduction: 60%
```

### Step 6: Access Output Files

Output is split across multiple files (one per reduce task):

```bash
# List output files
docker exec coordinator ls /mapreduce-data/outputs/my_result/

# View output (example)
docker exec coordinator cat /mapreduce-data/outputs/my_result/part-0.txt
docker exec coordinator cat /mapreduce-data/outputs/my_result/part-1.txt
docker exec coordinator cat /mapreduce-data/outputs/my_result/part-2.txt
docker exec coordinator cat /mapreduce-data/outputs/my_result/part-3.txt
```

Output format: Tab-separated key-value pairs, sorted by key within each partition.

## Example Jobs

### Word Count

Classic MapReduce example that counts word frequencies.

```bash
./examples/run_wordcount.sh
```

See [examples/README.md](examples/README.md) for detailed explanation.

### Inverted Index

Builds an inverted index mapping words to document IDs.

```bash
./examples/run_inverted_index.sh
```

See [examples/README.md](examples/README.md) for detailed explanation.

## Testing

### Run All Tests

```bash
./run_tests.sh
```

This runs:
1. Unit tests (fast, no Docker required)
2. Integration tests (slower, requires Docker containers)

### Unit Tests Only

```bash
pytest tests/unit/ -v
```

### Integration Tests Only

```bash
docker-compose up -d
pytest tests/integration/ -v -m integration
```

### Coverage Report

```bash
pytest tests/unit/ --cov=worker --cov=coordinator --cov-report=html
open htmlcov/index.html
```

See [tests/README.md](tests/README.md) for detailed testing documentation.

## Performance Analysis

Compare performance with and without combiner:

```bash
python3 tools/compare_performance.py wordcount examples/wordcount.py /mapreduce-data/inputs/shakespeare.txt
```

Generate performance visualizations:

```bash
python3 tools/visualize_performance.py
```

Creates charts:
- `performance_comparison.png`: Execution time comparison
- `phase_breakdown.png`: Time spent in each phase
- `data_size_comparison.png`: Data size reduction from combiner

## Documentation

- **[Design Document](design.md)**: Detailed architecture, technology choices, and implementation details
- **[Performance Report](performance_report.md)**: Performance evaluation results and analysis
- **[Examples README](examples/README.md)**: Example jobs and custom job tutorial
- **[Testing README](tests/README.md)**: Testing strategy and how to run tests

## Troubleshooting

### Containers won't start

```bash
# Stop and remove all containers
docker-compose down

# Rebuild images
docker-compose build

# Start containers
docker-compose up -d

# Check logs
docker-compose logs coordinator
docker-compose logs worker-1
```

### Job stuck in map_phase or reduce_phase

1. Check worker logs for errors:
   ```bash
   docker-compose logs worker-1
   docker-compose logs worker-2
   docker-compose logs worker-3
   docker-compose logs worker-4
   ```

2. Verify input file exists:
   ```bash
   docker exec coordinator ls /mapreduce-data/inputs/
   ```

3. Check for Python errors in map/reduce functions (common issues: syntax errors, missing imports)

### Permission denied errors

Make scripts executable:

```bash
chmod +x examples/*.sh
chmod +x run_tests.sh
chmod +x client/client.py
```

### gRPC connection refused

Ensure coordinator is running:

```bash
docker-compose ps coordinator
```

If not running, check logs:

```bash
docker-compose logs coordinator
```

Common causes:
- Port 50051 already in use
- Coordinator failed to start (check logs with `docker-compose logs coordinator`)

### Job fails with "module not found"

Your job file may import libraries not available in worker containers.

Option 1: Use only Python standard library
Option 2: Modify `worker/Dockerfile` to install required packages:

```dockerfile
RUN pip install <your-package>
```

Then rebuild:

```bash
docker-compose build
docker-compose up -d
```

### Docker volume permission issues

If you get permission errors accessing `/mapreduce-data`:

```bash
# Linux: Fix permissions
docker run --rm -v mapreduce-data:/data busybox chmod -R 777 /data

# Or recreate volume
docker-compose down -v
docker-compose up -d
```

### Can't see output files from host machine

Output files are inside the Docker volume. Access them using:

```bash
# Method 1: Use docker exec
docker exec coordinator cat /mapreduce-data/outputs/<job-id>/part-0.txt

# Method 2: Copy to host
docker cp coordinator:/mapreduce-data/outputs/<job-id> ./output-local

# Method 3: Use get-results with --download
python3 client/client.py get-results <job-id> --download ./output-local
```

## Project Structure

```
coolest-mapreduce/
├── client/                  # Client CLI for job submission
│   └── client.py
├── coordinator/             # Coordinator server
│   ├── coordinator_server.py
│   ├── job_manager.py
│   └── metrics.py
├── worker/                  # Worker processes
│   ├── worker_server.py
│   ├── map_executor.py
│   ├── reduce_executor.py
│   └── function_loader.py
├── examples/                # Example MapReduce jobs
│   ├── wordcount.py
│   ├── inverted_index.py
│   ├── setup_examples.sh
│   └── README.md
├── tests/                   # Test suite
│   ├── unit/
│   ├── integration/
│   └── README.md
├── tools/                   # Performance analysis tools
│   ├── compare_performance.py
│   └── visualize_performance.py
├── mapreduce.proto          # Protocol Buffer definition
├── docker-compose.yml       # Container orchestration
├── run_tests.sh            # Test runner
├── design.md               # Design document
├── performance_report.md   # Performance evaluation
└── README.md               # This file
```

## System Requirements

### Minimum

- 4 CPU cores
- 8 GB RAM
- 10 GB disk space
- Docker 20.10+

### Recommended

- 8 CPU cores
- 16 GB RAM
- 20 GB disk space
- Docker 24.0+
- SSD for better I/O performance

## Configuration

### Adjust Number of Workers

Edit `docker-compose.yml`:

```yaml
services:
  worker-1:
    ...
  worker-2:
    ...
  # Add more workers
  worker-5:
    build: ./worker
    container_name: worker-5
    volumes:
      - mapreduce-data:/mapreduce-data
    networks:
      - mapreduce-net
    deploy:
      resources:
        limits:
          cpus: '1.0'
```

### Adjust CPU Limits

Edit `docker-compose.yml`:

```yaml
deploy:
  resources:
    limits:
      cpus: '2.0'  # Allow 2 CPUs per worker
```

### Change Coordinator Port

Edit `docker-compose.yml`:

```yaml
coordinator:
  ports:
    - "50052:50051"  # Map host port 50052 to container port 50051
```

Update client to use new port in `client/client.py`:

```python
channel = grpc.insecure_channel('coordinator:50052')
```

## Development

### Rebuilding After Code Changes

```bash
docker-compose down
docker-compose build
docker-compose up -d
```

### Viewing Logs in Real-Time

```bash
# All containers
docker-compose logs -f

# Specific container
docker-compose logs -f coordinator
docker-compose logs -f worker-1
```

### Debugging gRPC Communication

Enable gRPC debug logging in coordinator or worker:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Cleaning Up

```bash
# Stop containers
docker-compose down

# Remove volumes (deletes all data)
docker-compose down -v

# Remove images
docker rmi coolest-mapreduce-coordinator coolest-mapreduce-worker
```

## License

This project is for educational purposes. See LICENSE file for details.

## Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Acknowledgments

Based on the MapReduce paper by Dean and Ghemawat (2004):
"MapReduce: Simplified Data Processing on Large Clusters"

## Contact

For issues or questions, please open an issue on GitHub.
