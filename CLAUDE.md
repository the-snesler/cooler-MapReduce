# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A simplified implementation of Google's MapReduce framework deployed using Docker containers on a single VM. The system consists of:
- **Coordinator**: Manages job submission, task distribution, and worker coordination (port 50051)
- **Workers**: Execute map and reduce tasks (4 workers, each limited to 1 CPU)
- **Client**: Command-line tool for job submission and monitoring
- **Shared Storage**: Common volume mounted to all containers at `/shared`

Built with Python 3.9+ using gRPC for inter-component communication.

## Development Commands

### Generate gRPC Code from Proto Files
After modifying `.proto` files in the `proto/` directory:
```bash
python -m grpc_tools.protoc -I./proto --python_out=./src --grpc_python_out=./src proto/coordinator.proto proto/worker.proto
```

### Running Tests
```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test files
python -m pytest tests/test_week2.py -v              # Week 2 task execution
python -m pytest tests/test_job_pipeline.py -v       # Job state transitions
python -m pytest tests/test_task_executor.py -v      # Task executor unit tests

# Run with timeout protection
python -m pytest tests/ -v --timeout=30
```

### Docker Operations
```bash
# Build and start all containers
docker compose up --build

# Start containers in detached mode
docker compose up -d

# Stop and remove containers
docker compose down

# View logs
docker compose logs -f coordinator
docker compose logs -f worker1

# Rebuild after code changes
docker compose build
docker compose up
```

### Running Components Locally (Development)
```bash
# Run coordinator
python src/coordinator/server.py

# Run worker
python src/worker/server.py --worker-id worker-1 --coordinator localhost:50051 --port 50052

# Run client (using helper script)
./mapreduce.sh submit --input /shared/input/data.txt --output /shared/output/result --job-file /shared/jobs/wordcount.py --num-map 8 --num-reduce 4

# Or run client directly
PYTHONPATH=$PYTHONPATH:$(pwd)/src python src/client/client.py <command>
```

### Client Commands
```bash
# Submit a job
./mapreduce.sh submit \
  --input /shared/input/data.txt \
  --output /shared/output/result \
  --job-file /shared/jobs/wordcount.py \
  --num-map 8 \
  --num-reduce 4

# Check job status
./mapreduce.sh status --job-id <job_id>

# List all jobs
./mapreduce.sh list

# Get job results
./mapreduce.sh results --job-id <job_id>
```

## Architecture and Key Patterns

### Job Lifecycle and State Management
Jobs transition through states managed by the coordinator:
1. **SUBMITTED** → Job created, awaiting task creation
2. **MAPPING** → Map tasks being executed by workers
3. **REDUCING** → All maps complete, reduce tasks executing
4. **COMPLETED** → All reduce tasks complete, results ready
5. **FAILED** → Job failed (alternative terminal state)

Key coordinator code: `src/coordinator/server.py`
- `JobState` class manages job state and phase transitions
- `transition_to_reduce_phase()` validates intermediate files before proceeding
- Task tracking via `map_tasks` and `reduce_tasks` dictionaries

### Task Assignment and Execution Flow
1. Coordinator creates tasks from job request (`_create_map_tasks`, `_create_reduce_tasks`)
2. Workers send heartbeats to coordinator to indicate availability
3. Coordinator assigns tasks to idle workers via `AssignTask` gRPC call
4. Workers execute tasks using `TaskExecutor` class (`src/worker/task_executor.py`)
5. Workers report progress and completion back to coordinator

### Data Flow and File Organization
```
/shared/
├── input/              # Input data files uploaded by user
├── jobs/               # Job definition files (pickled map_fn/reduce_fn)
├── intermediate/       # Map outputs: {job_id}_map_{task_id}_part_{partition}.pickle
└── output/             # Final results: {job_id}_reduce_{task_id}.txt
```

Map tasks:
- Read input chunks from `/shared/input`
- Apply `map_fn` from job file
- Partition outputs by `hash(key) % num_reduce_tasks`
- Write partitioned outputs to `/shared/intermediate` as pickle files

Reduce tasks:
- Read intermediate files matching their partition ID
- Group by key and apply `reduce_fn`
- Write final output to `/shared/output` as text files

### gRPC Services
Two proto definitions in `proto/`:
- `coordinator.proto`: Client → Coordinator (SubmitJob, GetJobStatus, ListJobs, GetJobResults)
- `worker.proto`: Coordinator ↔ Workers (Heartbeat, AssignTask, GetTaskStatus)

Generated Python files are in `src/`:
- `coordinator_pb2.py`, `coordinator_pb2_grpc.py`
- `worker_pb2.py`, `worker_pb2_grpc.py`

### Worker Task Execution
`src/worker/task_executor.py` contains the `TaskExecutor` class:
- `execute_map_task()`: Loads job functions, processes input, partitions output
- `execute_reduce_task()`: Reads intermediate files, groups by key, writes output
- Progress tracking via `_update_progress()` and `get_task_progress()`
- Resource monitoring via psutil for CPU/memory usage

### Failure Handling
- Workers track task state and report failures
- Coordinator can reassign failed tasks to different workers
- Jobs marked FAILED if unrecoverable errors occur
- Intermediate file validation prevents proceeding with corrupt data

## Job File Format
User-provided Python files in `/shared/jobs` must define:
```python
def map_fn(key, value):
    """
    Args:
        key: Input key (e.g., filename)
        value: Input value (e.g., line of text)
    Yields:
        (intermediate_key, intermediate_value) tuples
    """
    # Example: word count
    for word in value.split():
        yield (word, 1)

def reduce_fn(key, values):
    """
    Args:
        key: Intermediate key from map outputs
        values: Iterator of values for this key
    Yields:
        (key, final_value) tuples
    """
    # Example: sum counts
    yield (key, sum(values))
```

Optional `combine_fn` (same signature as `reduce_fn`) can be added for local aggregation optimization.

## Important Implementation Notes

### Working with Shared Storage
- All file paths in gRPC requests use `/shared` prefix (container perspective)
- Local development may need `./shared` prefix (host perspective)
- Always ensure intermediate and output directories exist before writing

### gRPC Context
- Coordinator runs on `0.0.0.0:50051` in containers, `localhost:50051` from host
- Workers register via heartbeat with their container hostname (e.g., `worker-1`)
- Client connects to coordinator via `--coordinator-host` parameter

### Task Serialization
- Job functions are pickled and stored in `/shared/jobs/{job_id}.pickle`
- Intermediate data uses pickle format for efficient Python object serialization
- Final outputs are text files (tab-separated key-value pairs)

### Threading and Concurrency
- Coordinator uses locks for thread-safe job state updates
- Workers use `ThreadPoolExecutor` for concurrent task execution
- gRPC servers use `concurrent.futures.ThreadPoolExecutor` for request handling

## Development Workflow

1. **Modify proto files** → Regenerate gRPC code → Update server/client implementations
2. **Code changes** → Write tests → Run pytest → Build Docker images → Test in containers
3. **New features** → Update relevant components (coordinator/worker/client) → Integration test

## Testing Strategy
- Unit tests for individual components (task executor, job state)
- Integration tests for end-to-end job execution
- Test files include sample jobs (word count, weather stats) in `shared/samples/`
- Tests use pytest fixtures for setup/teardown
