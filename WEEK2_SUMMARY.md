# Week 2 Implementation Summary

## Overview
This document summarizes the Week 2 (Phase 2: Task Scheduling and Execution) implementation progress for the MapReduce framework, building upon Week 1's infrastructure.

## Implemented Features

### 1. Coordinator Task Management ✅
- Job state management with phase transitions (SUBMITTED → MAPPING → REDUCING → COMPLETED)
- Task creation and scheduling system
- Worker tracking through heartbeat mechanism
- Automatic task retry on failures

### 2. Worker Task Execution ✅
- Map task execution with intermediate file generation
- Reduce task execution with final output generation
- Progress reporting and monitoring
- Resource usage tracking (CPU, memory)
- Task status updates and error handling

Added features:
- Progress tracking with detailed status updates
- Resource monitoring for workers
- Robust error handling and reporting
- Automatic cleanup of intermediate files

### 3. Job Pipeline Management ✅
- Automatic phase transitions (map → reduce)
- Intermediate file validation
- Job completion detection
- Error handling and job failure states

### 4. Testing Status
- Unit tests for job state transitions
- Tests for task creation and execution
- Job pipeline integration tests
- Worker performance tracking tests

## Using the MapReduce System

### Setting Up
1. Start the coordinator and worker containers:
```bash
docker compose up --build
```

### Submitting a Job
```bash
# First, upload your input data to the shared directory
cp your_input_file.txt shared/input/

# Upload your MapReduce job file
cp your_job.py shared/jobs/

# Submit the job
python src/client/client.py submit \
  --input shared/input/your_input_file.txt \
  --output shared/output/result \
  --job-file shared/jobs/your_job.py \
  --num-map 4 \
  --num-reduce 2

# You'll receive a job ID in response, like: job_123456
```

### Monitoring Jobs
```bash
# Check status of a specific job
python src/client/client.py status --job-id job_123456

# List all jobs
python src/client/client.py list

# Get results when job completes
python src/client/client.py results --job-id job_123456
```

### Example Job File (word_count.py)
```python
def map_fn(key, value):
    """Count word frequencies in input text."""
    for word in value.split():
        yield (word, 1)

def reduce_fn(key, values):
    """Sum up word counts."""
    yield (key, sum(values))
```

## Running Tests
```bash
# Run all Week 2 tests
python -m pytest tests/test_job_pipeline.py tests/test_week2.py tests/test_task_executor.py -v

# Run specific test files
python -m pytest tests/test_job_pipeline.py -v  # Job state tests
python -m pytest tests/test_week2.py -v         # Task execution tests
python -m pytest tests/test_task_executor.py -v
python -m pytest tests/test_shuffle.py -v       # Shuffle phase test 1
python -m pytest tests/test_shuffle_data_fetch.py -v # shuffle phase updated test
```

## Next Steps
1. Implement worker failure recovery
2. Add performance benchmarking
3. Complete documentation updates
4. Add support for combiners (optimization)


##For Heartbeat functionality
### We have implemented
#### Heartbeat functionality:
- workers send heartbeats every 5 seconds
- coordinator tracks worker health
- timeout detection works (30 second threshold)
- worker removal works
#### Dead worker handling (partially)
- worker timeout is detected
- worker is removed from registry

### What we need to implemented - TODO
If we want to imitate the fault tolerance in the MapReduce paper, we still need to do:
#### Dead worker handling (->complete)
- tasks are not reassigned
- tasks remain stuck in "IN_PROGRESS"
- no automatic recovery

## For User Monitoring - TODO
- add a functionality to cancel a job