# Week 2 Implementation Summary

## Overview
Week 2 implements **Phase 2: Task Scheduling and Execution** of the MapReduce framework. This phase adds core MapReduce functionality including task scheduling, map/reduce execution, data shuffling, and job pipeline management.

## Key Implementations

### 1. Task Scheduling System ✅
- **Priority Queue**: Tasks scheduled based on priority (retried tasks get higher priority)
- **Worker Load Balancing**: Tasks assigned to workers based on available slots and performance scores
- **Straggler Detection**: Automatically creates backup tasks for slow-running tasks (>2x average duration)
- **Task Retry Mechanism**: Failed tasks retry up to 3 times with increasing priority

### 2. Input File Splitting ✅
- **Split Calculation**: Input files split into chunks for parallel map processing
- **Boundary Handling**: Splits align to newline boundaries to avoid splitting lines
- **Split Distribution**: Each map task processes only its assigned split (no duplicate processing)
- **Implementation**: Split boundaries (`start_pos`, `end_pos`) stored in `Task` objects and passed to workers via `TaskAssignment` proto

### 3. Map Task Execution ✅
- **Function Loading**: Dynamically loads `map_fn` from user-provided Python job files
- **Line-by-Line Processing**: Processes input file within assigned split boundaries
- **Partitioning**: Output partitioned by `hash(key) % num_reduce_tasks` into intermediate pickle files
- **Progress Tracking**: Reports progress at multiple stages (LOADING, READING, MAPPING, WRITING)

### 4. Shuffle Phase ✅
- **Intermediate File Collection**: Coordinator collects intermediate file locations from map task completions
- **Partition Organization**: Files organized by partition ID for reduce tasks
- **Network Fetch**: Reduce tasks fetch intermediate files from map workers via gRPC `FetchIntermediateFile` RPC
- **Data Merging**: Reduce tasks merge and sort data by key before applying reduce function

### 5. Reduce Task Execution ✅
- **Shuffle Input**: Receives list of intermediate file locations (worker address + filename) for its partition
- **Data Fetching**: Fetches intermediate files from multiple map workers via gRPC
- **Key Grouping**: Groups values by key after merging all intermediate data
- **Reduce Application**: Applies `reduce_fn(key, iter(values))` to each key group
- **Output Generation**: Writes final results to output files

### 6. Job State Management ✅
- **Phase Transitions**: SUBMITTED → MAPPING → REDUCING → COMPLETED
- **Progress Tracking**: Tracks completed map/reduce tasks for each phase
- **Automatic Transitions**: Automatically moves to reduce phase when all map tasks complete
- **Error Handling**: Jobs marked as FAILED with error messages on critical failures

### 7. Worker Heartbeat System ✅
- **Periodic Heartbeats**: Workers send heartbeats every 5 seconds
- **Health Monitoring**: Coordinator tracks worker status, available slots, and CPU usage
- **Timeout Detection**: Workers removed after 30 seconds of no heartbeat
- **Performance Scoring**: Workers scored based on CPU usage and task completion history
- **Automatic Task Reassignment**: Tasks from failed workers automatically reassigned to available workers

### 8. Task Completion Reporting ✅
- **Completion Reports**: Workers report task completion with intermediate file locations
- **Status Updates**: Coordinator updates task status and collects shuffle data
- **Intermediate File Tracking**: Files organized by partition for reduce task creation

## Architecture Components

### Coordinator (`src/coordinator/server.py`)
- **CoordinatorServicer**: Handles job submission, status queries, task completion reports
- **TaskScheduler**: Manages task queue, worker assignments, straggler detection
- **JobState**: Tracks job progress, phase transitions, intermediate file locations
- **Task**: Represents individual map/reduce tasks with state, retries, split boundaries

### Worker (`src/worker/server.py`)
- **WorkerServer**: Manages worker lifecycle, task execution, heartbeat sending
- **WorkerServicer**: Handles task assignments, status queries, intermediate file serving
- **TaskExecutor**: Executes map/reduce tasks, manages intermediate files, progress tracking

### Client (`src/client/client.py`)
QUICK_REFERENCE contains all the commands.
- **Job Submission**: Submit jobs with input/output paths and job files
- **Status Monitoring**: Check job status, progress, and phase
- **Job Management**: List jobs, get results, monitor progress

## Usage

### Start System
```bash
docker compose up --build
```

### Submit a Job
```bash
python3 src/client/client.py submit \
  --input shared/input/test.txt \
  --output shared/output/test \
  --job-file shared/jobs/test.py \
  --num-map 2 \
  --num-reduce 2
```

### Check Job Status
```bash
# Basic status
python3 src/client/client.py status <job_id>

# With task details
python3 src/client/client.py status <job_id> --tasks

# With resource usage
python3 src/client/client.py status <job_id> --resources
```

### Verify Map Output
```bash
# Check intermediate files
python3 scripts/check_map_output.py

# Check specific job
python3 scripts/check_map_output.py --job-id <job_id>
```

## Testing

### Run All Week 2 Tests
```bash
# Unit tests for coordinator logic
python3 -m pytest tests/test_week2.py -v

# Integration test for shuffle data fetch
python3 -m pytest tests/test_shuffle_data_fetch.py -v

# Map execution test
python3 -m pytest tests/test_map_execution.py -v
```

### Test Coverage

**`test_week2.py`** - Coordinator unit tests (13 tests):
- `test_task_creation`: Task object initialization
- `test_task_state_transitions`: Task lifecycle (PENDING → IN_PROGRESS → COMPLETED/FAILED)
- `test_job_state_progress`: Job phase transitions (MAP → REDUCE)
- `test_priority_queue`: Task scheduling priority
- `test_worker_performance_tracking`: Worker metrics and heartbeat handling
- `test_task_retry_mechanism`: Task retry on failures
- `test_worker_failure_recovery`: Worker timeout handling
- `test_worker_failure_automatic_reassignment`: Automatic task reassignment on worker failure
- `test_straggler_detection`: Backup task creation for slow tasks
- `test_intermediate_file_collection`: Shuffle data collection
- `test_shuffle_location_tracking`: Reduce task shuffle input setup
- `test_multiple_concurrent_jobs`: Multi-job management
- `test_task_completion_reporting`: Task completion handling

**`test_shuffle_data_fetch.py`** - Shuffle integration tests:
- `test_01_fetch_intermediate_file_success`: Successful file fetch
- `test_02_fetch_intermediate_file_not_found`: Error handling
- `test_03_fetch_internal_server_error`: Internal error handling
- `test_04_fetch_empty_data_response`: Empty file handling
- `test_05_input_file_splitting`: **Input splitting verification** (NEW)

**`test_map_execution.py`** - Map task execution tests:
- `test_map_execution_creates_files`: Intermediate file creation
- `test_intermediate_file_content`: Data structure validation
- `test_partitioning`: Partition correctness

## Key Features

### ✅ Implemented
- Task scheduling with priority queue
- Input file splitting with boundary alignment
- Map task execution with partitioning
- Shuffle phase with network data fetch
- Reduce task execution with key grouping
- Job state management and phase transitions
- Worker heartbeat and health monitoring
- Task retry mechanism (up to 3 retries)
- Worker failure recovery with automatic task reassignment (fault tolerance)
- Straggler detection and backup tasks
- Progress tracking and reporting
- Intermediate file collection and organization

### ❌ Not Yet Implemented
- **Combiner Support**: Local reduce operations before writing intermediate files (Week 3)
- **Job Cancellation**: Cancel running jobs (RPC not implemented)
- **Detailed Resource Monitoring**: CPU/memory/I/O metrics per job (RPCs not implemented)
- **Benchmarks**

## Implementation Details

### Input Splitting
- Files split by byte position, aligned to newline boundaries
- Split boundaries stored in `Task.start_pos` and `Task.end_pos`
- Passed to workers via `TaskAssignment` proto fields
- Workers process only their assigned split range

### Data Flow
1. **Submit Job** → Coordinator splits input, creates map tasks
2. **Schedule Tasks** → Tasks assigned to available workers
3. **Execute Map** → Workers process splits, write partitioned intermediate files
4. **Report Completion** → Workers report intermediate file locations
5. **Create Reduce Tasks** → Coordinator creates reduce tasks with shuffle locations
6. **Shuffle** → Reduce tasks fetch intermediate files from map workers
7. **Execute Reduce** → Workers merge, sort, and reduce data
8. **Job Complete** → Final output files written

### Fault Tolerance
- **Task Retries**: Failed tasks automatically retried (max 3 attempts)
- **Worker Timeouts**: Workers removed after 30s of no heartbeat
- **Automatic Task Reassignment**: Tasks from failed workers automatically reset to PENDING and re-queued for reassignment
  - Finds all tasks assigned to failed worker (from scheduler and job states)
  - Resets task status to PENDING and clears assigned_worker
  - Increments retries for higher priority
  - Removes tasks from scheduler tracking (running_tasks, task_start_times)
  - Re-queues tasks for reassignment to available workers
- **Straggler Handling**: Backup tasks created for slow-running tasks

## Files Modified/Created

### Core Implementation
- `src/coordinator/server.py`: Task scheduling, job state, split calculation, worker failure recovery
- `src/worker/server.py`: Task execution, completion reporting, heartbeat
- `src/worker/task_executor.py`: Map/reduce execution, file I/O, partitioning
- `src/client/client.py`: Job submission, status monitoring
- `src/client/monitoring.py`: Progress bars, resource display

### Protocol Buffers
- `proto/worker.proto`: Added `start_pos` and `end_pos` to `TaskAssignment`
- `proto/coordinator.proto`: `ReportTaskCompletion` and `Heartbeat` RPCs

### Tests
- `tests/test_week2.py`: 13 unit tests for coordinator logic (including worker failure recovery)
- `tests/test_shuffle_data_fetch.py`: 5 integration tests including input splitting
- `tests/test_map_execution.py`: Map execution verification

### Utilities
- `scripts/check_map_output.py`: Verify intermediate file creation
- `QUICK_REFERENCE.md`: Command reference guide

## Regenerating Protobuf Files

After modifying `.proto` files, regenerate Python code:
```bash
# locally (requires grpcio-tools)
python3 -m grpc_tools.protoc \
  -I./proto --python_out=./src --grpc_python_out=./src \
  proto/coordinator.proto proto/worker.proto
```

## TODO - Remaining Work

### Client Commands
- [ ] **Job Cancellation**: Implement CancelJob RPC
  - Add `CancelJob` RPC to `coordinator.proto`
  - Implement cancellation logic in coordinator
  - Stop running tasks, mark job as CANCELLED

### Combiner (merge)
- [ ] **Combiner Support** (Week 3): Implement local reduce before writing intermediate files
  - Load optional `combine_fn` from job files
  - Group map output by key within each partition
  - Apply combiner before writing intermediate files

### Performance Metrics (!)
- [ ] **Performance Metrics**: Add detailed resource monitoring RPCs
  - `GetResourceUsage` RPC for per-job metrics
  - `ListActiveTasks` RPC for task-level details
  - Track CPU, memory, I/O per job

### Other
- [ ] **Documentation Updates**: Update ARCHITECTURE.md with task scheduling details
- [ ] **Error Recovery**: Improve error messages and recovery strategies
- [ ] **Performance Optimization**: Optimize shuffle data transfer for large files
