# Week 2 Implementation Checklist

Based on the design.md Phase 2 requirements and Week 1 completion:

## Phase 2: Task Scheduling and Execution

### Coordinator Task Management ✅
- [x] Implement task creation in SubmitJob
  - [x] Split input into map tasks based on num_map_tasks
  - [x] **Store split boundaries (start_pos, end_pos) in Task objects** ✅
  - [x] Create task records with unique IDs
  - [x] Store task metadata (input splits, state, assigned worker)
- [x] Implement task scheduling logic
  - [x] Track available workers from heartbeats
  - [x] Assign tasks to workers with available slots
  - [x] **Handle task failures and retry** ✅
  - [ ] **Handle worker failures and task reassignment** ⚠️ (detection works, reassignment missing)
  - [x] Track task completion and job progress
- [x] Add task queue and worker load balancing
  - [x] Implement priority queue for pending tasks
  - [x] Consider worker CPU usage in assignment
  - [x] Handle stragglers (slow workers) with backup tasks

### Worker Task Execution ✅
- [x] Implement map task execution
  - [x] Load and validate user's map_fn
  - [x] **Process input splits with split boundaries (start_pos, end_pos)** ✅
  - [x] Write intermediate files with correct partitioning
  - [x] Report progress and completion to coordinator
- [x] Implement reduce task execution
  - [x] **Fetch intermediate files from map workers via gRPC** ✅
  - [x] Load and execute user's reduce_fn
  - [x] Write final output files
  - [x] Clean up intermediate files
- [x] Add task status reporting
  - [x] Periodic progress updates
  - [x] Error handling and reporting
  - [x] Resource usage monitoring (CPU)

### Job Pipeline Management ✅
- [x] Implement map phase completion detection
  - [x] Track all map tasks for a job
  - [x] Validate intermediate files
  - [x] Trigger reduce phase start
- [x] Implement reduce phase management
  - [x] **Create reduce tasks with shuffle input locations** ✅
  - [x] Track reduce task completion
  - [x] Mark job as completed
- [x] Add job status transitions
  - [x] SUBMITTED → MAPPING → REDUCING → COMPLETED
  - [x] Handle failures at each stage
  - [ ] Support job cancellation ❌ (RPC not implemented)

### Shuffle Phase ✅
- [x] Intermediate file collection
  - [x] Collect file locations from map task completions
  - [x] Organize files by partition ID
  - [x] Store worker addresses for shuffle
- [x] Network data transfer
  - [x] Reduce tasks fetch files via `FetchIntermediateFile` RPC
  - [x] Handle fetch errors and retries
  - [x] Merge and sort data by key

### Client Enhancements ✅
- [x] Add progress monitoring
  - [x] Show map/reduce task progress (with progress bars)
  - [x] Display current phase
  - [x] Show job status and error messages
- [x] Improve error reporting
  - [x] Detailed task failure information
  - [x] Worker failure notifications
  - [x] Job failure analysis
- [x] Add job control features
  - [x] List all jobs
  - [x] Get job status
  - [x] Get job results
  - [ ] Cancel running job ❌ (RPC not implemented)
  - [x] List active tasks (via status command)
  - [x] Show resource usage (basic, via status command)

### Testing Enhancements ✅
- [x] Unit tests (`tests/test_week2.py`)
  - [x] Task creation and scheduling
  - [x] Task state transitions
  - [x] Job state progress tracking
  - [x] Priority queue logic
  - [x] Worker performance tracking
  - [x] Task retry mechanism
  - [x] Worker failure recovery (timeout detection)
  - [x] Straggler detection
  - [x] Intermediate file collection
  - [x] Shuffle location tracking
  - [x] Multiple concurrent jobs
  - [x] Task completion reporting
- [x] Integration tests
  - [x] End-to-end job execution (verified with real jobs)
  - [x] **Input file splitting verification** (`test_shuffle_data_fetch.py::test_05_input_file_splitting`) ✅
  - [x] Shuffle data fetch (`test_shuffle_data_fetch.py`)
  - [x] Map execution (`test_map_execution.py`)
  - [ ] Worker failure recovery (automatic reassignment) ⚠️
  - [x] Multiple concurrent jobs
- [x] Performance tests
  - [x] Measure task completion times
  - [x] Monitor resource usage
  - [x] Test with varying task counts

### Documentation Updates ✅
- [x] Create WEEK2_SUMMARY.md
  - [x] Implementation highlights
  - [x] Testing results
  - [x] Usage instructions
  - [x] Test execution guide
- [ ] Update ARCHITECTURE.md
  - [ ] Task scheduling details
  - [ ] Execution flow diagrams
  - [ ] Failure handling
- [ ] Update README.md
  - [ ] New features and capabilities
  - [ ] Updated usage examples
  - [ ] Troubleshooting guide

## Files Modified/Created

### Core Implementation
1. **src/coordinator/server.py**
   - ✅ Task creation with split boundaries
   - ✅ Task scheduling with priority queue
   - ✅ Job state machine with phase transitions
   - ✅ Worker heartbeat handling
   - ✅ Task completion reporting
   - ✅ Straggler detection
   - ✅ Input file splitting logic

2. **src/worker/server.py**
   - ✅ Map/reduce execution
   - ✅ Progress reporting
   - ✅ Task completion reporting to coordinator
   - ✅ Heartbeat sending
   - ✅ Intermediate file serving (`FetchIntermediateFile`)

3. **src/worker/task_executor.py**
   - ✅ Map task execution with split boundaries
   - ✅ Reduce task execution with shuffle fetch
   - ✅ Job function loading
   - ✅ Intermediate file management

4. **src/client/client.py**
   - ✅ Job submission with path normalization
   - ✅ Status monitoring
   - ✅ Job listing and results

5. **src/client/monitoring.py**
   - ✅ Progress bars
   - ✅ Status display (using available RPCs)

### Protocol Buffers
- **proto/worker.proto**
  - ✅ Added `start_pos` and `end_pos` to `TaskAssignment`
- **proto/coordinator.proto**
  - ✅ `ReportTaskCompletion` RPC
  - ✅ `Heartbeat` RPC (moved from worker.proto)

### Testing
1. **tests/test_week2.py** - 12 unit tests
2. **tests/test_shuffle_data_fetch.py** - 5 integration tests (including input splitting)
3. **tests/test_map_execution.py** - 3 map execution tests

### Utilities
- **scripts/check_map_output.py** - Verify intermediate file creation
- **QUICK_REFERENCE.md** - Command reference guide

## Verification Steps ✅

### Functionality
- [x] Map tasks execute and produce intermediate files
- [x] **Input splitting works correctly (no duplicate processing)** ✅
- [x] Reduce tasks process intermediates correctly
- [x] Jobs complete end-to-end successfully
- [x] Shuffle phase works (network data fetch)
- [x] Task retries work on failures
- [x] Progress reporting is accurate
- [ ] Worker failure recovery (automatic reassignment) ⚠️

### Performance
- [x] Tasks are distributed evenly
- [x] Worker CPU limits are respected
- [x] Reasonable completion times
- [x] No memory leaks
- [x] Input splitting enables proper parallelism

### Reliability
- [x] All new tests pass
- [x] System handles task failures (retries)
- [x] No deadlocks or race conditions
- [x] Intermediate/output files are managed correctly
- [x] Split boundaries prevent duplicate processing

## Known Issues / Limitations

### ⚠️ Partially Implemented
1. **Worker Failure Recovery**
   - ✅ Worker timeout detection (30 seconds)
   - ✅ Worker removal from registry
   - ❌ Tasks not automatically reassigned when worker fails
   - **Impact**: Tasks assigned to failed workers remain stuck in "IN_PROGRESS" state
   - **Fix Needed**: Update `_check_worker_timeouts()` to reassign tasks

### ❌ Not Implemented
1. **Job Cancellation**
   - CancelJob RPC not defined in proto
   - No cancellation logic in coordinator
   - **Status**: Mentioned in client but not functional

2. **Combiner Support**
   - No `combine_fn` loading or execution
   - **Status**: Planned for Week 3

3. **Detailed Resource Monitoring**
   - GetResourceUsage and ListActiveTasks RPCs not implemented
   - Basic status available via GetJobStatus
   - **Status**: Monitoring functions exist but use available RPCs only

## TODO - Remaining Work

### High Priority
- [ ] **Worker Failure Recovery**: Complete automatic task reassignment
  - Update `_check_worker_timeouts()` in `src/coordinator/server.py`
  - Find tasks assigned to failed worker
  - Reset task status to PENDING
  - Clear assigned_worker field
  - Re-queue tasks for reassignment

### Medium Priority
- [ ] **Job Cancellation**: Implement CancelJob RPC
  - Add `CancelJob` RPC to `coordinator.proto`
  - Implement cancellation logic in coordinator
  - Stop running tasks, mark job as CANCELLED

- [ ] **Combiner Support** (Week 3)
  - Load optional `combine_fn` from job files
  - Group map output by key within each partition
  - Apply combiner before writing intermediate files

### Low Priority
- [ ] **Documentation Updates**
  - Update ARCHITECTURE.md with task scheduling details
  - Update README.md with new features
  - Add execution flow diagrams

- [ ] **Performance Metrics RPCs**
  - Implement GetResourceUsage RPC
  - Implement ListActiveTasks RPC
  - Track detailed metrics per job