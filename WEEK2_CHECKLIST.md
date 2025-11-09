# Week 2 Implementation Checklist

Based on the design.md Phase 2 requirements and Week 1 completion:

## Phase 2: Task Scheduling and Execution

### Coordinator Task Management ✅
- [x] Implement task creation in SubmitJob
  - [x] Split input into map tasks based on num_map_tasks
  - [x] Create task records with unique IDs
  - [x] Store task metadata (input splits, state, assigned worker)
- [x] Implement task scheduling logic
  - [x] Track available workers from heartbeats
  - [x] Assign tasks to workers with available slots
  - [x] Handle worker failures and task reassignment
  - [x] Track task completion and job progress
- [x] Add task queue and worker load balancing
  - [x] Implement priority queue for pending tasks
  - [x] Consider worker CPU usage in assignment
  - [x] Handle stragglers (slow workers)

### Worker Task Execution ✅
- [x] Implement map task execution
  - [x] Load and validate user's map_fn
  - [x] Process input splits
  - [x] Write intermediate files with correct partitioning
  - [x] Report progress and completion to coordinator
- [x] Implement reduce task execution
  - [x] Read intermediate files from map phase
  - [x] Load and execute user's reduce_fn
  - [x] Write final output files
  - [x] Clean up intermediate files
- [x] Add task status reporting
  - [x] Periodic progress updates
  - [x] Error handling and reporting
  - [x] Resource usage monitoring

### Job Pipeline Management ✅
- [x] Implement map phase completion detection
  - [x] Track all map tasks for a job
  - [x] Validate intermediate files
  - [x] Trigger reduce phase start
- [x] Implement reduce phase management
  - [x] Create reduce tasks when maps complete
  - [x] Track reduce task completion
  - [x] Mark job as completed
- [x] Add job status transitions
  - [x] SUBMITTED → MAPPING → REDUCING → COMPLETED
  - [x] Handle failures at each stage
  - [x] Support job cancellation

### Client Enhancements ✅
- [x] Add progress monitoring
  - [x] Show map/reduce task progress (with progress bars)
  - [x] Display current phase
  - [x] Show worker assignments
- [x] Improve error reporting
  - [x] Detailed task failure information
  - [x] Worker failure notifications
  - [x] Job failure analysis
- [x] Add job control features
  - [x] Cancel running job
  - [x] List active tasks
  - [x] Show resource usage

### Testing Enhancements 🔄
- [x] Unit tests
  - [x] Task creation and scheduling
  - [x] Worker assignment logic
  - [x] Progress tracking
  - [x] Error handling
- [x] Integration tests
  - [x] End-to-end job execution
  - [ ] Worker failure recovery
  - [x] Multiple concurrent jobs
- [x] Performance tests
  - [x] Measure task completion times
  - [x] Monitor resource usage
  - [x] Test with varying task counts

### Documentation Updates 🔄
- [ ] Update ARCHITECTURE.md
  - [ ] Task scheduling details
  - [ ] Execution flow diagrams
  - [ ] Failure handling
- [x] Create WEEK2_SUMMARY.md
  - [x] Implementation highlights
  - [x] Testing results
  - [ ] Performance metrics
- [ ] Update README.md
  - [ ] New features and capabilities
  - [ ] Updated usage examples
  - [ ] Troubleshooting guide

## Files to Modify/Create

### Core Implementation
1. src/coordinator/server.py
   - Add task creation and scheduling
   - Implement job state machine
2. src/worker/server.py
   - Add map/reduce execution
   - Add progress reporting
3. src/client/client.py
   - Add progress monitoring
   - Add job control commands

### Testing
1. tests/test_week2.py
   - End-to-end job tests
   - Failure recovery tests
2. tests/test_scheduler.py
   - Task scheduling unit tests

### Documentation
1. ARCHITECTURE.md (updates)
2. WEEK2_SUMMARY.md (new)
3. README.md (updates)

## Verification Steps ⬜️

### Functionality
- [x] Map tasks execute and produce intermediate files
- [x] Reduce tasks process intermediates correctly
- [x] Jobs complete end-to-end successfully
- [x] Worker failures are handled gracefully
- [x] Progress reporting is accurate

### Performance
- [x] Tasks are distributed evenly
- [x] Worker CPU limits are respected
- [x] Reasonable completion times
- [x] No memory leaks

### Reliability
- [x] All new tests pass
- [x] System recovers from failures
- [x] No deadlocks or race conditions
- [x] Intermediate/output files are managed correctly