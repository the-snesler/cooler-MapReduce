# Week 2 Implementation Checklist

Based on the design.md Phase 2 requirements and Week 1 completion:

## Phase 2: Task Scheduling and Execution

### Coordinator Task Management ⬜️
- [ ] Implement task creation in SubmitJob
  - [ ] Split input into map tasks based on num_map_tasks
  - [ ] Create task records with unique IDs
  - [ ] Store task metadata (input splits, state, assigned worker)
- [ ] Implement task scheduling logic
  - [ ] Track available workers from heartbeats
  - [ ] Assign tasks to workers with available slots
  - [ ] Handle worker failures and task reassignment
  - [ ] Track task completion and job progress
- [ ] Add task queue and worker load balancing
  - [ ] Implement priority queue for pending tasks
  - [ ] Consider worker CPU usage in assignment
  - [ ] Handle stragglers (slow workers)

### Worker Task Execution ⬜️
- [ ] Implement map task execution
  - [ ] Load and validate user's map_fn
  - [ ] Process input splits
  - [ ] Write intermediate files with correct partitioning
  - [ ] Report progress and completion to coordinator
- [ ] Implement reduce task execution
  - [ ] Read intermediate files from map phase
  - [ ] Load and execute user's reduce_fn
  - [ ] Write final output files
  - [ ] Clean up intermediate files
- [ ] Add task status reporting
  - [ ] Periodic progress updates
  - [ ] Error handling and reporting
  - [ ] Resource usage monitoring

### Job Pipeline Management ⬜️
- [ ] Implement map phase completion detection
  - [ ] Track all map tasks for a job
  - [ ] Validate intermediate files
  - [ ] Trigger reduce phase start
- [ ] Implement reduce phase management
  - [ ] Create reduce tasks when maps complete
  - [ ] Track reduce task completion
  - [ ] Mark job as completed
- [ ] Add job status transitions
  - [ ] SUBMITTED → MAPPING → REDUCING → COMPLETED
  - [ ] Handle failures at each stage
  - [ ] Support job cancellation

### Client Enhancements ⬜️
- [ ] Add progress monitoring
  - [ ] Show map/reduce task progress
  - [ ] Display current phase
  - [ ] Show worker assignments
- [ ] Improve error reporting
  - [ ] Detailed task failure information
  - [ ] Worker failure notifications
  - [ ] Job failure analysis
- [ ] Add job control features
  - [ ] Cancel running job
  - [ ] List active tasks
  - [ ] Show resource usage

### Testing Enhancements ⬜️
- [ ] Unit tests
  - [ ] Task creation and scheduling
  - [ ] Worker assignment logic
  - [ ] Progress tracking
  - [ ] Error handling
- [ ] Integration tests
  - [ ] End-to-end job execution
  - [ ] Worker failure recovery
  - [ ] Multiple concurrent jobs
- [ ] Performance tests
  - [ ] Measure task completion times
  - [ ] Monitor resource usage
  - [ ] Test with varying task counts

### Documentation Updates ⬜️
- [ ] Update ARCHITECTURE.md
  - [ ] Task scheduling details
  - [ ] Execution flow diagrams
  - [ ] Failure handling
- [ ] Create WEEK2_SUMMARY.md
  - [ ] Implementation highlights
  - [ ] Testing results
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
- [ ] Map tasks execute and produce intermediate files
- [ ] Reduce tasks process intermediates correctly
- [ ] Jobs complete end-to-end successfully
- [ ] Worker failures are handled gracefully
- [ ] Progress reporting is accurate

### Performance
- [ ] Tasks are distributed evenly
- [ ] Worker CPU limits are respected
- [ ] Reasonable completion times
- [ ] No memory leaks

### Reliability
- [ ] All new tests pass
- [ ] System recovers from failures
- [ ] No deadlocks or race conditions
- [ ] Intermediate/output files are managed correctly