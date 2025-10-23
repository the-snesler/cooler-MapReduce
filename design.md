# MapReduce Implementation Design Document

## Overview
This document specifies the design for a simplified reimplementation of Google's MapReduce framework, optimized for deployment on a single VM with 4 CPUs using Docker containers.

## System Architecture

### Deployment
- **Platform**: Docker Compose on a single VM (4 CPUs)
- **Components**:
  - 1 Coordinator (Boss) container
  - 4 Worker containers (each limited to 1 CPU via `cpus: 1.0` in compose file)
  - Shared storage volume mounted to all containers
  - Client program (runs outside containers on host)

### Implementation Language
**Python 3.9+** will be used for the entire system:
- **Rationale**: 
  - Excellent for rapid development and prototyping
  - Rich ecosystem for networking (gRPC), serialization (pickle, JSON), and data processing
  - Easy dynamic code loading for user-provided map/reduce functions
  - Strong support for concurrent programming (threading, asyncio)

## Client Interface

### Data Upload Commands
```bash
# Copy input data to shared storage
cp wordcount_input.txt /shared/input/

# Or use the provided helper script
./scripts/upload_data.sh <local_path> <shared_path>
```

### Job Submission Command
```bash
# Submit a MapReduce job
python client.py submit \
  --input /shared/input/wordcount_input.txt \
  --output /shared/output/wordcount_result \
  --job-file jobs/wordcount.py \
  --num-map 8 \
  --num-reduce 4 \
  --coordinator-host localhost:50051
```

**Parameters**:
- `--input`: Path to input file(s) in shared storage (glob patterns supported)
- `--output`: Directory path for output files in shared storage
- `--job-file`: User-provided Python file containing `map_fn`, `reduce_fn`, and optionally `combine_fn`
- `--num-map`: Number of map tasks to create
- `--num-reduce`: Number of reduce tasks to create
- `--coordinator-host`: Coordinator service address (default: localhost:50051)

### Job Status Commands
```bash
# Check job status
python client.py status --job-id <job_id>

# List all jobs
python client.py list

# Get job results
python client.py results --job-id <job_id>
```

### User-Provided Job File Format
Users provide a Python file with the following functions:

```python
def map_fn(key, value):
    """
    Map function.
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
    Reduce function.
    Args:
        key: Intermediate key
        values: Iterator of values for this key
    Yields:
        (key, final_value) tuples
    """
    # Example: sum counts
    yield (key, sum(values))

def combine_fn(key, values):
    """
    Optional combiner function (same signature as reduce_fn).
    If not provided, no combining is performed.
    Args:
        key: Intermediate key
        values: Iterator of values for this key
    Yields:
        (key, combined_value) tuples
    """
    # Example: local aggregation of counts
    yield (key, sum(values))
```

## Communication Mechanisms

### Technology: gRPC
All inter-component communication will use **gRPC** with Protocol Buffers:
- **Rationale**:
  - Efficient binary protocol with strong typing
  - Built-in support for streaming (useful for task distribution)
  - Excellent Python support
  - Easy service definition via `.proto` files

### Communication Paths

#### 1. Client ↔ Coordinator
```protobuf
service CoordinatorService {
  rpc SubmitJob(JobRequest) returns (JobResponse);
  rpc GetJobStatus(JobStatusRequest) returns (JobStatusResponse);
  rpc ListJobs(Empty) returns (JobListResponse);
  rpc GetJobResults(JobResultsRequest) returns (JobResultsResponse);
}
```

#### 2. Coordinator ↔ Workers
```protobuf
service WorkerService {
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
  rpc AssignTask(TaskAssignment) returns (TaskAck);
  rpc GetTaskStatus(TaskStatusRequest) returns (TaskStatusResponse);
}
```

Workers periodically send heartbeats to the coordinator. The coordinator pushes task assignments to workers and polls for task completion.

## Shared Storage

### Solution: Docker Volume Mount
All containers will mount the same host directory:
```yaml
volumes:
  - ./shared:/shared
```

**Rationale**:
- Simple and efficient for single-VM deployment
- No additional infrastructure needed (vs. HDFS)
- Direct filesystem access with excellent performance
- Easy to debug and inspect intermediate files

### Directory Structure
```
/shared/
├── input/          # Input data files
├── output/         # Final output files
├── intermediate/   # Intermediate map outputs
│   ├── job_<id>/
│   │   ├── map_<task_id>_<partition>.pkl
└── jobs/           # User job files uploaded by client
    └── job_<id>.py
```

## Data Formats

### Input Data Format
- **Format**: Plain text files (newline-delimited)
- **Key**: Filename or line number
- **Value**: Line content
- **Rationale**: Simple, human-readable, flexible for various applications

### Intermediate Data Format
- **Format**: Python pickle files (`.pkl`)
- **Structure**: List of `(key, value)` tuples, partitioned by hash(key) % num_reduce
- **Rationale**:
  - Native Python serialization (handles arbitrary Python objects)
  - Efficient for local processing
  - Easy to implement
- **Alternative considered**: JSON (human-readable but slower, limited types)

### Output Data Format
- **Format**: Plain text files, one per reduce task
- **Structure**: `key\tvalue\n` (tab-separated)
- **Filenames**: `part-r-00000`, `part-r-00001`, etc.
- **Rationale**: Simple, readable, compatible with Unix tools (grep, sort, etc.)

### Network Protocol Format
- **Format**: Protocol Buffers (for gRPC messages)
- **Rationale**: Efficient, strongly-typed, version-compatible

## Combiner Implementation

### Design
Combiners are **optional local reduce operations** executed immediately after map tasks, before data is written to intermediate storage.

### Workflow
1. Map task completes, produces key-value pairs in memory
2. **If `combine_fn` is provided**:
   - Group pairs by key locally (in memory)
   - Apply `combine_fn` to each group
   - Write combined results to intermediate storage
3. **If no `combine_fn`**:
   - Write map outputs directly to intermediate storage

### Benefits
- **Reduced I/O**: Less data written to disk
- **Reduced Network**: Less data shuffled to reducers
- **Faster execution**: Especially for associative/commutative operations (sum, count, max)

### Implementation Details
```python
class MapTask:
    def execute(self, map_fn, combine_fn=None):
        # Run map function
        map_outputs = []
        for key, value in self.input_data:
            map_outputs.extend(map_fn(key, value))
        
        # Partition by reduce task
        partitions = self.partition(map_outputs, num_reduce_tasks)
        
        # Apply combiner if provided
        for partition_id, pairs in partitions.items():
            if combine_fn:
                # Group by key and combine
                grouped = defaultdict(list)
                for k, v in pairs:
                    grouped[k].append(v)
                combined = []
                for k, values in grouped.items():
                    combined.extend(combine_fn(k, values))
                pairs = combined
            
            # Write to intermediate storage
            self.write_intermediate(partition_id, pairs)
```

### Correctness Requirements
The combiner must produce the same result as the reducer would (i.e., must be associative and commutative). The user is responsible for ensuring this property.

## Test Strategy

### 1. Unit Tests
**File**: `tests/test_units.py`
- Test map function execution
- Test reduce function execution
- Test combiner function execution
- Test data partitioning logic
- Test serialization/deserialization

### 2. Integration Tests
**File**: `tests/test_integration.py`
- Test end-to-end job execution (without combiner)
- Test end-to-end job execution (with combiner)
- Test failure recovery (worker failure, task retry)
- Test multiple concurrent jobs

### 3. Example Applications
**Directory**: `examples/`

#### WordCount
- **Input**: Large text file (e.g., Shakespeare's complete works)
- **Expected output**: Word frequencies
- **With combiner**: Aggregates counts locally before shuffle

#### Grep
- **Input**: Large log files
- **Expected output**: Lines matching pattern
- **No combiner**: Not applicable (no reduction)

#### Inverted Index
- **Input**: Multiple text documents
- **Expected output**: Word → [list of documents] mapping
- **With combiner**: Merges document lists locally

### 4. Correctness Tests
**File**: `tests/test_correctness.py`
- Compare output with known-good results
- Verify combiner produces same results as without combiner
- Test various input sizes (small, medium, large)
- Test edge cases (empty input, single line, no matches)

## Performance Evaluation

### Objectives
1. Demonstrate that using 4 workers improves performance vs. 1 worker
2. Demonstrate that combiners reduce execution time and I/O

### Metrics to Collect
- **Execution Time**: Total job duration (submit to completion)
- **Phase Times**: Map phase duration, shuffle phase duration, reduce phase duration
- **Data Volume**: 
  - Input data size
  - Intermediate data size (with/without combiner)
  - Output data size
- **I/O Operations**:
  - Number of intermediate files written
  - Bytes written to intermediate storage
  - Bytes read from intermediate storage
- **Task Statistics**:
  - Number of tasks executed
  - Average task duration
  - Task failures/retries

### Experiments

#### Experiment 1: Scalability with Multiple Workers
**Goal**: Show performance improves with 4 workers vs. 1 worker

**Setup**:
- Job: WordCount on large text corpus (100 MB)
- Test configurations: 1, 2, 4 workers
- Map tasks: 16, Reduce tasks: 4

**Measurements**:
- Total execution time for each configuration
- CPU utilization per worker

**Expected Result**: Near-linear speedup (4x workers → ~4x faster)

**Plot**: Bar chart of execution time vs. number of workers

#### Experiment 2: Combiner Effectiveness
**Goal**: Demonstrate combiner reduces I/O and improves performance

**Setup**:
- Job: WordCount on large text corpus (100 MB)
- Two runs: with combiner, without combiner
- Workers: 4, Map tasks: 16, Reduce tasks: 4

**Measurements**:
- Total execution time
- Intermediate data size
- Map phase time
- Shuffle/reduce phase time

**Expected Result**: 
- Combiner reduces intermediate data by 10-100x (for word count)
- Combiner reduces total time by 20-50%

**Plots**:
1. **Bar chart**: Execution time comparison (with vs. without combiner)
2. **Bar chart**: Intermediate data size comparison
3. **Stacked bar chart**: Phase breakdown (map, shuffle, reduce)

#### Experiment 3: Combiner Impact on Different Workloads
**Goal**: Show combiner benefits vary by application

**Setup**:
- Three jobs: WordCount (high benefit), Grep (no benefit), InvertedIndex (medium benefit)
- All with and without combiner
- Workers: 4, Map tasks: 16

**Measurements**:
- Speedup factor (time without / time with combiner)
- Data reduction factor (size without / size with combiner)

**Expected Results**:
- WordCount: 2-3x speedup, 50-100x data reduction
- Grep: ~1x speedup (no combiner benefit)
- InvertedIndex: 1.5-2x speedup, 5-10x data reduction

**Plot**: Grouped bar chart showing speedup and data reduction for each workload

### Performance Testing Tools

#### Instrumentation
```python
class MetricsCollector:
    def record_task_start(self, task_id, task_type):
        ...
    
    def record_task_complete(self, task_id, duration):
        ...
    
    def record_data_written(self, bytes_written):
        ...
    
    def record_data_read(self, bytes_read):
        ...
    
    def export_metrics(self) -> dict:
        ...
```

#### Benchmarking Script
```bash
# Run performance tests
python benchmark.py \
  --workload wordcount \
  --input-size 100M \
  --workers 1,2,4 \
  --with-combiner \
  --without-combiner \
  --output results/wordcount_benchmark.json

# Generate plots
python plot_results.py results/wordcount_benchmark.json
```

### Expected Deliverables
1. **Performance report** (`PERFORMANCE.md`):
   - Summary of all experiments
   - Plots and charts
   - Analysis and conclusions
   
2. **Raw metrics** (`results/*.json`):
   - Structured data for all test runs
   - Reproducible benchmark configurations

3. **Visualization scripts** (`scripts/plot_results.py`):
   - Generate all plots from raw metrics
   - Export as PNG/PDF

## Implementation Phases

### Phase 1: Core Infrastructure (Week 1)
- Docker Compose setup
- gRPC service definitions
- Basic coordinator and worker scaffolding
- Shared storage setup

### Phase 2: MapReduce Core (Week 2)
- Task scheduling and distribution
- Map task execution
- Reduce task execution
- Data partitioning and shuffling

### Phase 3: Combiner Support (Week 3)
- Combiner integration in map tasks
- Testing and validation

### Phase 4: Testing & Evaluation (Week 4)
- Example applications
- Performance benchmarking
- Documentation and report writing

## Risk Mitigation

### Potential Issues
1. **Worker failures**: Implement task retry and reassignment
2. **Data skew**: May cause some reducers to be bottlenecks
3. **Memory constraints**: Large intermediate data may not fit in memory
4. **Serialization overhead**: Pickle may be slow for large objects

### Solutions
1. Coordinator tracks worker health via heartbeats; reassigns tasks from failed workers
2. Document limitation; potential optimization: use sampling to improve partitioning
3. Use streaming/chunked reading for large intermediate files
4. Profile and optimize hot paths; consider alternative serialization if needed

## Conclusion
This design provides a simplified but functional MapReduce implementation that demonstrates key concepts from the original paper, including distributed processing, data partitioning, and combiner optimization. The single-VM Docker deployment makes it easy to develop and test while still exhibiting parallelism and performance benefits.
