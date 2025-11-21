# MapReduce Implementation Design Document

## 1. Client Interface Specification

### Command-Line Interface

The client provides four main commands for interacting with the MapReduce system:

#### upload-data

Uploads local files to shared storage accessible by all containers.

**Usage:**
```bash
python3 client/client.py upload-data <source> <destination>
```

**Parameters:**
- `source`: Local file path
- `destination`: Path in shared storage (e.g., `/mapreduce-data/inputs/file.txt`)

**Example:**
```bash
python3 client/client.py upload-data data.txt /mapreduce-data/inputs/data.txt
```

#### submit-job

Submits a MapReduce job to the coordinator.

**Usage:**
```bash
python3 client/client.py submit-job --input <path> --output <path> --job-file <file> \
  --num-map-tasks <M> --num-reduce-tasks <R> [--use-combiner] [--job-id <id>]
```

**Parameters:**
- `--input`: Input data path in shared storage
- `--output`: Output directory path in shared storage
- `--job-file`: Python file containing map/reduce functions
- `--num-map-tasks`: Number of map tasks (M)
- `--num-reduce-tasks`: Number of reduce tasks (R)
- `--use-combiner`: Enable combiner optimization (optional)
- `--job-id`: Custom job identifier (auto-generated if omitted)

**Returns:**
- Job ID for tracking
- Initial status

**Example:**
```bash
python3 client/client.py submit-job \
  --input /mapreduce-data/inputs/shakespeare.txt \
  --output /mapreduce-data/outputs/wordcount-results \
  --job-file examples/wordcount.py \
  --num-map-tasks 8 \
  --num-reduce-tasks 4 \
  --use-combiner
```

#### job-status

Checks the current status of a submitted job.

**Usage:**
```bash
python3 client/client.py job-status <job-id>
```

**Returns:**
- Current status (pending/map_phase/reduce_phase/completed/failed)
- Progress information
- Number of completed tasks

**Example:**
```bash
python3 client/client.py job-status wordcount-20250120-1234
```

#### get-results

Retrieves results and metrics for a completed job.

**Usage:**
```bash
python3 client/client.py get-results <job-id> [--download <local-path>]
```

**Returns:**
- Output file locations
- Performance metrics
- Job execution summary

**Example:**
```bash
python3 client/client.py get-results wordcount-20250120-1234
```

### User Function Interface

Users provide a Python file with three functions defining the MapReduce computation:

```python
def map_function(key, value):
    """
    Process input record and emit key-value pairs.

    Args:
        key: Line number (int) from input file
        value: Line content (str)

    Yields:
        (output_key, output_value): Key-value pairs to be shuffled
    """
    yield (output_key, output_value)

def reduce_function(key, values):
    """
    Aggregate values for a key.

    Args:
        key: The key to process
        values: List of all values associated with this key

    Yields:
        (output_key, output_value): Final aggregated result
    """
    yield (output_key, output_value)

def combiner_function(key, values):  # Optional
    """
    Pre-aggregate values locally on the map side.

    Args:
        key: The key to process
        values: List of values from local map task

    Yields:
        (output_key, output_value): Partially aggregated result
    """
    yield (output_key, output_value)
```

**Function Requirements:**
- All functions must be generators (use `yield`)
- Map function receives `(int, str)` pairs
- Reduce/combiner functions receive `(any, list)` pairs
- Keys must be hashable (str, int, tuple, etc.)
- Values can be any JSON-serializable type

## 2. Language and Technology Choices

### Implementation Language: Python 3.9+

**Rationale:**

1. **Dynamic function loading**: Python's `importlib` and `SourceFileLoader` make loading user-provided functions straightforward without complex plugin systems
2. **Rapid development**: Clear syntax and extensive standard library accelerate development and debugging
3. **Educational value**: Code is easy to read and understand for learning distributed systems concepts
4. **Adequate performance**: For educational project scale (<1GB datasets), Python performance is sufficient

**Trade-offs:**

- **Performance**: Slower than Go or C++, but acceptable for demonstration purposes
  - Mitigated by: Parallel execution across multiple worker processes
- **Type safety**: Dynamic typing requires more runtime checks and testing
  - Mitigated by: Type hints and comprehensive test suite (Task 9)
- **GIL limitations**: Global Interpreter Lock prevents true parallelism within single process
  - Mitigated by: Using separate container processes for each worker

**Alternative considered: Go 1.19+**
- Would provide better performance (10-100x faster)
- Superior concurrency primitives (goroutines, channels)
- Compiled binaries easier to distribute
- **Rejected due to:**
  - Complexity of plugin system for loading user code
  - Steeper learning curve for educational context
  - Dynamic code loading requires Go plugins or RPC, adding complexity

### Communication Protocol: gRPC with Protocol Buffers

**Rationale:**

1. **Type safety**: Protobuf schemas enforce strict API contracts at compile time
2. **Efficiency**: Binary serialization reduces network overhead vs. JSON (40-50% smaller)
3. **Streaming support**: Bi-directional streaming available for future extensions (progress updates, task cancellation)
4. **Cross-language**: Could integrate Go workers or other languages later
5. **Error handling**: Rich status codes and metadata for debugging

**Trade-offs:**

- **Complexity**: More setup than HTTP REST (protobuf compilation, code generation)
  - Worth it: Benefits outweigh setup cost for distributed system
- **Debugging**: Binary format harder to inspect than JSON
  - Mitigated by: gRPC reflection and debug logging
- **Learning curve**: Requires understanding protobuf syntax and gRPC concepts
  - Acceptable: Good learning experience for production systems

**Alternative considered: HTTP REST with JSON**
- Simpler to implement (Flask/FastAPI)
- Easier to debug (curl, browser)
- More familiar to beginners
- **Rejected due to:**
  - Less efficient serialization
  - No native streaming support
  - Less type-safe (schema validation requires extra libraries)
  - Not industry standard for distributed systems

### Containerization: Docker & Docker Compose

**Rationale:**

1. **Isolation**: Each container has independent Python environment and dependencies
2. **Resource limits**: CPU constraints (1 core per worker) easily enforced via `cpus` setting
3. **Reproducibility**: Consistent environment across different machines and operating systems
4. **Single-VM deployment**: Docker Compose perfect for coordinating 5 containers on one host
5. **Networking**: Built-in DNS resolution for service discovery

**Configuration:**
```yaml
worker-1:
  cpus: 1.0          # Limit to 1 CPU core
  mem_limit: 1g      # Memory limit
  volumes:
    - mapreduce-data:/mapreduce-data  # Shared filesystem
  networks:
    - mapreduce-net  # Isolated network
```

**Trade-offs:**
- **Overhead**: Container overhead (~100MB per container) acceptable for 5 containers
- **Complexity**: Requires Docker knowledge, but minimal for this setup
- **Single-host limitation**: Can't scale across multiple physical machines
  - Acceptable: Project scope limited to single VM demonstration

## 3. Communication Protocol Details

### Client ↔ Coordinator: gRPC (JobService)

**Service definition** (from `mapreduce.proto`):

```protobuf
service JobService {
  rpc SubmitJob(JobSpec) returns (JobResponse);
  rpc GetJobStatus(JobStatusRequest) returns (JobStatusResponse);
  rpc GetJobResult(JobResultRequest) returns (JobResultResponse);
}
```

**Message structures:**

```protobuf
message JobSpec {
  string job_id = 1;
  string input_path = 2;
  string output_path = 3;
  string map_reduce_file = 4;
  int32 num_map_tasks = 5;
  int32 num_reduce_tasks = 6;
  bool use_combiner = 7;
}

message JobResponse {
  string job_id = 1;
  string status = 2;
  string message = 3;
}

message JobStatusResponse {
  string job_id = 1;
  string status = 2;  // "pending", "map_phase", "reduce_phase", "completed", "failed"
  int32 total_tasks = 3;
  int32 completed_tasks = 4;
  float progress = 5;
}

message JobResultResponse {
  string job_id = 1;
  string output_path = 2;
  JobMetrics metrics = 3;
}
```

**Error handling:**

- `NOT_FOUND`: Job ID doesn't exist in coordinator state
- `INVALID_ARGUMENT`: Invalid parameters (negative task counts, missing files, invalid paths)
- `FAILED_PRECONDITION`: Job not yet completed (for get-results)
- `INTERNAL`: Server-side errors during execution (worker crashes, I/O failures)

**Communication flow:**

1. Client calls `SubmitJob()` with job specification
2. Coordinator validates job, generates tasks, returns job ID
3. Client polls `GetJobStatus()` periodically to monitor progress
4. When status = "completed", client calls `GetJobResult()` to retrieve metrics

### Coordinator ↔ Worker: gRPC (TaskService)

**Service definition:**

```protobuf
service TaskService {
  rpc AssignMapTask(MapTaskRequest) returns (TaskResult);
  rpc AssignReduceTask(ReduceTaskRequest) returns (TaskResult);
}
```

**Message structures:**

```protobuf
message MapTaskRequest {
  int32 task_id = 1;
  string job_id = 2;
  string input_path = 3;
  int64 start_offset = 4;
  int64 end_offset = 5;
  int32 num_reduce_tasks = 6;
  string map_reduce_file = 7;
  bool use_combiner = 8;
}

message ReduceTaskRequest {
  int32 task_id = 1;
  string job_id = 2;
  string intermediate_dir = 3;
  string output_path = 4;
  string map_reduce_file = 5;
  int32 num_map_tasks = 6;
}

message TaskResult {
  bool success = 1;
  int32 execution_time_ms = 2;
  string error_message = 3;
}
```

**Task assignment flow:**

1. **Coordinator generates tasks**: Based on input file size and `num_map_tasks`, calculates byte offsets for each map task
2. **Coordinator selects worker**: Round-robin selection from available workers (`worker-1`, `worker-2`, `worker-3`, `worker-4`)
3. **Coordinator calls gRPC**: `AssignMapTask()` or `AssignReduceTask()` via gRPC stub
4. **Worker executes task synchronously**:
   - Map: Read input split, apply map function, partition by key hash, write intermediate files
   - Reduce: Read intermediate partitions, group by key, apply reduce function, write output
5. **Worker returns `TaskResult`**: Success status, execution time, error message if failed
6. **Coordinator marks task completed**: Updates job state, starts next task or phase

**Concurrency model:**

- Coordinator uses **thread pool** (`concurrent.futures.ThreadPoolExecutor`) to assign multiple tasks in parallel
- Workers process **one task at a time** (simplified model, no internal concurrency)
- Maximum parallelism: 4 concurrent tasks (one per worker)

**Error handling and fault tolerance:**

- **Task retry**: Coordinator retries failed tasks up to 3 times (configurable)
- **Worker failure detection**: gRPC timeout (60 seconds) detects unresponsive workers
- **Job failure**: Job marked as failed if any task fails 3 times
- **No worker recovery**: Workers are stateless, coordinator can reassign failed task to any worker

**Why synchronous task execution:**

- Simplicity: No need for task queues, asynchronous callbacks, or complex state management
- Sufficient: 4 workers provide adequate parallelism for demonstration
- Clear semantics: Task assignment and completion are atomic operations

## 4. Shared Storage Architecture

### Docker Volume Mount

**Implementation:**

The system uses a single Docker volume `mapreduce-data` mounted at `/mapreduce-data` in all containers (coordinator, all workers, and accessible to client via volume mount operations).

**Directory structure:**

```
/mapreduce-data/
├── inputs/              # Input datasets uploaded by users
│   ├── shakespeare.txt
│   └── dataset.txt
├── intermediate/        # Map output files, partitioned by reduce task
│   └── <job-id>/
│       ├── map-0-reduce-0.txt
│       ├── map-0-reduce-1.txt
│       ├── map-1-reduce-0.txt
│       └── ...
├── outputs/             # Final reduce output
│   └── <job-id>/
│       ├── part-0.txt   # Output from reduce task 0
│       ├── part-1.txt   # Output from reduce task 1
│       └── ...
├── jobs/               # User-provided map/reduce Python files
│   ├── wordcount.py
│   └── inverted_index.py
└── metrics/            # Performance metrics (JSON format)
    └── <job-id>.json
```

**Why shared filesystem:**

1. **Simplicity**: No network filesystem (NFS, HDFS, GlusterFS) required
2. **Single-VM deployment**: All containers on same Docker host can share volume efficiently
3. **Performance**: Local disk I/O much faster than network transfers (10-100x speedup)
4. **Atomic operations**: POSIX filesystem provides atomicity guarantees for file operations
5. **Easy debugging**: Can inspect files directly from host machine

**Limitations:**

- **Not distributed**: Won't work across multiple physical machines
- **Single point of failure**: If volume corrupts, all data lost
- **Acceptable for project**: Scope limited to single VM demonstration, not production system

**File access patterns:**

- **Input files**: Read-only, multiple workers may read same file concurrently
- **Intermediate files**: Write-once by map tasks, read-only by reduce tasks
  - Naming guarantees no conflicts: `map-{task_id}-reduce-{partition}.txt`
- **Output files**: Write-once by reduce tasks, one file per reduce partition
  - No coordination needed, each reduce task writes to unique file

## 5. Data Format Specifications

### Input Data Format

**Format**: Plain text, line-delimited (newline-separated)

- Each line is one input record
- Map function receives `(line_number, line_content)` as `(key, value)`
- Line numbers start at 0
- Empty lines are included (not filtered)

**Example input file:**

```
the quick brown fox
jumps over the lazy dog
the end
```

**Parsed as:**

```python
(0, "the quick brown fox")
(1, "jumps over the lazy dog")
(2, "the end")
```

### Intermediate Data Format

**Format**: JSON, one record per line (JSON Lines format)

**Structure:**

```json
{"key": "word", "value": 1}
{"key": "another", "value": 2}
```

**Partitioning strategy:**

```python
partition_id = hash(str(key)) % num_reduce_tasks
```

- Uses Python's built-in `hash()` function for deterministic hashing
- Ensures same key always goes to same reduce task
- Approximately uniform distribution across partitions

**File naming convention:**

```
/mapreduce-data/intermediate/<job-id>/map-<task-id>-reduce-<partition-id>.txt
```

**Example:**

- Job: `wordcount-20250120`
- Map task 0 produces 4 files (for R=4):
  - `map-0-reduce-0.txt` (keys hashing to partition 0)
  - `map-0-reduce-1.txt` (keys hashing to partition 1)
  - `map-0-reduce-2.txt` (keys hashing to partition 2)
  - `map-0-reduce-3.txt` (keys hashing to partition 3)

**Why JSON:**

- **Human-readable**: Easy to debug by inspecting intermediate files
- **Easy to parse**: Python's `json` module is fast and built-in
- **Type preservation**: Can serialize numbers, strings, lists, dicts
- **Acceptable overhead**: ~20% size overhead vs. binary format, negligible for <1GB datasets

### Output Data Format

**Format**: Tab-separated key-value pairs (TSV)

**Structure:**

```
apple\t5
banana\t3
cherry\t12
```

**Properties:**

1. **Sorted by key**: Output is sorted lexicographically within each partition
2. **One line per key**: Each unique key appears exactly once
3. **Tab-separated**: Key and value separated by `\t` character
4. **String representation**: Values converted to strings using `str()`

**File naming convention:**

```
/mapreduce-data/outputs/<job-id>/part-<partition-id>.txt
```

**Example:**

- Job: `wordcount-20250120`, R=4 reduce tasks
- Output files:
  - `part-0.txt` (keys from partition 0, sorted)
  - `part-1.txt` (keys from partition 1, sorted)
  - `part-2.txt` (keys from partition 2, sorted)
  - `part-3.txt` (keys from partition 3, sorted)

**Reading complete output:**

To get full sorted output, user must:

1. Read all `part-*.txt` files
2. Merge-sort them (since each part is already sorted)

Or simply concatenate if global sorting not required.

**Why TSV:**

- **Standard format**: Widely supported by tools (Excel, pandas, awk)
- **Simple parsing**: Split on `\t` character
- **Compact**: Smaller than JSON, more readable than binary

## 6. Test Strategy

### Unit Tests

**Goal**: Test individual components in isolation without external dependencies

**Framework**: pytest 7.4+

**Coverage target**: >80% line coverage

**Test categories:**

1. **Map Executor** (`tests/unit/test_map_executor.py`):
   - Input split reading with byte offsets
   - Partial file splits (handling split boundaries)
   - Empty input handling
   - Hash-based partitioning correctness
   - Combiner data reduction effectiveness
   - Combiner value aggregation correctness

2. **Reduce Executor** (`tests/unit/test_reduce_executor.py`):
   - Reading intermediate files from multiple map tasks
   - Key grouping (all values for same key grouped together)
   - Output sorting by key
   - JSON parsing and error handling
   - Output format (TSV) correctness

3. **Function Loader** (`tests/unit/test_function_loader.py`):
   - Dynamic module loading from file path
   - Map/reduce/combiner function extraction
   - Error handling for missing functions
   - Error handling for invalid Python files
   - Module caching behavior

**Mocking strategy:**

- Mock file I/O for tests that don't need real files
- Use `pytest` fixtures for temporary directories and sample data
- Mock gRPC stubs for coordinator/worker communication tests

**Running unit tests:**

```bash
pytest tests/unit/ -v --cov=worker --cov=coordinator --cov-report=term-missing
```

### Integration Tests

**Goal**: Test complete system with real Docker containers and gRPC communication

**Framework**: pytest with `@pytest.mark.integration` marker

**Requirements**: Docker and Docker Compose must be running

**Test categories:**

1. **End-to-End Job Execution** (`tests/integration/test_end_to_end.py`):
   - Submit job via client
   - Monitor job progress
   - Verify job completes successfully
   - Validate output files exist

2. **Correctness Validation** (`tests/integration/test_correctness.py`):
   - Word count produces correct counts for known input
   - Inverted index produces correct word-to-document mappings
   - Output sorting is correct

3. **Performance Tests** (optional):
   - Combiner effectiveness (with vs. without)
   - Scalability with varying M and R
   - Concurrent job execution

**Running integration tests:**

```bash
docker-compose up -d
pytest tests/integration/ -v -m integration
```

### Correctness Tests

**Strategy**: Compare output against known ground truth

**Example test case (word count):**

```python
def test_wordcount_produces_correct_counts():
    input_text = """the quick brown fox
the lazy dog
the fox"""

    expected_counts = {
        'the': 3,
        'quick': 1,
        'brown': 1,
        'fox': 2,
        'lazy': 1,
        'dog': 1
    }

    # Submit job, wait for completion, parse output
    actual_counts = parse_output(job_id)

    assert actual_counts == expected_counts
```

**Validation approaches:**

1. **Small input with known output**: Test with hand-calculated results
2. **Output properties**: Verify sorting, no duplicate keys, correct format
3. **Idempotency**: Running same job twice produces identical output
4. **Comparison with reference**: Compare with Python's `collections.Counter` for word count

## 7. Performance Evaluation Methodology

### Metrics Collected

The system tracks the following metrics for each job (implemented in `coordinator/metrics.py`):

1. **Timing metrics**:
   - `job_start_time`: When job submitted
   - `job_end_time`: When job completed
   - `map_phase_start`: When first map task started
   - `map_phase_end`: When last map task completed
   - `reduce_phase_start`: When first reduce task started
   - `reduce_phase_end`: When last reduce task completed

2. **Data size metrics**:
   - `input_size_bytes`: Total size of input file
   - `intermediate_size_bytes`: Total size of all intermediate files
   - `output_size_bytes`: Total size of all output files

3. **Configuration metrics**:
   - `num_map_tasks`: Number of map tasks (M)
   - `num_reduce_tasks`: Number of reduce tasks (R)
   - `use_combiner`: Whether combiner was enabled

4. **Derived metrics**:
   - `total_execution_time`: `job_end_time - job_start_time`
   - `map_phase_duration`: `map_phase_end - map_phase_start`
   - `reduce_phase_duration`: `reduce_phase_end - reduce_phase_start`
   - `combiner_reduction_ratio`: `1 - (intermediate_size_bytes / map_output_size_before_combiner)`

### Experiments

#### Experiment 1: Combiner Effectiveness

**Objective**: Measure impact of combiner optimization on execution time and data transfer

**Procedure**:

1. Run word count job **WITHOUT combiner**:
   ```bash
   python3 client/client.py submit-job \
     --input /mapreduce-data/inputs/shakespeare.txt \
     --output /mapreduce-data/outputs/wordcount-no-combiner \
     --job-file examples/wordcount.py \
     --num-map-tasks 8 \
     --num-reduce-tasks 4
   ```

2. Run identical job **WITH combiner**:
   ```bash
   python3 client/client.py submit-job \
     --input /mapreduce-data/inputs/shakespeare.txt \
     --output /mapreduce-data/outputs/wordcount-with-combiner \
     --job-file examples/wordcount.py \
     --num-map-tasks 8 \
     --num-reduce-tasks 4 \
     --use-combiner
   ```

3. Compare metrics using `tools/compare_performance.py`

**Dataset**: Shakespeare complete works (~5MB, 100K+ words)

**Configuration**: M=8, R=4 (fixed)

**Hypothesis**:
- Combiner should reduce intermediate data by >50%
- Execution time should improve by >20%
- Reduction ratio depends on data skew (word frequency distribution)

**Metrics to compare**:
- Total execution time
- Map phase duration
- Reduce phase duration
- Intermediate data size
- Combiner reduction ratio

#### Experiment 2: Scalability Analysis

**Objective**: Measure how performance scales with number of map tasks

**Procedure**:

1. Run word count with varying M: `M ∈ {2, 4, 8, 16, 32}`
2. Keep R=4 fixed, combiner enabled
3. Plot execution time vs. M

**Expected results**:
- Execution time decreases as M increases (more parallelism)
- Diminishing returns after M > 4 (limited by 4 workers)
- Overhead increases for very large M (task scheduling cost)

**Optimal configuration**: M=8 or M=16 for 4 workers

### Visualization

**Tools**: `matplotlib`, `numpy`

**Generated charts** (`tools/visualize_performance.py`):

1. **Performance Comparison Bar Chart**:
   - X-axis: Combiner disabled vs. enabled
   - Y-axis: Execution time (seconds)
   - Side-by-side bars for easy comparison

2. **Phase Breakdown Pie Chart**:
   - Slices: Map phase time, reduce phase time, overhead
   - Helps identify bottlenecks

3. **Data Size Comparison**:
   - X-axis: Input, intermediate (no combiner), intermediate (with combiner), output
   - Y-axis: Size in MB
   - Shows combiner data reduction visually

4. **Scalability Line Plot**:
   - X-axis: Number of map tasks (M)
   - Y-axis: Execution time (seconds)
   - Shows performance scaling

**Example usage:**

```bash
python3 tools/compare_performance.py wordcount examples/wordcount.py /mapreduce-data/inputs/shakespeare.txt
python3 tools/visualize_performance.py
```

### Performance Report

Results documented in `performance_report.md` with:
- Executive summary of findings
- Experimental setup details
- Results tables and charts
- Analysis and interpretation
- Conclusions and recommendations
