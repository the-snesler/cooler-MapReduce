# MapReduce Examples

This directory contains example MapReduce jobs demonstrating how to use the coolest-mapreduce system.

## Prerequisites

- Docker and docker-compose installed
- Python 3.7+
- `uuidgen` command (usually pre-installed on Linux/macOS)

## Quick Start

1. **Set up example datasets:**
   ```bash
   ./examples/setup_examples.sh
   ```

2. **Run the word count example:**
   ```bash
   ./examples/run_wordcount.sh
   ```

3. **Run the inverted index example:**
   ```bash
   ./examples/run_inverted_index.sh
   ```

## Available Examples

### 1. Word Count

Classic MapReduce example that counts word frequencies in text.

**File:** `examples/wordcount.py`

**What it does:**
- **Map:** Tokenizes each line, removes punctuation, converts to lowercase, emits `(word, 1)` for each word
- **Combiner:** Pre-aggregates counts locally to reduce network traffic
- **Reduce:** Sums all counts for each word

**Dataset:** Shakespeare's complete works (~5MB, ~150K lines)

**Run:**
```bash
./examples/run_wordcount.sh
```

**Expected output:** Tab-separated word and count pairs, sorted by frequency. Top words include "the", "and", "of", "to", etc.

### 2. Inverted Index

Creates a search index mapping each word to the documents/lines it appears in.

**File:** `examples/inverted_index.py`

**What it does:**
- **Map:** Emits `(word, document_id)` for each word, using line number as document ID
- **Combiner:** Removes duplicate document IDs locally
- **Reduce:** Collects all document IDs for each word, sorts and deduplicates

**Dataset:** Synthetic data (10,000 lines with random words)

**Run:**
```bash
./examples/run_inverted_index.sh
```

**Expected output:** Tab-separated word and comma-separated document ID list (e.g., `hello    doc_1,doc_5,doc_42`)

## Writing Custom MapReduce Jobs

### Basic Structure

Create a Python file with three functions:

```python
def map_function(key, value):
    """
    Process a single input record.

    Args:
        key: Input record key (e.g., line number, filename)
        value: Input record value (e.g., line content)

    Yields:
        (output_key, output_value) tuples
    """
    # Your map logic here
    yield (output_key, output_value)


def reduce_function(key, values):
    """
    Aggregate all values for a key.

    Args:
        key: The key to process
        values: List of all values for this key (from all mappers)

    Yields:
        (output_key, output_value) tuples
    """
    # Your reduce logic here
    yield (output_key, output_value)


def combiner_function(key, values):
    """
    Optional: Pre-aggregate values locally before shuffle.

    Args:
        key: The key to process
        values: List of values from local mapper

    Yields:
        (intermediate_key, intermediate_value) tuples
    """
    # Your combiner logic here (often same as reduce)
    yield (intermediate_key, intermediate_value)
```

### Example: Maximum Value Finder

```python
def map_function(key, value):
    """Emit (category, number) pairs."""
    category, number = value.split(',')
    yield (category, int(number))


def reduce_function(key, values):
    """Find maximum value for each category."""
    yield (key, max(values))


def combiner_function(key, values):
    """Find local maximum to reduce data transfer."""
    yield (key, max(values))
```

### Running Your Custom Job

1. **Upload your data and job file:**
   ```bash
   # Using docker volume mounts
   docker run --rm \
     --network coolest-mapreduce_mapreduce-net \
     -v mapreduce-data:/mapreduce-data \
     -v $(pwd)/my_jobs:/my_jobs \
     busybox cp /my_jobs/my_data.txt /mapreduce-data/inputs/

   docker run --rm \
     --network coolest-mapreduce_mapreduce-net \
     -v mapreduce-data:/mapreduce-data \
     -v $(pwd)/my_jobs:/my_jobs \
     busybox cp /my_jobs/my_job.py /mapreduce-data/jobs/
   ```

2. **Submit the job:**
   ```bash
   python3 client/client.py submit-job \
     --job-id $(uuidgen) \
     --input /mapreduce-data/inputs/my_data.txt \
     --output /mapreduce-data/outputs/my_output \
     --job-file my_jobs/my_job.py \
     --num-map-tasks 4 \
     --num-reduce-tasks 2 \
     --use-combiner  # Optional
   ```

3. **Monitor and retrieve results:**
   ```bash
   # Check status
   python3 client/client.py job-status <job-id>

   # Get results when completed
   python3 client/client.py get-results <job-id>
   ```

## Performance Tips

### Choosing Task Counts

- **Map tasks:** Generally 2-4x the number of worker nodes. More tasks = better load balancing but higher overhead
- **Reduce tasks:** Typically fewer than map tasks. Start with number of workers, adjust based on output size
- **Combiner:** Almost always beneficial for associative/commutative operations (sum, max, min)

### Optimizing Your Jobs

1. **Use combiners** when your reduce function is associative and commutative
2. **Minimize output** from map phase to reduce shuffle overhead
3. **Filter early** in the map function to reduce data volume
4. **Avoid expensive operations** in map function if possible (e.g., complex parsing)

## Troubleshooting

### Container Startup Issues

If runner scripts fail with connection errors:
```bash
# Check if containers are running
docker-compose ps

# Restart cluster
docker-compose down
docker-compose up -d

# Check logs
docker-compose logs coordinator
docker-compose logs worker-1 worker-2 worker-3 worker-4
```

**Common causes:**
- Port 50051 already in use (check with `lsof -i :50051`)
- Docker daemon not running (`systemctl status docker`)
- Insufficient resources (Docker needs 4+ cores, 8GB+ RAM)

### Data Upload Failures

If data doesn't appear in the system:
```bash
# Verify volume exists
docker volume ls | grep mapreduce-data

# Check volume contents
docker run --rm -v mapreduce-data:/data busybox ls -lah /data/inputs/

# Recreate volume if corrupted
docker-compose down -v
docker volume create mapreduce-data
docker-compose up -d
```

**Common causes:**
- Volume not created (run `docker volume create mapreduce-data`)
- Permission denied (ensure Docker has file access permissions)
- Volume mount path incorrect in docker-compose.yml

### Job Execution Errors

If jobs fail or hang:
```bash
# Check job status
python3 client/client.py job-status <job-id>

# View coordinator logs for task assignment
docker-compose logs --tail=100 coordinator

# View worker logs for execution errors
docker-compose logs --tail=50 worker-1
docker-compose logs --tail=50 worker-2
docker-compose logs --tail=50 worker-3
docker-compose logs --tail=50 worker-4

# Check intermediate files
docker exec coordinator ls -lh /mapreduce-data/intermediate/<job-id>/
```

**Common causes:**
- **Job stuck in map_phase**: Worker crashed or input file missing
  - Check worker logs for Python exceptions
  - Verify input file exists: `docker exec coordinator cat /mapreduce-data/inputs/<file>`
- **Job stuck in reduce_phase**: Intermediate files missing or corrupted
  - Check intermediate directory: `docker exec coordinator ls /mapreduce-data/intermediate/<job-id>/`
  - Verify JSON format: `docker exec coordinator head /mapreduce-data/intermediate/<job-id>/map-0-reduce-0.txt`
- **Job status: failed**: Function error or file I/O error
  - Check coordinator logs for error message
  - Review your map/reduce functions for bugs

### Python Import Errors

**Error:** `ModuleNotFoundError: No module named 'X'`

**Solution 1:** Use only Python standard library
```python
# Good - uses standard library
import string
import re

# Bad - requires external package
import pandas as pd  # Not available in worker containers
```

**Solution 2:** Install package in worker containers

Edit `worker/Dockerfile`:
```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
# Add your package here
RUN pip install pandas numpy
COPY . .
CMD ["python3", "worker_server.py"]
```

Then rebuild:
```bash
docker-compose build worker
docker-compose up -d
```

### Function Definition Errors

**Error:** `AttributeError: module has no attribute 'map_function'`

**Cause:** Missing or misspelled function name

**Solution:** Ensure your job file defines all three functions exactly:
```python
def map_function(key, value):      # Must be named exactly this
    yield (key, value)

def reduce_function(key, values):   # Must be named exactly this
    yield (key, sum(values))

def combiner_function(key, values): # Optional, but name must match
    yield (key, sum(values))
```

### gRPC Connection Errors

**Error:** `grpc._channel._InactiveRpcError: Connection refused`

**Cause:** Coordinator not running or not accessible

**Solution:**
```bash
# Check coordinator is running
docker-compose ps coordinator

# Check coordinator logs for startup errors
docker-compose logs coordinator

# Restart coordinator
docker-compose restart coordinator

# Check port mapping
docker-compose ps | grep coordinator  # Should show 0.0.0.0:50051->50051/tcp
```

### Performance Issues

**Job is very slow:**

1. **Check CPU usage:**
   ```bash
   docker stats --no-stream
   ```
   - Workers should be at ~100% CPU during execution
   - If low CPU usage, check for I/O bottleneck

2. **Check intermediate data size:**
   ```bash
   docker exec coordinator du -sh /mapreduce-data/intermediate/<job-id>/
   ```
   - If very large (>100x output), consider using combiner
   - Optimize map function to emit fewer key-value pairs

3. **Adjust task counts:**
   - Increase `--num-map-tasks` for better parallelism (try 8, 16)
   - Decrease if overhead is too high (try 4)

4. **Enable combiner:**
   - Add `--use-combiner` flag if not already enabled
   - Ensure combiner_function is defined

### Output File Issues

**Can't access output files from host:**

Output files are inside Docker volume. Access them using:

```bash
# Method 1: View in container
docker exec coordinator cat /mapreduce-data/outputs/<job-id>/part-0.txt

# Method 2: Copy to host
docker cp coordinator:/mapreduce-data/outputs/<job-id> ./output-local

# Method 3: Use volume mount
docker run --rm \
  -v mapreduce-data:/data \
  -v $(pwd)/output:/output \
  busybox cp -r /data/outputs/<job-id> /output/
```

**Output format issues:**

Expected format: Tab-separated key-value pairs
```
key1\tvalue1
key2\tvalue2
```

If you see different format, check your reduce_function yields:
```python
def reduce_function(key, values):
    # Good - yields tuple
    yield (key, sum(values))

    # Bad - yields string
    yield f"{key}: {sum(values)}"  # Will break output format
```

### Debugging Tips

1. **Start small:** Test with tiny input file (10-100 lines) first
2. **Check each phase:** Verify map output, intermediate files, reduce output separately
3. **Use print statements:** Add `print()` in map/reduce functions (appears in worker logs)
4. **Simplify first:** Start with simplest possible map/reduce, then add complexity
5. **Check JSON:** Intermediate files must be valid JSON (one object per line)

**Example debug map function:**
```python
def map_function(key, value):
    print(f"DEBUG: Processing line {key}: {value[:50]}")  # Truncate long lines
    words = value.split()
    print(f"DEBUG: Found {len(words)} words")
    for word in words:
        yield (word.lower(), 1)
```

View debug output:
```bash
docker-compose logs worker-1 | grep DEBUG
```

## Project Structure

```
examples/
├── README.md                   # This file
├── wordcount.py               # Word count example
├── inverted_index.py          # Inverted index example
├── setup_examples.sh          # Dataset setup script
├── run_wordcount.sh           # Word count runner
├── run_inverted_index.sh      # Inverted index runner
└── data/                      # Generated datasets
    ├── shakespeare.txt        # Shakespeare complete works
    └── synthetic.txt          # Synthetic test data
```

## Further Reading

- [Main Project README](../README.md) - System architecture and setup
- [Client Documentation](../client/README.md) - Full client CLI reference
- [Worker Documentation](../worker/README.md) - Worker implementation details

## Contributing Examples

Have an interesting MapReduce job example? Consider contributing it! Good examples:
- Demonstrate a specific use case or algorithm
- Include clear documentation
- Use realistic datasets
- Follow the same structure as existing examples
