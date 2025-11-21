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
docker-compose logs worker
```

### Data Upload Failures

If data doesn't appear in the system:
```bash
# Verify volume exists
docker volume ls | grep mapreduce-data

# Check volume contents
docker run --rm -v mapreduce-data:/data busybox ls -lah /data/inputs/
```

### Job Execution Errors

If jobs fail or hang:
```bash
# Check job status
python3 client/client.py job-status <job-id>

# View coordinator logs
docker-compose logs coordinator

# View worker logs
docker-compose logs worker
```

### Python Import Errors

Make sure your job file:
- Has all three required functions defined
- Uses only standard library imports (or libraries available in worker containers)
- Has proper Python syntax

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
