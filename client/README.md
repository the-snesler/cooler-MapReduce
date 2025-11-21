# MapReduce Client CLI

Command-line interface for interacting with the MapReduce system.

## Features

- Upload data to shared storage
- Submit MapReduce jobs
- Check job status and progress
- Retrieve job results

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Upload Data

Upload a local file to the MapReduce shared storage:

```bash
./client.py upload-data <local-file> <destination-path>
```

Example:
```bash
./client.py upload-data input.txt /data/input.txt
```

### Submit Job

Submit a MapReduce job:

```bash
./client.py submit-job --input <input-path> --output <output-path> --job-file <job-file.py> [options]
```

Options:
- `--num-map-tasks`: Number of map tasks (default: 4)
- `--num-reduce-tasks`: Number of reduce tasks (default: 2)
- `--use-combiner`: Enable combiner optimization
- `--job-id`: Custom job ID (auto-generated if not provided)

Example:
```bash
./client.py submit-job --input /data/input.txt --output /output --job-file wordcount.py --num-map-tasks 8
```

### Check Job Status

Query the status and progress of a job:

```bash
./client.py job-status <job-id>
```

Example:
```bash
./client.py job-status abc-123
```

### Get Results

Retrieve results for a completed job:

```bash
./client.py get-results <job-id> [--download <local-dir>]
```

Example:
```bash
./client.py get-results abc-123 --download ./results
```

## Environment Variables

- `COORDINATOR_HOST`: Coordinator address (default: `localhost:50051`)
- `SHARED_VOLUME`: Shared storage mount path (default: `/mapreduce-data`)

## Docker Usage

Use the provided wrapper script to run the client in a Docker container:

```bash
./run_client.sh upload-data input.txt /data/input.txt
./run_client.sh submit-job --input /data/input.txt --output /output --job-file wordcount.py
./run_client.sh job-status <job-id>
./run_client.sh get-results <job-id>
```
