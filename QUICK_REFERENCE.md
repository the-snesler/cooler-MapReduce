# Quick Reference: Checking Job Status

## Basic Commands

### 1. Submit a Job
```bash
python3 src/client/client.py submit \
  --input shared/input/test.txt \
  --output shared/output/test \
  --job-file shared/jobs/test.py \
  --num-map 2 \
  --num-reduce 2
```

### 2. Check Job Status (Basic)
```bash
python3 src/client/client.py status <job_id>
```

Example:
```bash
python3 src/client/client.py status bbd2a324-28e8-4a61-abb5-05c1becf874c
```

### 3. Watch Job Progress (Real-time)
```bash
python3 src/client/client.py status <job_id> --watch
```

### 4. Get Detailed Status
```bash
# Show active tasks
python3 src/client/client.py status <job_id> --tasks

# Show resource usage
python3 src/client/client.py status <job_id> --resources

# Show both
python3 src/client/client.py status <job_id> --tasks --resources
```

### 5. List All Jobs
```bash
python3 src/client/client.py list
```

### 6. Get Job Results
```bash
python3 src/client/client.py results <job_id>
```

## Alternative: Check Coordinator Logs

```bash
# View recent logs
docker logs mapreduce-coordinator --tail 50

# Follow logs in real-time
docker logs mapreduce-coordinator -f

# View logs for a specific job
docker logs mapreduce-coordinator 2>&1 | grep <job_id>
```

## Check Worker Logs

```bash
# Check all workers
docker logs mapreduce-worker1 --tail 20
docker logs mapreduce-worker2 --tail 20
docker logs mapreduce-worker3 --tail 20
docker logs mapreduce-worker4 --tail 20
```

## Check Intermediate Files

```bash
# Check if map tasks created intermediate files
python3 scripts/check_map_output.py

# Check for specific job
python3 scripts/check_map_output.py --job-id <job_id>

# List files directly
ls -lh shared/intermediate/
```

## Common Status Values

- **SUBMITTED**: Job received, tasks being created
- **MAPPING**: Map phase in progress
- **REDUCING**: Reduce phase in progress
- **COMPLETED**: Job finished successfully
- **FAILED**: Job failed (check error_message)
- **CANCELLED**: Job was cancelled

## Troubleshooting

### Job stuck in SUBMITTED
- Check coordinator logs for errors
- Verify workers are running: `docker ps`
- Check if workers are sending heartbeats

### Job stuck in MAPPING
- Check if intermediate files are being created
- Check worker logs for errors
- Verify map function is correct

### No intermediate files
- Check worker logs for map task execution errors
- Verify input file exists and is readable
- Check job file has correct `map_fn` function

