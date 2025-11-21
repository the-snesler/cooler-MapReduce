#!/bin/bash
# examples/run_wordcount.sh

set -e

echo "Running Word Count Example"
echo "==========================="

# Build and start containers
echo "Starting MapReduce cluster..."
docker-compose up -d
sleep 5

# Upload example data
echo "Uploading example data..."
docker run --rm \
  --network coolest-mapreduce_mapreduce-net \
  -v mapreduce-data:/mapreduce-data \
  -v $(pwd)/examples:/examples \
  busybox cp /examples/data/shakespeare.txt /mapreduce-data/inputs/

docker run --rm \
  --network coolest-mapreduce_mapreduce-net \
  -v mapreduce-data:/mapreduce-data \
  -v $(pwd)/examples:/examples \
  busybox cp /examples/wordcount.py /mapreduce-data/jobs/

# Submit job
echo "Submitting word count job..."
JOB_ID=$(uuidgen)

python3 client/client.py submit-job \
  --job-id $JOB_ID \
  --input /mapreduce-data/inputs/shakespeare.txt \
  --output /mapreduce-data/outputs/wordcount \
  --job-file examples/wordcount.py \
  --num-map-tasks 8 \
  --num-reduce-tasks 4 \
  --use-combiner

# Poll job status
echo "Waiting for job to complete..."
while true; do
    STATUS=$(python3 client/client.py job-status $JOB_ID | grep "Status:" | awk '{print $2}')
    if [ "$STATUS" == "completed" ]; then
        break
    fi
    sleep 2
done

# Get results
echo "Job completed! Retrieving results..."
python3 client/client.py get-results $JOB_ID

# Show sample output
echo ""
echo "Sample output (top 20 words):"
docker run --rm \
  -v mapreduce-data:/mapreduce-data \
  busybox sh -c "cat /mapreduce-data/outputs/wordcount/part-*.txt | sort -t$'\t' -k2 -nr | head -20"
