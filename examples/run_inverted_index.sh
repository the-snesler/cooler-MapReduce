#!/bin/bash
# examples/run_inverted_index.sh

set -e

echo "Running Inverted Index Example"
echo "==============================="

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
  busybox cp /examples/data/synthetic.txt /mapreduce-data/inputs/

docker run --rm \
  --network coolest-mapreduce_mapreduce-net \
  -v mapreduce-data:/mapreduce-data \
  -v $(pwd)/examples:/examples \
  busybox cp /examples/inverted_index.py /mapreduce-data/jobs/

# Submit job
echo "Submitting inverted index job..."
JOB_ID=$(uuidgen)

python3 client/client.py submit-job \
  --job-id $JOB_ID \
  --input /mapreduce-data/inputs/synthetic.txt \
  --output /mapreduce-data/outputs/inverted_index \
  --job-file examples/inverted_index.py \
  --num-map-tasks 4 \
  --num-reduce-tasks 2 \
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
echo "Sample output (first 20 entries):"
docker run --rm \
  -v mapreduce-data:/mapreduce-data \
  busybox sh -c "cat /mapreduce-data/outputs/inverted_index/part-*.txt | head -20"
