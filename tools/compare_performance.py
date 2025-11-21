#!/usr/bin/env python3
"""
Run the same job with and without combiner to measure effectiveness.
"""

import subprocess
import json
import uuid
import time
import sys


def run_job(job_file, input_path, num_map_tasks, num_reduce_tasks, use_combiner):
    """Run a MapReduce job and return its ID and metrics."""
    job_id = str(uuid.uuid4())

    cmd = [
        'python3', 'client/client.py', 'submit-job',
        '--job-id', job_id,
        '--input', input_path,
        '--output', f'/mapreduce-data/outputs/{job_id}',
        '--job-file', job_file,
        '--num-map-tasks', str(num_map_tasks),
        '--num-reduce-tasks', str(num_reduce_tasks)
    ]

    if use_combiner:
        cmd.append('--use-combiner')

    print(f"Submitting job {job_id}...")
    subprocess.run(cmd, check=True)

    # Poll for completion
    print("Waiting for job to complete...")
    while True:
        result = subprocess.run(
            ['python3', 'client/client.py', 'job-status', job_id],
            capture_output=True, text=True
        )
        if 'completed' in result.stdout.lower():
            break
        if 'failed' in result.stdout.lower():
            print(f"Job {job_id} failed!")
            return job_id, None
        time.sleep(2)

    # Get metrics
    print(f"Job {job_id} completed!")
    result = subprocess.run(
        ['python3', 'client/client.py', 'get-results', job_id],
        capture_output=True, text=True
    )

    return job_id, result.stdout


def main():
    if len(sys.argv) < 3:
        print("Usage: python3 compare_performance.py <job_file> <input_path> [num_map_tasks] [num_reduce_tasks]")
        sys.exit(1)

    job_file = sys.argv[1]
    input_path = sys.argv[2]
    num_map_tasks = int(sys.argv[3]) if len(sys.argv) > 3 else 8
    num_reduce_tasks = int(sys.argv[4]) if len(sys.argv) > 4 else 4

    print("=" * 60)
    print("MAPREDUCE PERFORMANCE COMPARISON")
    print("=" * 60)
    print(f"Job file: {job_file}")
    print(f"Input: {input_path}")
    print(f"Map tasks: {num_map_tasks}, Reduce tasks: {num_reduce_tasks}")
    print("=" * 60)

    print("\n[1/2] Running job WITHOUT combiner...")
    print("-" * 60)
    job_id_no_combiner, metrics_no_combiner = run_job(
        job_file, input_path, num_map_tasks, num_reduce_tasks, use_combiner=False
    )

    print("\n[2/2] Running job WITH combiner...")
    print("-" * 60)
    job_id_with_combiner, metrics_with_combiner = run_job(
        job_file, input_path, num_map_tasks, num_reduce_tasks, use_combiner=True
    )

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)

    print("\nWithout Combiner:")
    print(f"  Job ID: {job_id_no_combiner}")
    if metrics_no_combiner:
        print(metrics_no_combiner)

    print("\nWith Combiner:")
    print(f"  Job ID: {job_id_with_combiner}")
    if metrics_with_combiner:
        print(metrics_with_combiner)

    print("\n" + "=" * 60)
    print("Metrics files saved at:")
    print(f"  /mapreduce-data/metrics/{job_id_no_combiner}.json")
    print(f"  /mapreduce-data/metrics/{job_id_with_combiner}.json")
    print("=" * 60)


if __name__ == '__main__':
    main()
