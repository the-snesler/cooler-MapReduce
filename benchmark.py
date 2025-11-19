#!/usr/bin/env python3
"""
Automated benchmarking script for MapReduce system.
Runs multiple job configurations and collects performance metrics.
"""

import subprocess
import json
import time
import os
import sys
from datetime import datetime
from pathlib import Path
import csv

# Configuration
COORDINATOR_HOST = "localhost:50051"
CLIENT_CMD = ["python3", "src/client/client.py"]
RESULTS_DIR = Path("benchmark_results")
SHARED_DIR = Path("shared")
INPUT_DIR = SHARED_DIR / "input"
OUTPUT_DIR = SHARED_DIR / "output"
SAMPLES_DIR = Path("samples")

# Benchmark configurations
BENCHMARKS = [
    # Experiment 1: Input Size Scaling (fixed parallelism)
    {
        "name": "input_size_small",
        "input": "/shared/input/story_sm.txt",
        "job": "/shared/samples/word_count.py",
        "maps": 4,
        "reduces": 2,
        "description": "Small input (4KB), baseline"
    },
    {
        "name": "input_size_medium",
        "input": "/shared/input/story_medium.txt",
        "job": "/shared/samples/word_count.py",
        "maps": 4,
        "reduces": 2,
        "description": "Medium input (~1MB)"
    },
    {
        "name": "input_size_large",
        "input": "/shared/input/story_large.txt",
        "job": "/shared/samples/word_count.py",
        "maps": 4,
        "reduces": 2,
        "description": "Large input (~10MB)"
    },
    {
        "name": "input_size_xlarge",
        "input": "/shared/input/story_xlarge.txt",
        "job": "/shared/samples/word_count.py",
        "maps": 4,
        "reduces": 2,
        "description": "Extra large input (~50MB)"
    },

    # Experiment 2: Map Task Scaling (fixed input)
    {
        "name": "map_scaling_1",
        "input": "/shared/input/story_large.txt",
        "job": "/shared/samples/word_count.py",
        "maps": 1,
        "reduces": 2,
        "description": "1 map task"
    },
    {
        "name": "map_scaling_2",
        "input": "/shared/input/story_large.txt",
        "job": "/shared/samples/word_count.py",
        "maps": 2,
        "reduces": 2,
        "description": "2 map tasks"
    },
    {
        "name": "map_scaling_4",
        "input": "/shared/input/story_large.txt",
        "job": "/shared/samples/word_count.py",
        "maps": 4,
        "reduces": 2,
        "description": "4 map tasks"
    },
    {
        "name": "map_scaling_8",
        "input": "/shared/input/story_large.txt",
        "job": "/shared/samples/word_count.py",
        "maps": 8,
        "reduces": 2,
        "description": "8 map tasks"
    },

    # Experiment 3: Reduce Task Scaling (fixed input)
    {
        "name": "reduce_scaling_1",
        "input": "/shared/input/story_large.txt",
        "job": "/shared/samples/word_count.py",
        "maps": 4,
        "reduces": 1,
        "description": "1 reduce task"
    },
    {
        "name": "reduce_scaling_2",
        "input": "/shared/input/story_large.txt",
        "job": "/shared/samples/word_count.py",
        "maps": 4,
        "reduces": 2,
        "description": "2 reduce tasks"
    },
    {
        "name": "reduce_scaling_4",
        "input": "/shared/input/story_large.txt",
        "job": "/shared/samples/word_count.py",
        "maps": 4,
        "reduces": 4,
        "description": "4 reduce tasks"
    },
    {
        "name": "reduce_scaling_8",
        "input": "/shared/input/story_large.txt",
        "job": "/shared/samples/word_count.py",
        "maps": 4,
        "reduces": 8,
        "description": "8 reduce tasks"
    },

    # Experiment 4: Combined Scaling
    {
        "name": "combined_1_1",
        "input": "/shared/input/story_xlarge.txt",
        "job": "/shared/samples/word_count.py",
        "maps": 1,
        "reduces": 1,
        "description": "Combined: 1 map, 1 reduce"
    },
    {
        "name": "combined_2_2",
        "input": "/shared/input/story_xlarge.txt",
        "job": "/shared/samples/word_count.py",
        "maps": 2,
        "reduces": 2,
        "description": "Combined: 2 maps, 2 reduces"
    },
    {
        "name": "combined_4_2",
        "input": "/shared/input/story_xlarge.txt",
        "job": "/shared/samples/word_count.py",
        "maps": 4,
        "reduces": 2,
        "description": "Combined: 4 maps, 2 reduces"
    },
    {
        "name": "combined_8_4",
        "input": "/shared/input/story_xlarge.txt",
        "job": "/shared/samples/word_count.py",
        "maps": 8,
        "reduces": 4,
        "description": "Combined: 8 maps, 4 reduces"
    },
]


def setup_directories():
    """Create necessary directories."""
    RESULTS_DIR.mkdir(exist_ok=True)
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"✓ Created directories: {RESULTS_DIR}, {INPUT_DIR}, {OUTPUT_DIR}")


def check_prerequisites():
    """Verify required files exist."""
    required_files = [
        "src/client/client.py",
        "samples/word_count.py",
        "shared/input/story_sm.txt"
    ]

    for file_path in required_files:
        if not Path(file_path).exists():
            print(f"❌ Missing required file: {file_path}")
            sys.exit(1)

    print("✓ All prerequisite files found")


def get_file_size(path):
    """Get file size in bytes."""
    try:
        return os.path.getsize(path)
    except:
        return 0


def submit_job(config):
    """Submit a MapReduce job and return job ID."""
    output_path = f"/shared/output/{config['name']}"

    cmd = CLIENT_CMD + [
        "submit",
        "--input", config["input"],
        "--output", output_path,
        "--job-file", config["job"],
        "--num-map", str(config["maps"]),
        "--num-reduce", str(config["reduces"])
    ]

    print(f"  Submitting: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"  ❌ Submission failed: {result.stderr}")
        return None

    # Parse job ID from output (format: "Job submitted successfully. Job ID: <id>")
    for line in result.stdout.split('\n'):
        if "Job ID:" in line:
            job_id = line.split("Job ID:")[-1].strip()
            print(f"  ✓ Job submitted: {job_id}")
            return job_id

    print(f"  ❌ Could not parse job ID from output:\n{result.stdout}")
    return None


def get_job_status(job_id):
    """Get current status of a job."""
    cmd = CLIENT_CMD + ["status", job_id]
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        return None

    # Parse status from output
    status_data = {}
    for line in result.stdout.split('\n'):
        if ':' in line:
            key, value = line.split(':', 1)
            status_data[key.strip()] = value.strip()

    return status_data


def wait_for_completion(job_id, timeout=600, poll_interval=2):
    """
    Poll job status until completion or timeout.
    Returns: (success: bool, duration: float, status_data: dict)
    """
    start_time = time.time()
    last_status = None

    print(f"  Waiting for completion (timeout: {timeout}s)...")

    while time.time() - start_time < timeout:
        status_data = get_job_status(job_id)

        if status_data is None:
            print(f"  ❌ Failed to get job status")
            return False, time.time() - start_time, None

        current_status = status_data.get('Status', 'UNKNOWN')

        if current_status != last_status:
            print(f"    Status: {current_status}")
            last_status = current_status

        if current_status == "COMPLETED":
            duration = time.time() - start_time
            print(f"  ✓ Job completed in {duration:.2f}s")
            return True, duration, status_data

        if current_status == "FAILED":
            duration = time.time() - start_time
            print(f"  ❌ Job failed after {duration:.2f}s")
            return False, duration, status_data

        time.sleep(poll_interval)

    # Timeout
    duration = time.time() - start_time
    print(f"  ⏱️ Timeout after {duration:.2f}s")
    return False, duration, get_job_status(job_id)


def run_benchmark(config, run_number=1):
    """Run a single benchmark configuration."""
    print(f"\n{'='*70}")
    print(f"Benchmark: {config['name']} (Run {run_number})")
    print(f"Description: {config['description']}")
    print(f"Config: {config['maps']} maps, {config['reduces']} reduces")
    print(f"{'='*70}")

    # Check input file exists (convert container path to host path)
    host_input_path = config['input'].replace('/shared/', 'shared/')
    if not Path(host_input_path).exists():
        print(f"❌ Input file not found: {host_input_path}")
        print(f"   Skipping this benchmark...")
        return None

    input_size = get_file_size(host_input_path)
    print(f"Input size: {input_size / 1024 / 1024:.2f} MB")

    # Wait a moment to ensure workers are ready (they send heartbeats every 5 seconds)
    print("  Waiting for workers to be ready...")
    time.sleep(6)  # Wait slightly longer than heartbeat interval

    # Submit job
    job_id = submit_job(config)
    if job_id is None:
        return None

    # Wait for completion
    success, duration, status_data = wait_for_completion(job_id)

    # Collect results
    result = {
        "benchmark_name": config["name"],
        "description": config["description"],
        "run_number": run_number,
        "timestamp": datetime.now().isoformat(),
        "job_id": job_id,
        "input_file": config["input"],
        "input_size_bytes": input_size,
        "input_size_mb": round(input_size / 1024 / 1024, 2),
        "job_file": config["job"],
        "num_map_tasks": config["maps"],
        "num_reduce_tasks": config["reduces"],
        "success": success,
        "total_runtime_seconds": round(duration, 2),
        "throughput_mbps": round((input_size / 1024 / 1024) / duration, 3) if duration > 0 else 0,
        "status": status_data.get('Status', 'UNKNOWN') if status_data else 'UNKNOWN'
    }

    # Add phase durations if available
    if status_data:
        result.update({
            "phase": status_data.get('Phase', ''),
            "completed_map_tasks": status_data.get('Completed Map Tasks', ''),
            "completed_reduce_tasks": status_data.get('Completed Reduce Tasks', ''),
        })

    return result


def save_results(results, timestamp):
    """Save results to JSON and CSV files."""
    # JSON format
    json_file = RESULTS_DIR / f"benchmark_results_{timestamp}.json"
    with open(json_file, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\n✓ Results saved to: {json_file}")

    # CSV format
    csv_file = RESULTS_DIR / f"benchmark_results_{timestamp}.csv"
    if results:
        fieldnames = list(results[0].keys())
        with open(csv_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        print(f"✓ Results saved to: {csv_file}")

    return json_file, csv_file


def print_summary(results):
    """Print a summary table of results."""
    print(f"\n{'='*70}")
    print("BENCHMARK SUMMARY")
    print(f"{'='*70}")
    print(f"{'Benchmark':<25} {'Maps':>5} {'Reduces':>7} {'Runtime':>10} {'Status':>10}")
    print(f"{'-'*70}")

    for r in results:
        print(f"{r['benchmark_name']:<25} {r['num_map_tasks']:>5} "
              f"{r['num_reduce_tasks']:>7} {r['total_runtime_seconds']:>9.2f}s "
              f"{'✓' if r['success'] else '✗':>10}")

    print(f"{'='*70}")

    successful = sum(1 for r in results if r['success'])
    print(f"Total: {len(results)} benchmarks, {successful} successful, "
          f"{len(results) - successful} failed")


def main():
    """Main benchmarking workflow."""
    print("="*70)
    print("MapReduce Performance Benchmark Suite")
    print("="*70)

    # Setup
    setup_directories()
    check_prerequisites()

    # Check for auto-run mode
    auto_run = "--auto" in sys.argv or "-y" in sys.argv

    # Ask for number of runs per benchmark
    if auto_run:
        runs_per_benchmark = 1
    else:
        try:
            runs_per_benchmark = int(input("\nNumber of runs per benchmark (1-5, default=1): ") or "1")
            runs_per_benchmark = max(1, min(5, runs_per_benchmark))
        except ValueError:
            runs_per_benchmark = 1

    print(f"\nRunning {len(BENCHMARKS)} benchmarks × {runs_per_benchmark} runs = "
          f"{len(BENCHMARKS) * runs_per_benchmark} total jobs")

    # Confirm
    if auto_run:
        response = 'y'
    else:
        response = input("\nProceed? (y/n): ")

    if response.lower() != 'y':
        print("Aborted.")
        return

    # Run benchmarks
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    all_results = []

    for config in BENCHMARKS:
        for run in range(1, runs_per_benchmark + 1):
            result = run_benchmark(config, run_number=run)
            if result:
                all_results.append(result)

            # Brief pause between runs
            if run < runs_per_benchmark:
                time.sleep(2)

    # Save results
    if all_results:
        json_file, csv_file = save_results(all_results, timestamp)
        print_summary(all_results)

        print(f"\n{'='*70}")
        print("Next steps:")
        print(f"  1. Review results: cat {json_file}")
        print(f"  2. Generate plots: python plot_results.py {json_file}")
        print(f"  3. Update report: docs/WEEK4_REPORT.md")
        print(f"{'='*70}")
    else:
        print("\n❌ No results collected")


if __name__ == "__main__":
    main()
