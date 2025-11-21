#!/usr/bin/env python3
"""
MapReduce Client CLI
Provides commands for data upload, job submission, status checking, and result retrieval
"""

import argparse
import sys
import os
import uuid
import shutil
import grpc

# Add parent directory to path to import common modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import mapreduce_pb2
import mapreduce_pb2_grpc
from common.grpc_client import get_job_service_stub

# Configuration from environment
COORDINATOR_HOST = os.getenv('COORDINATOR_HOST', 'localhost:50051')
SHARED_VOLUME = os.getenv('SHARED_VOLUME', '/mapreduce-data')


def upload_data(args):
    """Upload local file to shared storage"""
    src = args.source
    dest = args.destination

    if not os.path.exists(src):
        print(f"Error: Source file {src} not found")
        return 1

    # Ensure destination path is within shared volume
    dest_path = os.path.join(SHARED_VOLUME, dest.lstrip('/'))
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    try:
        shutil.copy(src, dest_path)
        print(f"✓ Uploaded {src} to {dest_path}")
        return 0
    except Exception as e:
        print(f"Error uploading file: {e}")
        return 1


def submit_job(args):
    """Submit MapReduce job to coordinator"""
    job_id = args.job_id or str(uuid.uuid4())

    # Upload job file first
    if not os.path.exists(args.job_file):
        print(f"Error: Job file {args.job_file} not found")
        return 1

    job_dest = f"/jobs/{os.path.basename(args.job_file)}"

    # Upload job file to shared storage
    upload_args = type('obj', (object,), {
        'source': args.job_file,
        'destination': job_dest
    })
    upload_result = upload_data(upload_args)
    if upload_result != 0:
        print("Error: Failed to upload job file")
        return upload_result

    # Create gRPC stub and submit job
    try:
        stub = get_job_service_stub(COORDINATOR_HOST)

        request = mapreduce_pb2.JobSpec(
            job_id=job_id,
            input_path=args.input,
            output_path=args.output,
            map_reduce_file=job_dest,
            num_map_tasks=args.num_map_tasks,
            num_reduce_tasks=args.num_reduce_tasks,
            use_combiner=args.use_combiner
        )

        response = stub.SubmitJob(request)
        print(f"✓ Job submitted successfully!")
        print(f"  Job ID: {response.job_id}")
        print(f"  Status: {response.status}")
        return 0

    except ConnectionError as e:
        print(f"Error connecting to coordinator: {e}")
        return 1
    except grpc.RpcError as e:
        print(f"Error submitting job: {e.details()}")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1


def job_status(args):
    """Check job status"""
    try:
        stub = get_job_service_stub(COORDINATOR_HOST)

        request = mapreduce_pb2.JobStatusRequest(job_id=args.job_id)
        response = stub.GetJobStatus(request)

        print(f"Job ID: {args.job_id}")
        print(f"Status: {response.status}")
        print(f"Progress: {response.progress_percentage}%")
        return 0

    except ConnectionError as e:
        print(f"Error connecting to coordinator: {e}")
        return 1
    except grpc.RpcError as e:
        print(f"Error getting status: {e.details()}")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1


def get_results(args):
    """Retrieve job results"""
    try:
        stub = get_job_service_stub(COORDINATOR_HOST)

        request = mapreduce_pb2.JobResultRequest(job_id=args.job_id)
        response = stub.GetJobResult(request)

        print(f"Job ID: {args.job_id}")
        print(f"Output path: {response.output_path}")
        print(f"Metrics: {response.metrics}")

        # Optionally download results to local directory
        if args.download:
            src_path = os.path.join(SHARED_VOLUME, response.output_path.lstrip('/'))
            dest_path = args.download

            if os.path.exists(src_path):
                print(f"Downloading results to {dest_path}...")
                os.makedirs(dest_path, exist_ok=True)

                # Copy all files from output directory
                if os.path.isdir(src_path):
                    for filename in os.listdir(src_path):
                        shutil.copy(
                            os.path.join(src_path, filename),
                            os.path.join(dest_path, filename)
                        )
                else:
                    shutil.copy(src_path, dest_path)

                print(f"✓ Results downloaded to {dest_path}")
            else:
                print(f"Warning: Output path {src_path} not found in shared volume")

        return 0

    except ConnectionError as e:
        print(f"Error connecting to coordinator: {e}")
        return 1
    except grpc.RpcError as e:
        print(f"Error getting results: {e.details()}")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description='MapReduce Client CLI',
        epilog='Example: %(prog)s submit-job --input /data/input.txt --output /output --job-file wordcount.py'
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # upload-data command
    upload_parser = subparsers.add_parser(
        'upload-data',
        help='Upload data to shared storage',
        description='Upload a local file to the MapReduce shared storage volume'
    )
    upload_parser.add_argument('source', help='Local source file path')
    upload_parser.add_argument('destination', help='Destination path in shared storage (relative to /mapreduce-data)')
    upload_parser.set_defaults(func=upload_data)

    # submit-job command
    submit_parser = subparsers.add_parser(
        'submit-job',
        help='Submit MapReduce job',
        description='Submit a new MapReduce job to the coordinator'
    )
    submit_parser.add_argument('--input', required=True, help='Input data path (relative to /mapreduce-data)')
    submit_parser.add_argument('--output', required=True, help='Output path (relative to /mapreduce-data)')
    submit_parser.add_argument('--job-file', required=True, help='Python file with map/reduce functions')
    submit_parser.add_argument('--num-map-tasks', type=int, default=4, help='Number of map tasks (default: 4)')
    submit_parser.add_argument('--num-reduce-tasks', type=int, default=2, help='Number of reduce tasks (default: 2)')
    submit_parser.add_argument('--use-combiner', action='store_true', help='Enable combiner optimization')
    submit_parser.add_argument('--job-id', help='Custom job ID (auto-generated if not provided)')
    submit_parser.set_defaults(func=submit_job)

    # job-status command
    status_parser = subparsers.add_parser(
        'job-status',
        help='Check job status',
        description='Query the status and progress of a submitted job'
    )
    status_parser.add_argument('job_id', help='Job ID to check')
    status_parser.set_defaults(func=job_status)

    # get-results command
    results_parser = subparsers.add_parser(
        'get-results',
        help='Get job results',
        description='Retrieve results and metrics for a completed job'
    )
    results_parser.add_argument('job_id', help='Job ID')
    results_parser.add_argument('--download', help='Download results to local directory')
    results_parser.set_defaults(func=get_results)

    # Parse arguments
    args = parser.parse_args()

    # If no command specified, print help
    if not hasattr(args, 'func'):
        parser.print_help()
        return 1

    # Execute the command
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
