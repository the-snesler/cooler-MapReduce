"""
Client for submitting and managing MapReduce jobs.
"""

import grpc
import sys
import os
import argparse

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import coordinator_pb2
import coordinator_pb2_grpc


def submit_job(args):
    """Submit a MapReduce job to the coordinator."""
    with grpc.insecure_channel(args.coordinator_host) as channel:
        stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
        
        request = coordinator_pb2.JobRequest(
            input_path=args.input,
            output_path=args.output,
            job_file_path=args.job_file,
            num_map_tasks=args.num_map,
            num_reduce_tasks=args.num_reduce
        )
        
        try:
            response = stub.SubmitJob(request)
            print(f"Job submitted successfully!")
            print(f"Job ID: {response.job_id}")
            print(f"Status: {response.status}")
        except grpc.RpcError as e:
            print(f"Error submitting job: {e}")
            sys.exit(1)


def get_status(args):
    """Get the status of a job."""
    with grpc.insecure_channel(args.coordinator_host) as channel:
        stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
        
        request = coordinator_pb2.JobStatusRequest(job_id=args.job_id)
        
        try:
            response = stub.GetJobStatus(request)
            print(f"Job ID: {response.job_id}")
            print(f"Status: {response.status}")
            print(f"Map tasks: {response.completed_map_tasks}/{response.total_map_tasks}")
            print(f"Reduce tasks: {response.completed_reduce_tasks}/{response.total_reduce_tasks}")
            if response.error_message:
                print(f"Error: {response.error_message}")
        except grpc.RpcError as e:
            print(f"Error getting job status: {e}")
            sys.exit(1)


def list_jobs(args):
    """List all jobs."""
    with grpc.insecure_channel(args.coordinator_host) as channel:
        stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
        
        request = coordinator_pb2.Empty()
        
        try:
            response = stub.ListJobs(request)
            print(f"Total jobs: {len(response.jobs)}")
            print()
            for job in response.jobs:
                print(f"Job ID: {job.job_id}")
                print(f"  Status: {job.status}")
                print(f"  Submit Time: {job.submit_time}")
                print()
        except grpc.RpcError as e:
            print(f"Error listing jobs: {e}")
            sys.exit(1)


def get_results(args):
    """Get results of a job."""
    with grpc.insecure_channel(args.coordinator_host) as channel:
        stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
        
        request = coordinator_pb2.JobResultsRequest(job_id=args.job_id)
        
        try:
            response = stub.GetJobResults(request)
            print(f"Job ID: {response.job_id}")
            print(f"Status: {response.status}")
            print(f"Output files:")
            for output_file in response.output_files:
                print(f"  - {output_file}")
        except grpc.RpcError as e:
            print(f"Error getting job results: {e}")
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='MapReduce Client')
    parser.add_argument('--coordinator-host', type=str, default='localhost:50051',
                        help='Coordinator address (default: localhost:50051)')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Submit command
    submit_parser = subparsers.add_parser('submit', help='Submit a new job')
    submit_parser.add_argument('--input', type=str, required=True,
                              help='Path to input file(s) in shared storage')
    submit_parser.add_argument('--output', type=str, required=True,
                              help='Directory path for output files in shared storage')
    submit_parser.add_argument('--job-file', type=str, required=True,
                              help='Path to user job file in shared storage')
    submit_parser.add_argument('--num-map', type=int, required=True,
                              help='Number of map tasks')
    submit_parser.add_argument('--num-reduce', type=int, required=True,
                              help='Number of reduce tasks')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Get job status')
    status_parser.add_argument('--job-id', type=str, required=True,
                              help='Job ID')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List all jobs')
    
    # Results command
    results_parser = subparsers.add_parser('results', help='Get job results')
    results_parser.add_argument('--job-id', type=str, required=True,
                               help='Job ID')
    
    args = parser.parse_args()
    
    if args.command == 'submit':
        submit_job(args)
    elif args.command == 'status':
        get_status(args)
    elif args.command == 'list':
        list_jobs(args)
    elif args.command == 'results':
        get_results(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
