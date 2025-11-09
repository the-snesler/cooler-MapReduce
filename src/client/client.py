"""
Client for submitting and managing MapReduce jobs.
"""

import grpc
import sys
import os
import argparse
from typing import Dict, List, Optional

# Import monitoring functions from the same directory
from monitoring import (
    monitor_job_progress,
    format_progress_bar,
    show_resource_usage,
    list_active_tasks,
    cancel_job
)

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
    """Get the status of a job with detailed monitoring."""
    with grpc.insecure_channel(args.coordinator_host) as channel:
        stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
        
        if args.watch:
            # Interactive monitoring mode
            monitor_job_progress(stub, args.job_id, interval=args.interval)
        else:
            # One-time status report
            try:
                status = stub.GetJobStatus(coordinator_pb2.JobStatusRequest(job_id=args.job_id))
                print(f"Job ID: {status.job_id}")
                print(f"Status: {status.status}")
                print(f"Phase: {status.phase}")
                
                if status.phase == "MAP":
                    print("\nMap Progress:")
                    print(format_progress_bar(status.completed_map_tasks, status.total_map_tasks))
                elif status.phase == "REDUCE":
                    print("\nReduce Progress:")
                    print(format_progress_bar(status.completed_reduce_tasks, status.total_reduce_tasks))
                
                if status.error_message:
                    print(f"\nError: {status.error_message}")
                    
                if args.resources:
                    show_resource_usage(stub, args.job_id)
                    
                if args.tasks:
                    list_active_tasks(stub, args.job_id)
                    
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
    parser = argparse.ArgumentParser(description="MapReduce Client")
    parser.add_argument("--coordinator-host", default="localhost:50051",
                      help="Host:port of coordinator")
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Submit job command
    submit_parser = subparsers.add_parser("submit", help="Submit a new job")
    submit_parser.add_argument("--input", required=True, help="Input file path")
    submit_parser.add_argument("--output", required=True, help="Output directory")
    submit_parser.add_argument("--job-file", required=True, help="Python file with map/reduce functions")
    submit_parser.add_argument("--num-map", type=int, default=4, help="Number of map tasks")
    submit_parser.add_argument("--num-reduce", type=int, default=2, help="Number of reduce tasks")
    
    # Status command with monitoring options
    status_parser = subparsers.add_parser("status", help="Get job status")
    status_parser.add_argument("job_id", help="Job ID to check")
    status_parser.add_argument("--watch", "-w", action="store_true",
                             help="Watch job progress in real-time")
    status_parser.add_argument("--interval", type=float, default=1.0,
                             help="Update interval for watch mode (seconds)")
    status_parser.add_argument("--resources", "-r", action="store_true",
                             help="Show resource usage")
    status_parser.add_argument("--tasks", "-t", action="store_true",
                             help="List active tasks")
    
    # List jobs command
    list_parser = subparsers.add_parser("list", help="List all jobs")
    list_parser.add_argument("--all", action="store_true",
                           help="Show all jobs including completed")
    list_parser.add_argument("--failed", action="store_true",
                           help="Show failed jobs")
    
    # Cancel job command
    cancel_parser = subparsers.add_parser("cancel", help="Cancel a running job")
    cancel_parser.add_argument("job_id", help="Job ID to cancel")
    
    # Resource usage command
    resources_parser = subparsers.add_parser("resources", help="Show resource usage")
    resources_parser.add_argument("job_id", help="Job ID to show resources for")
    
    # Tasks command
    tasks_parser = subparsers.add_parser("tasks", help="List active tasks")
    tasks_parser.add_argument("job_id", help="Job ID to list tasks for")
    
    # Results command
    results_parser = subparsers.add_parser("results", help="Get job results")
    results_parser.add_argument("job_id", help="Job ID to get results for")
    
    args = parser.parse_args()
    
    if args.command == "submit":
        submit_job(args)
    elif args.command == "status":
        get_status(args)
    elif args.command == "list":
        list_jobs(args)
    elif args.command == "cancel":
        with grpc.insecure_channel(args.coordinator_host) as channel:
            stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
            cancel_job(stub, args.job_id)
    elif args.command == "resources":
        with grpc.insecure_channel(args.coordinator_host) as channel:
            stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
            show_resource_usage(stub, args.job_id)
    elif args.command == "tasks":
        with grpc.insecure_channel(args.coordinator_host) as channel:
            stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
            list_active_tasks(stub, args.job_id)
    elif args.command == "results":
        get_results(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
