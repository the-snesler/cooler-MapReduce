"""Client methods for detailed job monitoring and control."""

import time
import os
import psutil
from typing import Dict, List, Optional
from datetime import datetime

def format_duration(seconds: float) -> str:
    """Format duration in seconds to human readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds / 60)
    seconds = seconds % 60
    if minutes < 60:
        return f"{minutes}m {seconds:.1f}s"
    hours = int(minutes / 60)
    minutes = minutes % 60
    return f"{hours}h {minutes}m {seconds:.1f}s"

def format_progress_bar(completed: int, total: int, width: int = 40) -> str:
    """Create a progress bar string."""
    percentage = (completed / total) if total > 0 else 0
    filled = int(width * percentage)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {percentage:.1%}"

def monitor_job_progress(stub, job_id: str, interval: float = 1.0):
    """Monitor job progress with detailed stats and live updates."""
    last_status = None
    start_time = time.time()
    
    try:
        while True:
            request = coordinator_pb2.JobStatusRequest(job_id=job_id)
            status = stub.GetJobStatus(request)
            
            # Clear screen and reset cursor
            os.system('clear' if os.name == 'posix' else 'cls')
            
            # Job header
            print(f"Job ID: {status.job_id}")
            print(f"Status: {status.status}")
            print(f"Runtime: {format_duration(time.time() - start_time)}")
            print()
            
            # Phase progress
            if status.status != "COMPLETED" and status.status != "FAILED":
                if status.phase == "MAP":
                    print("Map Phase:")
                    print(format_progress_bar(status.completed_map_tasks, status.total_map_tasks))
                    print(f"Tasks: {status.completed_map_tasks}/{status.total_map_tasks}")
                elif status.phase == "REDUCE":
                    print("Reduce Phase:")
                    print(format_progress_bar(status.completed_reduce_tasks, status.total_reduce_tasks))
                    print(f"Tasks: {status.completed_reduce_tasks}/{status.total_reduce_tasks}")
            
            # Worker assignments
            if hasattr(status, 'worker_assignments'):
                print("\nActive Workers:")
                for worker in status.worker_assignments:
                    tasks = len(worker.current_tasks)
                    cpu = worker.cpu_usage
                    print(f"  {worker.worker_id}: {tasks} tasks, CPU {cpu}%")
            
            # Error reporting
            if status.error_message:
                print("\nErrors:")
                print(f"  {status.error_message}")
            
            if status.status in ["COMPLETED", "FAILED"]:
                break
                
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\nStopped monitoring. Job is still running.")
    except Exception as e:
        print(f"\nError monitoring job: {e}")

def cancel_job(stub, job_id: str):
    """Cancel a running job."""
    try:
        request = coordinator_pb2.JobCancelRequest(job_id=job_id)
        response = stub.CancelJob(request)
        if response.success:
            print(f"Successfully cancelled job {job_id}")
        else:
            print(f"Failed to cancel job: {response.error_message}")
    except Exception as e:
        print(f"Error cancelling job: {e}")

def list_active_tasks(stub, job_id: str):
    """List all currently active tasks for a job."""
    try:
        request = coordinator_pb2.JobTasksRequest(job_id=job_id)
        response = stub.ListActiveTasks(request)
        
        print(f"Active tasks for job {job_id}:")
        print(f"Phase: {response.current_phase}")
        
        if response.map_tasks:
            print("\nMap Tasks:")
            for task in response.map_tasks:
                print(f"  Task {task.task_id}:")
                print(f"    Worker: {task.worker_id}")
                print(f"    Status: {task.status}")
                print(f"    Progress: {task.progress:.1%}")
                
        if response.reduce_tasks:
            print("\nReduce Tasks:")
            for task in response.reduce_tasks:
                print(f"  Task {task.task_id}:")
                print(f"    Worker: {task.worker_id}")
                print(f"    Status: {task.status}")
                print(f"    Progress: {task.progress:.1%}")
                
    except Exception as e:
        print(f"Error listing tasks: {e}")

def show_resource_usage(stub, job_id: str):
    """Show detailed resource usage for a job."""
    try:
        request = coordinator_pb2.JobResourceRequest(job_id=job_id)
        response = stub.GetResourceUsage(request)
        
        print(f"Resource usage for job {job_id}:")
        print(f"Total CPU Time: {format_duration(response.total_cpu_time)}")
        print(f"Peak Memory: {response.peak_memory_mb:.1f} MB")
        
        print("\nWorker Resource Usage:")
        for worker in response.worker_stats:
            print(f"\n  Worker {worker.worker_id}:")
            print(f"    CPU Usage: {worker.cpu_usage:.1f}%")
            print(f"    Memory Usage: {worker.memory_mb:.1f} MB")
            print(f"    Network I/O: {worker.network_io_mb:.1f} MB")
            print(f"    Disk I/O: {worker.disk_io_mb:.1f} MB")
            
    except Exception as e:
        print(f"Error getting resource usage: {e}")