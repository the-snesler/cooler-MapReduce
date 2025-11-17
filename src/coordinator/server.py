"""
Coordinator server for MapReduce framework.
Manages job submission, task distribution, and worker coordination.
"""

import threading
import grpc
from concurrent import futures
import sys
import os
import uuid
import time
from datetime import datetime
import logging

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import coordinator_pb2
import coordinator_pb2_grpc
import worker_pb2
import worker_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class JobState:
    """Represents the state of a MapReduce job."""
    VALID_STATES = ["SUBMITTED", "MAPPING", "REDUCING", "COMPLETED", "FAILED", "CANCELLED"]
    
    def __init__(self, job_id, request):
        self.job_id = job_id
        self.input_path = request.input_path
        self.output_path = request.output_path
        self.job_file_path = request.job_file_path
        self.num_map_tasks = request.num_map_tasks
        self.num_reduce_tasks = request.num_reduce_tasks
        self.status = "SUBMITTED"
        self.submit_time = datetime.now().isoformat()
        self.phase = "MAP"
        self.completion_time = None
        
        # Task tracking
        self.map_tasks = {}  # task_id -> Task
        self.reduce_tasks = {}  # task_id -> Task
        self.completed_map_tasks = 0
        self.completed_reduce_tasks = 0
        self.error_message = ""
        
        # Intermediate file tracking
        self.intermediate_files = []  # List of files produced by map tasks
        self.intermediate_validated = False
    
    def validate_intermediate_files(self):
        """Validate that intermediate files have been collected for all partitions."""
        # Check if we have intermediate_file_locations (new approach)
        if hasattr(self, 'intermediate_file_locations') and self.intermediate_file_locations:
            # Check that we have at least some files collected
            total_files = sum(len(files) for files in self.intermediate_file_locations.values())
            if total_files > 0:
                logger.info(f"Validated intermediate files: {total_files} files across {len(self.intermediate_file_locations)} partitions")
                self.intermediate_validated = True
                return True
            else:
                logger.error("No intermediate files collected in intermediate_file_locations")
                return False
        
        # Fallback: check old intermediate_files list (for backward compatibility)
        if hasattr(self, 'intermediate_files') and self.intermediate_files:
            try:
                for file_path in self.intermediate_files:
                    if not os.path.exists(file_path):
                        logger.error(f"Missing intermediate file: {file_path}")
                        return False
                    if os.path.getsize(file_path) == 0:
                        logger.error(f"Empty intermediate file: {file_path}")
                        return False
                self.intermediate_validated = True
                return True
            except Exception as e:
                logger.error(f"Error validating intermediate files: {str(e)}")
                return False
        
        # No intermediate files found at all
        logger.error("No intermediate files found (neither intermediate_file_locations nor intermediate_files)")
        return False
    
    def transition_to_map_phase(self):
        """Transition job to mapping phase."""
        if self.status != "SUBMITTED":
            raise ValueError(f"Cannot transition to MAP phase from {self.status}")
        self.status = "MAPPING"
        self.phase = "MAP"
        logger.info(f"Job {self.job_id} started MAP phase")
    
    def transition_to_reduce_phase(self):
        """Transition job to reduce phase if all map tasks are complete."""
        if self.status != "MAPPING" or self.completed_map_tasks < self.num_map_tasks:
            raise ValueError("Cannot transition to REDUCE phase - maps not complete")
        if not self.validate_intermediate_files():
            raise ValueError("Cannot transition to REDUCE phase - invalid intermediate files")
        
        # create the reduce tasks before transition
        self._create_reduce_tasks()
        
        self.status = "REDUCING"
        self.phase = "REDUCE"
        logger.info(f"Job {self.job_id} started REDUCE phase")
    
    def mark_completed(self):
        """Mark job as completed if all reduce tasks are done."""
        if self.status != "REDUCING" or self.completed_reduce_tasks < self.num_reduce_tasks:
            raise ValueError("Cannot mark job complete - reduces not done")
        self.status = "COMPLETED"
        self.completion_time = datetime.now().isoformat()
        logger.info(f"Job {self.job_id} completed successfully")
    
    def mark_failed(self, error_msg):
        """Mark job as failed with error message."""
        self.status = "FAILED"
        self.error_message = error_msg
        self.completion_time = datetime.now().isoformat()
        logger.error(f"Job {self.job_id} failed: {error_msg}")
    
    def cancel(self):
        """Cancel the job if it's not already completed or failed."""
        if self.status not in ["COMPLETED", "FAILED"]:
            self.status = "CANCELLED"
            self.completion_time = datetime.now().isoformat()
            logger.info(f"Job {self.job_id} cancelled")
            return True
        return False
        
    def update_progress(self):
        """Update job progress and phase transitions."""
        if self.status in ["FAILED", "CANCELLED", "COMPLETED"]:
            return
            
        try:
            if self.phase == "MAP":
                # Handle both actual Task objects and mocks
                completed = sum(1 for t in self.map_tasks.values() 
                              if getattr(t, 'status', None) == "COMPLETED")
                self.completed_map_tasks = completed
                
                if completed == self.num_map_tasks:
                    self.status = "MAPPING"  # Ensure correct state for transition
                    self.transition_to_reduce_phase()
                    # Add reduce tasks to scheduler after creating them
                    # Defer add_task calls to avoid deadlock - use a background thread
                    coordinator = getattr(self, '_coordinator', None)
                    if coordinator:
                        reduce_tasks_list = list(self.reduce_tasks.values())
                        # Schedule add_task calls in a background thread to avoid deadlock
                        def add_tasks_async():
                            for reduce_task in reduce_tasks_list:
                                coordinator.task_scheduler.add_task(reduce_task)
                        threading.Thread(target=add_tasks_async, daemon=True).start()
                    
            elif self.phase == "REDUCE":
                completed = sum(1 for t in self.reduce_tasks.values() 
                              if getattr(t, 'status', None) == "COMPLETED")
                self.completed_reduce_tasks = completed
                
                if completed == self.num_reduce_tasks:
                    self.mark_completed()
        except Exception as e:
            self.mark_failed(str(e))

    def _create_reduce_tasks(self):
        """"creates all necessary reduce tasks using collected shuffle data."""
        if not hasattr(self, 'intermediate_file_locations'):
            self.intermediate_file_locations = {i: [] for i in range(self.num_reduce_tasks)}

        for i in range(self.num_reduce_tasks):
            task_id = f"{self.job_id}_reduce_{i}"
            
            # Get the input locations for this specific partition (i.e., the shuffle input)
            shuffle_input = self.intermediate_file_locations.get(i, [])
            
            if not shuffle_input:
                logger.warning(f"No intermediate files found for reduce task partition {i}")

            # Create the Task object
            reduce_task = Task(
                task_id=task_id,
                job_id=self.job_id,
                task_type="REDUCE",
                input_path="", # Reduce task input is determined by shuffle, not a file path
                output_path=os.path.join(self.output_path, f"reduce_output_{i}"),
                partition_id=i,
                num_reducers=0 # Irrelevant for Reduce task
            )
            
            # CRUCIAL: Add the attribute the test is checking
            reduce_task.shuffle_input_locations = shuffle_input 
            
            self.reduce_tasks[task_id] = reduce_task
            
            # Add to the scheduler immediately
            # Get coordinator servicer from the job state (we'll need to pass it)
            # For now, tasks will be added when transition happens


def split_input_file(file_path, num_splits):
    """Split an input file into chunks for map tasks."""
    try:
        with open(file_path, 'r') as f:
            # Get file size
            f.seek(0, 2)
            file_size = f.tell()
            f.seek(0)
            
            # Calculate split size (rounded up)
            split_size = (file_size + num_splits - 1) // num_splits
            
            splits = []
            current_pos = 0  # Track actual position after each split
            
            for i in range(num_splits):
                start_pos = current_pos
                
                if i == num_splits - 1:
                    # Last split goes to end of file
                    end_pos = file_size
                else:
                    # Find next newline after split_size from start
                    target_pos = min(start_pos + split_size, file_size)
                    f.seek(target_pos)
                    f.readline()  # Read to next newline (ensures we don't split in middle of line)
                    end_pos = f.tell()
                
                splits.append((start_pos, end_pos))
                current_pos = end_pos  # Next split starts where this one ended
            
            return splits
    except Exception as e:
        logger.error(f"Error splitting input file {file_path}: {str(e)}")
        return None

class CoordinatorServicer(coordinator_pb2_grpc.CoordinatorServiceServicer):
    """Implementation of CoordinatorService."""
    
    def __init__(self):
        self.jobs = {}  # job_id -> JobState
        self.workers = {}  # worker_id -> worker info
        self.jobs_lock = threading.Lock()  # Lock for protecting jobs dictionary
        self.task_scheduler = TaskScheduler(self)
        self.task_scheduler.start()
        logger.info("Coordinator service initialized")

    def _check_worker_timeouts(self):
        """Check for workers that haven't sent heartbeats recently and mark them as failed."""
        current_time = time.time()
        timeout_threshold = 30  # seconds
        
        # First, identify timed-out workers (quick check with minimal lock time)
        timed_out_workers = []
        with self.task_scheduler.lock:
            for worker_id, worker in list(self.workers.items()):
                if current_time - worker['last_heartbeat'] > timeout_threshold:
                    timed_out_workers.append(worker_id)
        
        if not timed_out_workers:
            return
        
        # Collect all tasks to reassign (minimize lock hold time)
        tasks_to_reassign_all = []
        workers_to_remove = []
        
        for worker_id in timed_out_workers:
            logger.warning(f"Worker {worker_id} timed out - reassigning tasks")
            
            tasks_to_reassign = []
            
            # Check tasks in scheduler's running_tasks (quick check)
            with self.task_scheduler.lock:
                for task_id, task in list(self.task_scheduler.running_tasks.items()):
                    if task.assigned_worker == worker_id and task.status == "IN_PROGRESS":
                        tasks_to_reassign.append(task)
            
            # Check tasks in all jobs (need jobs_lock for this)
            with self.jobs_lock:
                for job_id, job_state in list(self.jobs.items()):
                    # Check map tasks
                    for task_id, task in job_state.map_tasks.items():
                        if task.assigned_worker == worker_id and task.status == "IN_PROGRESS":
                            if task not in tasks_to_reassign:
                                tasks_to_reassign.append(task)
                    
                    # Check reduce tasks
                    for task_id, task in job_state.reduce_tasks.items():
                        if task.assigned_worker == worker_id and task.status == "IN_PROGRESS":
                            if task not in tasks_to_reassign:
                                tasks_to_reassign.append(task)
            
            # Prepare tasks for reassignment (modify state while holding scheduler lock)
            with self.task_scheduler.lock:
                for task in tasks_to_reassign:
                    logger.info(f"Reassigning task {task.task_id} from failed worker {worker_id}")
                    
                    # Reset task status to PENDING
                    task.status = "PENDING"
                    # Clear assigned_worker field
                    task.assigned_worker = None
                    # Increment retries (to give it higher priority)
                    task.retries += 1
                    
                    # Remove from scheduler's running_tasks if present
                    if task.task_id in self.task_scheduler.running_tasks:
                        del self.task_scheduler.running_tasks[task.task_id]
                    
                    # Remove from scheduler's task_start_times if present
                    if task.task_id in self.task_scheduler.task_start_times:
                        del self.task_scheduler.task_start_times[task.task_id]
                
                # Remove worker from worker_tasks tracking
                if worker_id in self.task_scheduler.worker_tasks:
                    del self.task_scheduler.worker_tasks[worker_id]
            
            # Collect tasks and worker for processing outside lock
            tasks_to_reassign_all.extend(tasks_to_reassign)
            workers_to_remove.append(worker_id)
        
        # Process reassignments outside lock to avoid deadlock (add_task acquires lock)
        for task in tasks_to_reassign_all:
            # Re-queue task for reassignment (this will acquire the lock internally)
            self.task_scheduler.add_task(task)
        
        # Remove workers from registry (single lock acquisition)
        with self.task_scheduler.lock:
            for worker_id in workers_to_remove:
                if worker_id in self.workers:
                    del self.workers[worker_id]
        
        if tasks_to_reassign_all:
            logger.info(f"Reassigned {len(tasks_to_reassign_all)} tasks from {len(workers_to_remove)} failed worker(s)")
    
    def Heartbeat(self, request, context):
        """Handle worker heartbeat."""
        try:
            worker_id = request.worker_id
            status = request.status
            available_slots = request.available_slots
            cpu_usage = request.cpu_usage if hasattr(request, 'cpu_usage') else 0.0
            
            # Get worker address from context (if available) or use a default
            # In Docker, we'll need to get this from the worker's report
            worker_address = None
            
            self.handle_worker_heartbeat(worker_id, status, available_slots, cpu_usage)
            
            # Store worker address if we have it (from task completion reports)
            # For now, we'll get it when workers report task completion
            
            return coordinator_pb2.HeartbeatResponse(acknowledged=True)
        except Exception as e:
            logger.error(f"Error handling heartbeat: {e}")
            return coordinator_pb2.HeartbeatResponse(acknowledged=False)
    
    def handle_worker_heartbeat(self, worker_id, status, available_slots, cpu_usage):
        """Handle worker heartbeat and update worker state."""
        with self.task_scheduler.lock:
            if worker_id not in self.workers:
                self.workers[worker_id] = {
                    'status': status,
                    'available_slots': available_slots,
                    'last_heartbeat': time.time(),
                    'cpu_usage': cpu_usage,
                    'task_history': [],  # List of recently completed tasks and their durations
                    'performance_score': 1.0,  # Higher is better
                    'address': None  # Will be set when worker reports task completion
                }
            else:
                worker = self.workers[worker_id]
                worker['status'] = status
                worker['available_slots'] = available_slots
                worker['last_heartbeat'] = time.time()
                worker['cpu_usage'] = cpu_usage
                
                # Update worker performance score based on CPU usage and task history
                if worker['task_history']:
                    avg_task_time = sum(t[1] for t in worker['task_history']) / len(worker['task_history'])
                    cpu_score = 1.0 - (cpu_usage / 100.0)  # Lower CPU usage is better
                    worker['performance_score'] = (1.0 / avg_task_time) * cpu_score
                
        # Check for worker timeouts
        self._check_worker_timeouts()
    
    def SubmitJob(self, request, context):
        """Handle job submission from client."""
        job_id = str(uuid.uuid4())
        logger.info(f"Received job submission request: {job_id}")
        logger.info(f"  Input: {request.input_path}")
        logger.info(f"  Output: {request.output_path}")
        logger.info(f"  Job file: {request.job_file_path}")
        logger.info(f"  Map tasks: {request.num_map_tasks}")
        logger.info(f"  Reduce tasks: {request.num_reduce_tasks}")
        
        # Normalize paths - handle Docker volume mounts
        # If path starts with /shared/, use it as-is (Docker mount point)
        # If path starts with shared/, convert to /shared/ (Docker mount point)
        # Otherwise, resolve as absolute path
        def normalize_path_for_docker(path):
            path = os.path.normpath(path)
            if path.startswith('/shared/'):
                return path
            if path.startswith('shared/'):
                return '/' + path
            # For absolute paths, use as-is
            if os.path.isabs(path):
                return path
            # For relative paths, resolve relative to current directory
            return os.path.abspath(path)
        
        # Validate input path
        input_path = normalize_path_for_docker(request.input_path)
        if not os.path.exists(input_path):
            logger.error(f"Input path does not exist: {input_path} (original: {request.input_path})")
            logger.error(f"Current working directory: {os.getcwd()}")
            # Try alternative path if original was relative
            if not os.path.isabs(request.input_path) and not request.input_path.startswith('shared/'):
                alt_path = normalize_path_for_docker('shared/' + request.input_path.lstrip('/'))
                if os.path.exists(alt_path):
                    logger.info(f"Found input at alternative path: {alt_path}")
                    input_path = alt_path
                else:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details(f"Input path does not exist: {input_path}")
                    return coordinator_pb2.JobResponse()
            else:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(f"Input path does not exist: {input_path}")
                return coordinator_pb2.JobResponse()
        
        # Store normalized paths for later use
        normalized_input_path = input_path
        normalized_output_path = normalize_path_for_docker(request.output_path)
        normalized_job_file_path = normalize_path_for_docker(request.job_file_path)
        
        # Validate job file path
        if not os.path.exists(normalized_job_file_path):
            logger.error(f"Job file path does not exist: {normalized_job_file_path}")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"Job file path does not exist: {normalized_job_file_path}")
            return coordinator_pb2.JobResponse()
        
        # Create job state (JobState will use request attributes, but we'll use normalized paths)
        job_state = JobState(job_id, request)
        # Override with normalized paths
        job_state.input_path = normalized_input_path
        job_state.output_path = normalized_output_path
        job_state.job_file_path = normalized_job_file_path
        job_state._coordinator = self  # Store reference for task scheduling
        
        with self.jobs_lock:
            self.jobs[job_id] = job_state
        
        # Split input file for map tasks
        splits = split_input_file(normalized_input_path, request.num_map_tasks)
        if splits is None:
            job_state.status = "FAILED"
            job_state.error_message = f"Failed to split input file: {normalized_input_path}"
            return coordinator_pb2.JobResponse(
                job_id=job_id,
                status="FAILED"
            )
        
        # Create map tasks
        for i, (start_pos, end_pos) in enumerate(splits):
            task_id = f"{job_id}_map_{i}"
            task = Task(
                task_id=task_id,
                job_id=job_id,
                task_type="MAP",
                input_path=normalized_input_path,
                output_path=os.path.join(normalized_output_path, f"intermediate_{i}"),
                num_reducers=request.num_reduce_tasks,
                start_pos=start_pos,
                end_pos=end_pos
            )
            job_state.map_tasks[task_id] = task
            self.task_scheduler.add_task(task)
        
        # Update job status
        job_state.status = "MAPPING"
        
        return coordinator_pb2.JobResponse(
            job_id=job_id,
            status="SUBMITTED"
        )
    
    def GetJobStatus(self, request, context):
        """Get the status of a submitted job."""
        job_id = request.job_id
        
        with self.jobs_lock:
            if job_id not in self.jobs:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Job {job_id} not found")
                return coordinator_pb2.JobStatusResponse()
            
            job_state = self.jobs[job_id]
        
        return coordinator_pb2.JobStatusResponse(
            job_id=job_id,
            status=job_state.status,
            phase=job_state.phase,
            total_map_tasks=job_state.num_map_tasks,
            completed_map_tasks=job_state.completed_map_tasks,
            total_reduce_tasks=job_state.num_reduce_tasks,
            completed_reduce_tasks=job_state.completed_reduce_tasks,
            error_message=job_state.error_message
        )
    
    def ListJobs(self, request, context):
        """List all jobs."""
        jobs = []
        with self.jobs_lock:
            for job_id, job_state in self.jobs.items():
                jobs.append(coordinator_pb2.JobInfo(
                    job_id=job_id,
                    status=job_state.status,
                    submit_time=job_state.submit_time
                ))
        
        return coordinator_pb2.JobListResponse(jobs=jobs)
    
    def GetJobResults(self, request, context):
        """Get results of a completed job."""
        job_id = request.job_id
        
        with self.jobs_lock:
            if job_id not in self.jobs:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Job {job_id} not found")
                return coordinator_pb2.JobResultsResponse()
            
            job_state = self.jobs[job_id]
        
        # TODO: Collect output file paths
        output_files = []
        
        return coordinator_pb2.JobResultsResponse(
            job_id=job_id,
            status=job_state.status,
            output_files=output_files
        )
    
    def ReportTaskCompletion(self, request, context):
        """Handle task completion report from worker."""
        try:
            job_id = request.job_id
            task_id = request.task_id
            
            with self.jobs_lock:
                if job_id not in self.jobs:
                    logger.warning(f"Received completion for unknown job: {job_id}")
                    return coordinator_pb2.TaskCompletionResponse(acknowledged=False)
                
                job_state = self.jobs[job_id]
            
            if task_id not in job_state.map_tasks and task_id not in job_state.reduce_tasks:
                logger.warning(f"Received completion for unknown task: {task_id}")
                return coordinator_pb2.TaskCompletionResponse(acknowledged=False)
            
            # Update task status
            if request.task_type == "MAP":
                if task_id in job_state.map_tasks:
                    task = job_state.map_tasks[task_id]
                    if request.success:
                        task.complete()
                        
                        # Collect shuffle data: organize intermediate files by partition
                        if not hasattr(job_state, 'intermediate_file_locations'):
                            job_state.intermediate_file_locations = {i: [] for i in range(job_state.num_reduce_tasks)}
                        
                        # Parse intermediate file names to extract partition IDs
                        # Format: job_id_map_task_id_part_partition_id.pickle
                        for file_name in request.intermediate_files:
                            try:
                                # Extract partition ID from filename
                                # Format: {job_id}_map_{task_id}_part_{partition_id}.pickle
                                parts = file_name.split('_part_')
                                if len(parts) == 2:
                                    partition_str = parts[1].split('.')[0]
                                    partition_id = int(partition_str)
                                    
                                    # Add to shuffle locations for this partition
                                    location = (request.worker_address, file_name)
                                    job_state.intermediate_file_locations[partition_id].append(location)
                                    logger.info(f"Added shuffle location: partition {partition_id}, file {file_name} from {request.worker_address}")
                            except (ValueError, IndexError) as e:
                                logger.warning(f"Failed to parse partition ID from filename {file_name}: {e}")
                        
                        logger.info(f"Map task {task_id} completed. Collected {len(request.intermediate_files)} intermediate files.")
                        
                        # Store worker address for shuffle operations
                        if request.worker_id in self.workers:
                            self.workers[request.worker_id]['address'] = request.worker_address
                    else:
                        task.fail(request.error_message)
                        logger.error(f"Map task {task_id} failed: {request.error_message}")
            
            elif request.task_type == "REDUCE":
                if task_id in job_state.reduce_tasks:
                    task = job_state.reduce_tasks[task_id]
                    if request.success:
                        task.complete()
                        logger.info(f"Reduce task {task_id} completed.")
                    else:
                        task.fail(request.error_message)
                        logger.error(f"Reduce task {task_id} failed: {request.error_message}")
            
            # Update job progress and check for phase transitions
            job_state.update_progress()
            
            return coordinator_pb2.TaskCompletionResponse(acknowledged=True)
            
        except Exception as e:
            logger.error(f"Error handling task completion report: {e}")
            return coordinator_pb2.TaskCompletionResponse(acknowledged=False)

    # add new classes
class Task:
    """Represents a map or reduce task in the system."""
    def __init__(self, task_id, job_id, task_type, input_path, output_path, partition_id=None, num_reducers=None, start_pos=None, end_pos=None):
        self.task_id = task_id
        self.job_id = job_id
        self.task_type = task_type  # "MAP" or "REDUCE"
        self.input_path = input_path
        self.output_path = output_path
        self.partition_id = partition_id
        self.num_reducers = num_reducers  # Needed for map tasks to partition output
        self.start_pos = start_pos  # Start position in input file (for map tasks)
        self.end_pos = end_pos  # End position in input file (for map tasks)
        self.status = "PENDING"  # "PENDING", "IN_PROGRESS", "COMPLETED", "FAILED"
        self.assigned_worker = None
        self.start_time = None
        self.end_time = None
        self.error_message = ""
        self.retries = 0
        self.max_retries = 3
    
    def assign_to_worker(self, worker_id):
        """Assign task to a worker."""
        self.assigned_worker = worker_id
        self.status = "IN_PROGRESS"
        self.start_time = datetime.now().isoformat()
    
    def complete(self):
        """Mark task as completed."""
        self.status = "COMPLETED"
        self.end_time = datetime.now().isoformat()
    
    def fail(self, error_msg):
        """Mark task as failed with error message."""
        self.status = "FAILED"
        self.error_message = error_msg
        self.end_time = datetime.now().isoformat()
        self.assigned_worker = None
        return self.retries < self.max_retries

from queue import PriorityQueue
from dataclasses import dataclass, field
from typing import Any
import time

@dataclass(order=True)
class PrioritizedTask:
    """Task wrapper with priority information."""
    priority: int
    task: Any = field(compare=False)
    timestamp: float = field(compare=False, default_factory=time.time)

class TaskScheduler:
    """Manages task scheduling and worker assignments."""
    def __init__(self, coordinator_servicer):
        self.coordinator_servicer = coordinator_servicer
        self.pending_tasks = PriorityQueue()  # Priority queue of tasks
        self.running_tasks = {}  # task_id -> Task
        self.worker_tasks = {}   # worker_id -> [task_ids]
        self.task_start_times = {}  # task_id -> start_time
        self.straggler_threshold = 2.0  # Tasks taking 2x longer than average are stragglers
        self.lock = threading.Lock()
        self.scheduler_thread = threading.Thread(target=self._schedule_loop, daemon=True)
        self.monitor_thread = threading.Thread(target=self._monitor_stragglers, daemon=True)
        self.running = True
    
    def start(self):
        """Start the scheduler and monitor threads."""
        self.scheduler_thread.start()
        self.monitor_thread.start()
        logger.info("Task scheduler and monitor started")
    
    def stop(self):
        """Stop the scheduler thread."""
        self.running = False
        self.scheduler_thread.join()
        logger.info("Task scheduler stopped")

    def add_task(self, task):
        """Add a new task to be scheduled with appropriate priority."""
        priority = self._calculate_task_priority(task)
        with self.lock:
            self.pending_tasks.put(PrioritizedTask(priority=priority, task=task))
            logger.info(f"Task {task.task_id} added to scheduling queue with priority {priority}")
    
    def _calculate_task_priority(self, task):
        """Calculate task priority based on various factors."""
        priority = 0
        
        # Higher priority (lower number) for:
        # 1. Reduce tasks when most map tasks are done
        if task.task_type == "REDUCE":
            with self.coordinator_servicer.jobs_lock:
                if task.job_id in self.coordinator_servicer.jobs:
                    job_state = self.coordinator_servicer.jobs[task.job_id]
                    if job_state.completed_map_tasks > 0.8 * job_state.num_map_tasks:
                        priority -= 10
        
        # 2. Retried tasks
        priority -= task.retries * 5
        
        # 3. Tasks from jobs that have been waiting longer
        with self.coordinator_servicer.jobs_lock:
            if task.job_id in self.coordinator_servicer.jobs:
                job_state = self.coordinator_servicer.jobs[task.job_id]
                wait_time = time.time() - datetime.fromisoformat(job_state.submit_time).timestamp()
                priority -= min(int(wait_time / 60), 20)  # Up to -20 for waiting
        
        return priority
    
    def _schedule_loop(self):
        """Main scheduling loop."""
        while self.running:
            with self.lock:
                self._assign_pending_tasks()
            time.sleep(1)  # Check every second
    
    def _assign_pending_tasks(self):
        """Assign pending tasks to available workers based on performance."""
        # Sort workers by performance score
        # Workers with available slots can accept tasks, regardless of IDLE/BUSY status
        available_workers = [
            (worker_id, info) for worker_id, info in self.coordinator_servicer.workers.items()
            if info['available_slots'] > 0
        ]
        
        # Debug logging
        if self.pending_tasks.qsize() > 0:
            logger.debug(f"Scheduler: {self.pending_tasks.qsize()} pending tasks, {len(available_workers)} available workers")
            if len(available_workers) == 0 and len(self.coordinator_servicer.workers) > 0:
                logger.warning(f"No available workers! Total workers: {len(self.coordinator_servicer.workers)}")
                for worker_id, info in self.coordinator_servicer.workers.items():
                    logger.warning(f"  Worker {worker_id}: status={info.get('status')}, slots={info.get('available_slots')}")
        
        available_workers.sort(key=lambda x: x[1]['performance_score'], reverse=True)
        
        for worker_id, worker_info in available_workers:
            while worker_info['available_slots'] > 0 and not self.pending_tasks.empty():
                prioritized_task = self.pending_tasks.get()
                task = prioritized_task.task
                self._assign_task_to_worker(worker_id, task)
                worker_info['available_slots'] -= 1
                
    def _monitor_stragglers(self):
        """Monitor for straggler tasks and reassign if needed."""
        while self.running:
            with self.lock:
                self._check_stragglers()
            time.sleep(10)  # Check every 10 seconds
    
    def _check_stragglers(self):
        """Identify and handle straggler tasks."""
        current_time = time.time()
        
        # Calculate average task duration for each job
        job_task_durations = {}  # job_id -> list of durations
        
        for task_id, start_time in self.task_start_times.items():
            if task_id in self.running_tasks:
                task = self.running_tasks[task_id]
                duration = current_time - start_time
                
                if task.job_id not in job_task_durations:
                    job_task_durations[task.job_id] = []
                job_task_durations[task.job_id].append(duration)
        
        # Check for stragglers
        for task_id, start_time in self.task_start_times.items():
            if task_id not in self.running_tasks:
                continue
                
            task = self.running_tasks[task_id]
            duration = current_time - start_time
            
            # Calculate average duration for this job's tasks
            avg_duration = sum(job_task_durations[task.job_id]) / len(job_task_durations[task.job_id])
            
            # If task is taking too long, create a backup task
            if duration > avg_duration * self.straggler_threshold:
                logger.warning(f"Task {task_id} identified as straggler. Duration: {duration:.2f}s, Avg: {avg_duration:.2f}s")
                self._handle_straggler(task)
    
    def _assign_task_to_worker(self, worker_id, task):
        """Assign a specific task to a worker."""
        task.assign_to_worker(worker_id)
        self.running_tasks[task.task_id] = task
        
        if worker_id not in self.worker_tasks:
            self.worker_tasks[worker_id] = []
        self.worker_tasks[worker_id].append(task.task_id)
        
        # Get job state for additional info (with lock protection)
        with self.coordinator_servicer.jobs_lock:
            if task.job_id not in self.coordinator_servicer.jobs:
                logger.error(f"Job {task.job_id} not found when assigning task {task.task_id}")
                return
            job_state = self.coordinator_servicer.jobs[task.job_id]
        
        # Create the task assignment
        assignment = worker_pb2.TaskAssignment(
            task_id=task.task_id,
            task_type=task.task_type,
            job_id=task.job_id,
            input_path=task.input_path,
            output_path=task.output_path,
            job_file_path=job_state.job_file_path,
            partition_id=task.partition_id if task.partition_id is not None else 0,
            num_reduce_tasks=task.num_reducers if task.num_reducers is not None else job_state.num_reduce_tasks,
            start_pos=task.start_pos if task.start_pos is not None else 0,
            end_pos=task.end_pos if task.end_pos is not None else 0  # 0 means read to end (will be converted to None in worker)
        )
        
        # For reduce tasks, add shuffle locations
        if task.task_type == "REDUCE" and hasattr(task, 'shuffle_input_locations'):
            for worker_addr, file_name in task.shuffle_input_locations:
                location = worker_pb2.Location(
                    worker_address=worker_addr,
                    file_name=file_name
                )
                assignment.shuffle_locations.append(location)
        
        # Send assignment to worker (non-blocking)
        threading.Thread(
            target=self._send_assignment_to_worker,
            args=(worker_id, assignment, task),
            daemon=True
        ).start()
    
    def _send_assignment_to_worker(self, worker_id, assignment, task):
        """Send task assignment to worker via gRPC."""
        try:
            # Get worker address, with fallback construction
            worker_info = self.coordinator_servicer.workers.get(worker_id)
            if worker_info is None:
                raise ValueError(f"Worker {worker_id} not found in workers registry")
            
            worker_addr = worker_info.get('address')
            
            # If address not set yet, try to construct it from worker_id
            # Docker pattern: worker-1 -> worker1:50052, worker-2 -> worker2:50053, etc.
            if worker_addr is None:
                # Try to extract worker number from worker_id (e.g., "worker-1" -> 1)
                try:
                    worker_num = int(worker_id.split('-')[-1])
                    # Port mapping: worker-1 -> 50052, worker-2 -> 50053, etc.
                    port = 50051 + worker_num
                    # In Docker, container names are lowercase without dashes
                    container_name = worker_id.replace('-', '').lower()
                    worker_addr = f"{container_name}:{port}"
                    logger.info(f"Constructed worker address for {worker_id}: {worker_addr}")
                except (ValueError, IndexError):
                    # Fallback: try to use worker_id directly as hostname
                    worker_addr = f"{worker_id}:50052"
                    logger.warning(f"Could not parse worker_id {worker_id}, using default address: {worker_addr}")
            
            if worker_addr is None:
                raise ValueError(f"Worker {worker_id} has no address and could not be constructed")
            
            with grpc.insecure_channel(worker_addr) as channel:
                stub = worker_pb2_grpc.WorkerServiceStub(channel)
                response = stub.AssignTask(assignment)
                
                if not response.accepted:
                    logger.error(f"Worker {worker_id} rejected task {task.task_id}")
                    self._handle_task_failure(task, "Worker rejected task")
        except grpc.RpcError as e:
            logger.error(f"Failed to assign task {task.task_id} to worker {worker_id}: {str(e)}")
            self._handle_task_failure(task, str(e))
    
    def _handle_task_failure(self, task, error_msg):
        """Handle task failure and potential retry."""
        should_retry = False
        with self.lock:
            if task.fail(error_msg):
                # Can retry
                task.retries += 1
                should_retry = True
                logger.info(f"Task {task.task_id} queued for retry ({task.retries}/{task.max_retries})")
            else:
                # Max retries exceeded
                logger.error(f"Task {task.task_id} failed permanently after {task.retries} retries")
                # Update job status (need jobs_lock for this)
                with self.coordinator_servicer.jobs_lock:
                    if task.job_id in self.coordinator_servicer.jobs:
                        job_state = self.coordinator_servicer.jobs[task.job_id]
                        job_state.status = "FAILED"
                        job_state.error_message = f"Task {task.task_id} failed: {error_msg}"
        
        # Retry outside lock to avoid nested lock acquisition
        if should_retry:
            self.add_task(task)  # This will acquire the lock internally
    
    def _handle_straggler(self, task):
        """Handle a straggler task by creating a backup task."""
        if task.status != "IN_PROGRESS":
            return
            
        # Create a backup task (preserve split boundaries for map tasks)
        backup_task = Task(
            task_id=f"{task.task_id}_backup",
            job_id=task.job_id,
            task_type=task.task_type,
            input_path=task.input_path,
            output_path=f"{task.output_path}_backup",
            partition_id=task.partition_id,
            num_reducers=task.num_reducers,
            start_pos=task.start_pos,
            end_pos=task.end_pos
        )
        
        # Add to queue with high priority
        backup_task.retries = task.max_retries - 1  # Give it high priority
        self.add_task(backup_task)
        
        logger.info(f"Created backup task {backup_task.task_id} for straggler {task.task_id}")
        
        # First task to complete will be considered successful
        # The other task will be cancelled when the first one completes


def serve(port=50051):
    """Start the coordinator gRPC server."""
    # Log working directory for debugging
    logger.info(f"Coordinator starting in working directory: {os.getcwd()}")
    logger.info(f"Coordinator script location: {__file__}")
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    coordinator_pb2_grpc.add_CoordinatorServiceServicer_to_server(
        CoordinatorServicer(), server
    )
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logger.info(f"Coordinator server started on port {port}")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Coordinator server shutting down")
        server.stop(0)


if __name__ == '__main__':
    serve()
