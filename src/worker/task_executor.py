"""
TaskExecutor, handles the actual execution of map and reduce tasks.
"""

import os
import sys
import glob
import pickle
import grpc
import psutil
import logging
import threading
from typing import Any, Dict, List, Tuple, Callable
from concurrent.futures import ThreadPoolExecutor

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import worker_pb2
import worker_pb2_grpc

class TaskExecutor:
    def __init__(self, shared_dir: str, max_workers: int = 4):
        """Initialize TaskExecutor with shared directory path."""
        self.shared_dir = shared_dir
        self.input_dir = os.path.join(shared_dir, 'input')
        self.intermediate_dir = os.path.join(shared_dir, 'intermediate')
        self.output_dir = os.path.join(shared_dir, 'output')
        self.jobs_dir = os.path.join(shared_dir, 'jobs')
        
        # Ensure directories exist
        for dir_path in [self.input_dir, self.intermediate_dir, self.output_dir, self.jobs_dir]:
            os.makedirs(dir_path, exist_ok=True)
            
        # Initialize thread pool for concurrent tasks
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.process = psutil.Process()
        
        # Task progress tracking
        self.task_progress: Dict[str, float] = {}
        self.task_states: Dict[str, str] = {}
        self._progress_lock = threading.Lock()

    def _get_task_key(self, job_id: str, task_id: int) -> str:
        """Generate unique key for task progress tracking."""
        return f"{job_id}_{task_id}"
        
    def get_task_progress(self, job_id: str, task_id: int) -> Tuple[float, str]:
        """Get current progress and state of a task."""
        task_key = self._get_task_key(job_id, task_id)
        with self._progress_lock:
            progress = self.task_progress.get(task_key, 0.0)
            state = self.task_states.get(task_key, "UNKNOWN")
        return progress, state

    def _update_progress(self, job_id: str, task_id: int, progress: float, state: str):
        """Update task progress and state."""
        task_key = self._get_task_key(job_id, task_id)
        with self._progress_lock:
            self.task_progress[task_key] = progress
            self.task_states[task_key] = state

    def cleanup_task(self, job_id: str, task_id: int):
        """Clean up intermediate files for completed tasks."""
        task_key = self._get_task_key(job_id, task_id)
        try:
            # Remove task progress tracking
            with self._progress_lock:
                self.task_progress.pop(task_key, None)
                self.task_states.pop(task_key, None)
                
            # Clean up intermediate files for map tasks
            pattern = f"{job_id}_map_{task_id}_part_*.pickle"
            for f in glob.glob(os.path.join(self.intermediate_dir, pattern)):
                os.remove(f)
                logging.info(f"Cleaned up intermediate file: {f}")
                
        except Exception as e:
            logging.warning(f"Cleanup failed for task {task_key}: {str(e)}")

    def _load_job_functions(self, job_id: str) -> Tuple[Callable, Callable]:
        """Load map and reduce functions from the job pickle file."""
        job_file = os.path.join(self.jobs_dir, f"{job_id}.pickle")
        with open(job_file, 'rb') as f:
            job_data = pickle.load(f)
        return job_data['map_fn'], job_data['reduce_fn']

    def get_memory_usage(self) -> float:
        """Get current memory usage in bytes."""
        return self.process.memory_info().rss
        
    def execute_map(self, job_id: str, task_id: int, input_file: str) -> List[str]:
        """Execute a map task and return paths to intermediate files."""
        task_key = self._get_task_key(job_id, task_id)
        self._update_progress(job_id, task_id, 0.0, "STARTING")
        
        try:
            # Load job functions
            self._update_progress(job_id, task_id, 0.1, "LOADING")
            map_fn, _ = self._load_job_functions(job_id)
            intermediate_files = []

            # Read input chunk
            input_path = os.path.join(self.input_dir, input_file)
            if not os.path.exists(input_path):
                raise FileNotFoundError(f"Input file not found: {input_file}")
            
            self._update_progress(job_id, task_id, 0.2, "READING")    
            with open(input_path, 'r') as f:
                input_data = f.read()

            # Apply map function
            self._update_progress(job_id, task_id, 0.4, "MAPPING")
            mapped_data = map_fn(input_data)

            # Write partitioned output
            total_partitions = len(mapped_data)
            for i, (partition, key_values) in enumerate(enumerate(mapped_data)):
                if not key_values:  # Skip empty partitions
                    continue
                    
                # Update progress for each partition
                progress = 0.4 + (0.6 * (i + 1) / max(total_partitions, 1))
                self._update_progress(job_id, task_id, progress, "WRITING")
                    
                outfile = f"{job_id}_map_{task_id}_part_{partition}.pickle"
                outpath = os.path.join(self.intermediate_dir, outfile)
                
                with open(outpath, 'wb') as f:
                    pickle.dump(key_values, f)
                intermediate_files.append(outfile)

            self._update_progress(job_id, task_id, 1.0, "COMPLETED")
            return intermediate_files

        except Exception as e:
            self._update_progress(job_id, task_id, 0.0, "FAILED")
            logging.error(f"Map task failed - Job: {job_id}, Task: {task_id}")
            logging.error(f"Error details: {str(e)}")
            raise

        except Exception as e:
            logging.error(f"Map task failed - Job: {job_id}, Task: {task_id}")
            logging.error(f"Error details: {str(e)}")
            raise

    def execute_reduce(self, job_id: str, task_id: int, partition_id: int, 
                        shuffle_locations: List[Tuple[str, str]]) -> str: 
        """Execute a reduce task and return path to output file."""
        self._update_progress(job_id, task_id, 0.0, "STARTING")
        
        try:
            # Load job functions
            self._update_progress(job_id, task_id, 0.1, "LOADING")
            _, reduce_fn = self._load_job_functions(job_id)
            
            # --- SHUFFLE & MERGE PHASE: Network Data Fetch ---
            self._update_progress(job_id, task_id, 0.2, "SHUFFLING & COLLECTING")
            
            # 1. Initialize data structure
            merged_data: Dict[Any, List] = {}
            total_files = len(shuffle_locations)
            
            if total_files == 0:
                logging.warning(f"Reduce task {task_id} found no intermediate files.")
                # If no data, proceed with empty merged_data

            # 2. Network Fetch Loop (The Shuffle)
            for i, (worker_address, file_name) in enumerate(shuffle_locations):
                progress = 0.2 + (0.4 * (i + 1) / total_files)
                self._update_progress(job_id, task_id, progress, "FETCHING DATA")
                
                # 🚀 CALL THE REMOTE RPC TO FETCH FILE DATA (The Core Shuffle Logic)
                file_data = self._fetch_file_via_grpc(worker_address, file_name)
                
                # 3. Unpickle and Merge (The Sort/Merge)
                partition_data = pickle.loads(file_data)
                for key, value in partition_data:
                    if key not in merged_data:
                        merged_data[key] = []
                    merged_data[key].append(value)
                    
            # 4. Apply reduce function to merged data (REDUCE PHASE)
            self._update_progress(job_id, task_id, 0.7, "REDUCING")
            reduced_data = []
            
            # Sorting keys for canonical output
            for i, (key, values) in enumerate(sorted(merged_data.items())):
                progress = 0.7 + (0.2 * (i + 1) / max(len(merged_data), 1))
                self._update_progress(job_id, task_id, progress, "REDUCING")
                
                result = reduce_fn(key, values)
                reduced_data.append((key, result))

            # Write output
            self._update_progress(job_id, task_id, 0.9, "WRITING")
            outfile = f"{job_id}_reduce_{task_id}.out"
            outpath = os.path.join(self.output_dir, outfile)
            
            with open(outpath, 'w') as f:
                for key, value in reduced_data:
                    f.write(f"{key}\t{value}\n")

            self._update_progress(job_id, task_id, 1.0, "COMPLETED")
            return outfile

        except Exception as e:
            self._update_progress(job_id, task_id, 0.0, "FAILED")
            logging.error(f"Reduce task failed - Job: {job_id}, Task: {task_id}. Error: {e}")
            raise

    def _fetch_file_via_grpc(self, worker_address: str, file_name: str) -> bytes:
        """
        Helper method to fetch a single intermediate file from a remote worker via gRPC.
        
        This function only needs the address and filename; job/task context 
        is managed by the calling function (execute_reduce).
        """
        try:
            # 1. Correctly uses worker_address argument
            with grpc.insecure_channel(worker_address) as channel:
                stub = worker_pb2_grpc.WorkerServiceStub(channel)
                
                # 2. Correctly uses file_name argument
                request = worker_pb2.FileRequest(file_name=file_name)
                
                # Use a short timeout for network operations
                response = stub.FetchIntermediateFile(request, timeout=15) 
                
                # 3. Correctly returns the file data (bytes)
                if response.file_data:
                    return response.file_data
                else:
                    raise RuntimeError(f"Fetch failed: Empty data received for {file_name} from {worker_address}")

        except grpc.RpcError as e:
            # Log error using the context passed in arguments
            logging.error(f"gRPC error fetching file {file_name} from {worker_address}: {e.details()}")
            raise RuntimeError(f"Shuffle failure from {worker_address}: {e.details()}")