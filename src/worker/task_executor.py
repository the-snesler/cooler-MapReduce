"""
TaskExecutor, handles the actual execution of map and reduce tasks.
"""

import os
import glob
import pickle
import psutil
import logging
import threading
from typing import Any, Dict, List, Tuple, Callable
from concurrent.futures import ThreadPoolExecutor

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

    def execute_reduce(self, job_id: str, task_id: int, partition_id: int) -> str:
        """Execute a reduce task and return path to output file."""
        self._update_progress(job_id, task_id, 0.0, "STARTING")
        
        try:
            # Load job functions
            self._update_progress(job_id, task_id, 0.1, "LOADING")
            _, reduce_fn = self._load_job_functions(job_id)
            
            # Collect all intermediate files for this partition
            self._update_progress(job_id, task_id, 0.2, "COLLECTING")
            pattern = f"{job_id}_map_*_part_{partition_id}.pickle"
            intermediate_files = glob.glob(os.path.join(self.intermediate_dir, pattern))

            if not intermediate_files:
                msg = f"No intermediate files found for partition {partition_id}"
                logging.warning(msg)
                raise FileNotFoundError(msg)

            # Read and merge intermediate data
            self._update_progress(job_id, task_id, 0.3, "MERGING")
            merged_data: Dict[Any, List] = {}
            total_files = len(intermediate_files)
            for i, file_path in enumerate(intermediate_files):
                progress = 0.3 + (0.3 * (i + 1) / total_files)
                self._update_progress(job_id, task_id, progress, "READING")
                
                with open(file_path, 'rb') as f:
                    partition_data = pickle.load(f)
                    for key, value in partition_data:
                        if key not in merged_data:
                            merged_data[key] = []
                        merged_data[key].append(value)

            # Apply reduce function to merged data
            self._update_progress(job_id, task_id, 0.7, "REDUCING")
            reduced_data = []
            total_keys = len(merged_data)
            for i, (key, values) in enumerate(merged_data.items()):
                progress = 0.7 + (0.2 * (i + 1) / total_keys)
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
            logging.error(f"Reduce task failed - Job: {job_id}, Task: {task_id}")
            logging.error(f"Error details: {str(e)}")
            raise

        except Exception as e:
            logging.error(f"Reduce task failed - Job: {job_id}, Task: {task_id}")
            logging.error(f"Error details: {str(e)}")
            raise