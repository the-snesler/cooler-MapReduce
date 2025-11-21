#!/usr/bin/env python3
"""
Job Manager for MapReduce Coordinator
Handles job state management, task generation, and progress tracking
"""

import threading
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Dict, Optional
import time
import os
import glob


class JobStatus(Enum):
    """Status of a MapReduce job"""
    PENDING = "pending"
    MAP_PHASE = "map_phase"
    SHUFFLE_PHASE = "shuffle_phase"
    REDUCE_PHASE = "reduce_phase"
    COMPLETED = "completed"
    FAILED = "failed"


class TaskStatus(Enum):
    """Status of individual map or reduce tasks"""
    PENDING = "pending"
    ASSIGNED = "assigned"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class MapTask:
    """Represents a single map task"""
    task_id: int
    input_path: str
    start_offset: int
    end_offset: int
    status: TaskStatus = TaskStatus.PENDING
    assigned_worker: Optional[str] = None


@dataclass
class ReduceTask:
    """Represents a single reduce task"""
    task_id: int
    partition_id: int
    intermediate_files: List[str] = field(default_factory=list)
    status: TaskStatus = TaskStatus.PENDING
    assigned_worker: Optional[str] = None


@dataclass
class Job:
    """Represents a complete MapReduce job"""
    job_id: str
    input_path: str
    output_path: str
    map_reduce_file: str
    num_map_tasks: int
    num_reduce_tasks: int
    use_combiner: bool
    status: JobStatus = JobStatus.PENDING
    map_tasks: List[MapTask] = field(default_factory=list)
    reduce_tasks: List[ReduceTask] = field(default_factory=list)
    start_time: float = 0.0
    end_time: float = 0.0


class JobManager:
    """Manages all MapReduce jobs and their lifecycle"""

    def __init__(self):
        self.jobs: Dict[str, Job] = {}
        self.lock = threading.Lock()

    def create_job(self, job_spec) -> Job:
        """Create new job from specification"""
        with self.lock:
            job = Job(
                job_id=job_spec.job_id,
                input_path=job_spec.input_path,
                output_path=job_spec.output_path,
                map_reduce_file=job_spec.map_reduce_file,
                num_map_tasks=job_spec.num_map_tasks,
                num_reduce_tasks=job_spec.num_reduce_tasks,
                use_combiner=job_spec.use_combiner,
                start_time=time.time()
            )
            self.jobs[job.job_id] = job
            return job

    def generate_map_tasks(self, job: Job) -> List[MapTask]:
        """Split input file into M map tasks"""
        file_size = os.path.getsize(job.input_path)
        chunk_size = file_size // job.num_map_tasks

        map_tasks = []
        for i in range(job.num_map_tasks):
            start = i * chunk_size
            end = file_size if i == job.num_map_tasks - 1 else (i + 1) * chunk_size

            task = MapTask(
                task_id=i,
                input_path=job.input_path,
                start_offset=start,
                end_offset=end
            )
            map_tasks.append(task)

        job.map_tasks = map_tasks
        return map_tasks

    def generate_reduce_tasks(self, job: Job) -> List[ReduceTask]:
        """Create R reduce tasks with intermediate file assignments"""
        reduce_tasks = []
        for partition_id in range(job.num_reduce_tasks):
            # Find all intermediate files for this partition
            intermediate_pattern = f"/mapreduce-data/intermediate/{job.job_id}/map-*-reduce-{partition_id}.txt"
            intermediate_files = sorted(glob.glob(intermediate_pattern))

            task = ReduceTask(
                task_id=partition_id,
                partition_id=partition_id,
                intermediate_files=intermediate_files
            )
            reduce_tasks.append(task)

        job.reduce_tasks = reduce_tasks
        return reduce_tasks

    def get_next_pending_map_task(self, job_id: str) -> Optional[MapTask]:
        """Get next pending map task for assignment"""
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return None

            for task in job.map_tasks:
                if task.status == TaskStatus.PENDING:
                    task.status = TaskStatus.ASSIGNED
                    return task
            return None

    def get_next_pending_reduce_task(self, job_id: str) -> Optional[ReduceTask]:
        """Get next pending reduce task for assignment"""
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return None

            for task in job.reduce_tasks:
                if task.status == TaskStatus.PENDING:
                    task.status = TaskStatus.ASSIGNED
                    return task
            return None

    def mark_map_task_completed(self, job_id: str, task_id: int):
        """Mark map task as completed"""
        with self.lock:
            job = self.jobs.get(job_id)
            if job and task_id < len(job.map_tasks):
                job.map_tasks[task_id].status = TaskStatus.COMPLETED

                # Check if all map tasks completed
                if all(t.status == TaskStatus.COMPLETED for t in job.map_tasks):
                    job.status = JobStatus.SHUFFLE_PHASE

    def mark_reduce_task_completed(self, job_id: str, task_id: int):
        """Mark reduce task as completed"""
        with self.lock:
            job = self.jobs.get(job_id)
            if job and task_id < len(job.reduce_tasks):
                job.reduce_tasks[task_id].status = TaskStatus.COMPLETED

                # Check if all reduce tasks completed
                if all(t.status == TaskStatus.COMPLETED for t in job.reduce_tasks):
                    job.status = JobStatus.COMPLETED
                    job.end_time = time.time()

    def get_job_status(self, job_id: str) -> Optional[Dict]:
        """Get current job status with progress"""
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return None

            total_tasks = len(job.map_tasks) + len(job.reduce_tasks)
            completed_tasks = sum(1 for t in job.map_tasks if t.status == TaskStatus.COMPLETED)
            completed_tasks += sum(1 for t in job.reduce_tasks if t.status == TaskStatus.COMPLETED)

            progress = int((completed_tasks / total_tasks * 100)) if total_tasks > 0 else 0

            return {
                'status': job.status.value,
                'progress': progress,
                'map_completed': sum(1 for t in job.map_tasks if t.status == TaskStatus.COMPLETED),
                'map_total': len(job.map_tasks),
                'reduce_completed': sum(1 for t in job.reduce_tasks if t.status == TaskStatus.COMPLETED),
                'reduce_total': len(job.reduce_tasks)
            }
