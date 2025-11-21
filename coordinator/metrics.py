"""
Performance metrics collection for MapReduce jobs.
"""

import time
import json
from dataclasses import dataclass, asdict


@dataclass
class JobMetrics:
    """Metrics for a single MapReduce job execution."""

    job_id: str
    start_time: float
    end_time: float
    map_phase_start: float
    map_phase_end: float
    reduce_phase_start: float
    reduce_phase_end: float
    num_map_tasks: int
    num_reduce_tasks: int
    use_combiner: bool
    input_size_bytes: int
    intermediate_size_bytes: int
    output_size_bytes: int
    combiner_reduction_ratio: float = 0.0

    @property
    def total_time_seconds(self) -> float:
        """Total job execution time in seconds."""
        return self.end_time - self.start_time

    @property
    def map_phase_time_seconds(self) -> float:
        """Map phase execution time in seconds."""
        return self.map_phase_end - self.map_phase_start

    @property
    def reduce_phase_time_seconds(self) -> float:
        """Reduce phase execution time in seconds."""
        return self.reduce_phase_end - self.reduce_phase_start

    def to_dict(self) -> dict:
        """Convert metrics to dictionary."""
        return asdict(self)

    def save_to_file(self, filepath: str):
        """Save metrics to JSON file."""
        with open(filepath, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)


class MetricsCollector:
    """Collects and manages metrics for MapReduce jobs."""

    def __init__(self):
        self.job_metrics = {}

    def start_job(self, job_id: str, num_map_tasks: int, num_reduce_tasks: int,
                  use_combiner: bool, input_path: str):
        """Initialize metrics tracking for a new job."""
        import os

        input_size = os.path.getsize(input_path) if os.path.exists(input_path) else 0

        self.job_metrics[job_id] = JobMetrics(
            job_id=job_id,
            start_time=time.time(),
            end_time=0,
            map_phase_start=time.time(),
            map_phase_end=0,
            reduce_phase_start=0,
            reduce_phase_end=0,
            num_map_tasks=num_map_tasks,
            num_reduce_tasks=num_reduce_tasks,
            use_combiner=use_combiner,
            input_size_bytes=input_size,
            intermediate_size_bytes=0,
            output_size_bytes=0
        )

    def end_map_phase(self, job_id: str):
        """Mark the end of the map phase."""
        if job_id in self.job_metrics:
            self.job_metrics[job_id].map_phase_end = time.time()

    def start_reduce_phase(self, job_id: str, intermediate_dir: str):
        """Mark the start of the reduce phase and calculate intermediate data size."""
        import os
        import glob

        if job_id in self.job_metrics:
            self.job_metrics[job_id].reduce_phase_start = time.time()

            # Calculate intermediate data size
            pattern = f"{intermediate_dir}/{job_id}/map-*-reduce-*.txt"
            intermediate_files = glob.glob(pattern)
            intermediate_size = sum(os.path.getsize(f) for f in intermediate_files if os.path.exists(f))
            self.job_metrics[job_id].intermediate_size_bytes = intermediate_size

            # Calculate combiner reduction ratio
            input_size = self.job_metrics[job_id].input_size_bytes
            if input_size > 0:
                self.job_metrics[job_id].combiner_reduction_ratio = \
                    1.0 - (intermediate_size / input_size)

    def end_job(self, job_id: str, output_path: str):
        """Mark job completion and calculate output size."""
        import os
        import glob

        if job_id in self.job_metrics:
            self.job_metrics[job_id].reduce_phase_end = time.time()
            self.job_metrics[job_id].end_time = time.time()

            # Calculate output size
            output_files = glob.glob(f"{output_path}/part-*.txt")
            output_size = sum(os.path.getsize(f) for f in output_files if os.path.exists(f))
            self.job_metrics[job_id].output_size_bytes = output_size

    def get_metrics(self, job_id: str) -> JobMetrics:
        """Retrieve metrics for a specific job."""
        return self.job_metrics.get(job_id)
