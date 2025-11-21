#!/usr/bin/env python3
"""
Map Task Executor
Executes map tasks by reading input splits, applying map functions,
partitioning output, and writing intermediate files
"""

import os
import json
import time
from collections import defaultdict
from function_loader import FunctionLoader


class MapExecutor:
    """Executes a single map task"""

    def __init__(self, task_id: int, input_path: str, start_offset: int,
                 end_offset: int, num_reduce_tasks: int, map_reduce_file: str,
                 use_combiner: bool, job_id: str):
        """
        Initialize the map executor

        Args:
            task_id: Unique ID for this map task
            input_path: Path to input file
            start_offset: Byte offset where this task should start reading
            end_offset: Byte offset where this task should stop reading
            num_reduce_tasks: Number of reduce tasks (for partitioning)
            map_reduce_file: Path to user's map/reduce Python file
            use_combiner: Whether to apply combiner function
            job_id: Unique job identifier
        """
        self.task_id = task_id
        self.input_path = input_path
        self.start_offset = start_offset
        self.end_offset = end_offset
        self.num_reduce_tasks = num_reduce_tasks
        self.map_reduce_file = map_reduce_file
        self.use_combiner = use_combiner
        self.job_id = job_id
        self.loader = FunctionLoader(map_reduce_file)

    def execute(self) -> dict:
        """
        Execute the map task

        Returns:
            Dictionary with 'success', 'execution_time_ms', and 'error_message' fields
        """
        start_time = time.time()

        try:
            print(f"Map task {self.task_id}: Loading map function")
            # Load map function
            map_func = self.loader.get_map_function()

            print(f"Map task {self.task_id}: Reading input split")
            # Read input split
            key_values = self._read_input_split()

            print(f"Map task {self.task_id}: Processing {len(key_values)} key-value pairs")
            # Apply map function and partition output
            intermediate = defaultdict(list)
            for key, value in key_values:
                for out_key, out_value in map_func(key, value):
                    # Partition by reduce task using hash-based partitioning
                    partition = hash(str(out_key)) % self.num_reduce_tasks
                    intermediate[partition].append((out_key, out_value))

            print(f"Map task {self.task_id}: Generated {sum(len(v) for v in intermediate.values())} intermediate pairs")

            # Apply combiner if enabled
            if self.use_combiner:
                print(f"Map task {self.task_id}: Applying combiner")
                intermediate = self._apply_combiner(intermediate)
                print(f"Map task {self.task_id}: After combiner: {sum(len(v) for v in intermediate.values())} pairs")

            # Write intermediate files
            print(f"Map task {self.task_id}: Writing intermediate files")
            self._write_intermediate_files(intermediate)

            execution_time = int((time.time() - start_time) * 1000)
            print(f"Map task {self.task_id}: Completed in {execution_time}ms")

            return {
                'success': True,
                'execution_time_ms': execution_time,
                'error_message': ''
            }

        except Exception as e:
            execution_time = int((time.time() - start_time) * 1000)
            error_msg = f"Map task {self.task_id} failed: {str(e)}"
            print(error_msg)
            return {
                'success': False,
                'execution_time_ms': execution_time,
                'error_message': str(e)
            }

    def _read_input_split(self):
        """
        Read assigned portion of input file with line boundary alignment

        Returns:
            List of (line_number, line_content) tuples
        """
        key_values = []

        with open(self.input_path, 'r', encoding='utf-8', errors='ignore') as f:
            f.seek(self.start_offset)

            # Align to line boundary (except for first split)
            if self.start_offset > 0:
                f.readline()  # Skip partial line

            line_num = 0
            while f.tell() < self.end_offset:
                line = f.readline()
                if not line:
                    break
                key_values.append((line_num, line.strip()))
                line_num += 1

        return key_values

    def _apply_combiner(self, intermediate: dict) -> dict:
        """
        Apply combiner function to local map output

        Args:
            intermediate: Dictionary mapping partition_id to list of (key, value) pairs

        Returns:
            Dictionary with same structure but with combined values
        """
        combiner_func = self.loader.get_combiner_function()
        if not combiner_func:
            return intermediate

        combined = {}
        for partition, kv_pairs in intermediate.items():
            # Group by key
            key_groups = defaultdict(list)
            for k, v in kv_pairs:
                key_groups[k].append(v)

            # Apply combiner to each key group
            combined_pairs = []
            for key, values in key_groups.items():
                for out_key, out_value in combiner_func(key, values):
                    combined_pairs.append((out_key, out_value))

            combined[partition] = combined_pairs

        return combined

    def _write_intermediate_files(self, intermediate: dict):
        """
        Write intermediate key-value pairs to disk in JSON format

        Args:
            intermediate: Dictionary mapping partition_id to list of (key, value) pairs
        """
        # Create intermediate directory for this job
        intermediate_dir = f"/mapreduce-data/intermediate/{self.job_id}"
        os.makedirs(intermediate_dir, exist_ok=True)

        for partition, kv_pairs in intermediate.items():
            filename = f"{intermediate_dir}/map-{self.task_id}-reduce-{partition}.txt"

            with open(filename, 'w', encoding='utf-8') as f:
                for key, value in kv_pairs:
                    # Write as JSON for easy parsing
                    f.write(json.dumps({'key': key, 'value': value}) + '\n')
