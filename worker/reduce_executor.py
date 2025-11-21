#!/usr/bin/env python3
"""
Reduce Task Executor
Executes reduce tasks by reading intermediate data, grouping by key,
applying reduce functions, and writing final output
"""

import os
import json
import time
from collections import defaultdict
from function_loader import FunctionLoader


class ReduceExecutor:
    """Executes a single reduce task"""

    def __init__(self, task_id: int, partition_id: int, intermediate_files: list,
                 map_reduce_file: str, output_path: str, job_id: str):
        """
        Initialize the reduce executor

        Args:
            task_id: Unique ID for this reduce task
            partition_id: Partition ID this reduce task is responsible for
            intermediate_files: List of intermediate file paths to read
            map_reduce_file: Path to user's map/reduce Python file
            output_path: Directory path where final output should be written
            job_id: Unique job identifier
        """
        self.task_id = task_id
        self.partition_id = partition_id
        self.intermediate_files = intermediate_files
        self.map_reduce_file = map_reduce_file
        self.output_path = output_path
        self.job_id = job_id
        self.loader = FunctionLoader(map_reduce_file)

    def execute(self) -> dict:
        """
        Execute the reduce task

        Returns:
            Dictionary with 'success', 'execution_time_ms', and 'error_message' fields
        """
        start_time = time.time()

        try:
            print(f"Reduce task {self.task_id}: Loading reduce function")
            # Load reduce function
            reduce_func = self.loader.get_reduce_function()

            print(f"Reduce task {self.task_id}: Reading and grouping intermediate data")
            # Read and group intermediate data by key
            key_groups = self._read_and_group_intermediate()
            print(f"Reduce task {self.task_id}: Grouped {len(key_groups)} unique keys")

            # Apply reduce function to each key group
            print(f"Reduce task {self.task_id}: Applying reduce function")
            results = []
            for key in sorted(key_groups.keys()):  # Sort by key for deterministic output
                values = key_groups[key]
                for out_key, out_value in reduce_func(key, values):
                    results.append((out_key, out_value))

            print(f"Reduce task {self.task_id}: Generated {len(results)} output pairs")

            # Write final output
            print(f"Reduce task {self.task_id}: Writing output")
            self._write_output(results)

            execution_time = int((time.time() - start_time) * 1000)
            print(f"Reduce task {self.task_id}: Completed in {execution_time}ms")

            return {
                'success': True,
                'execution_time_ms': execution_time,
                'error_message': ''
            }

        except Exception as e:
            execution_time = int((time.time() - start_time) * 1000)
            error_msg = f"Reduce task {self.task_id} failed: {str(e)}"
            print(error_msg)
            return {
                'success': False,
                'execution_time_ms': execution_time,
                'error_message': str(e)
            }

    def _read_and_group_intermediate(self) -> dict:
        """
        Read all intermediate files and group by key

        Returns:
            Dictionary mapping key (as string) to list of values
        """
        key_groups = defaultdict(list)
        files_read = 0
        lines_processed = 0
        lines_skipped = 0

        for filepath in self.intermediate_files:
            if not os.path.exists(filepath):
                print(f"Reduce task {self.task_id}: Warning - file not found: {filepath}")
                continue

            files_read += 1

            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        record = json.loads(line)
                        key = record['key']
                        value = record['value']

                        # Convert key to string for consistent hashing/grouping
                        key_str = str(key)
                        key_groups[key_str].append(value)
                        lines_processed += 1

                    except (json.JSONDecodeError, KeyError) as e:
                        # Skip malformed lines
                        lines_skipped += 1
                        print(f"Reduce task {self.task_id}: Skipping malformed line in {filepath}: {e}")
                        continue

        print(f"Reduce task {self.task_id}: Read {files_read} files, processed {lines_processed} records, skipped {lines_skipped} malformed records")
        return key_groups

    def _write_output(self, results: list):
        """
        Write final reduce output

        Args:
            results: List of (key, value) tuples to write
        """
        # Create output directory
        output_dir = f"{self.output_path}"
        os.makedirs(output_dir, exist_ok=True)

        output_file = f"{output_dir}/part-{self.partition_id}.txt"

        with open(output_file, 'w', encoding='utf-8') as f:
            for key, value in results:
                f.write(f"{key}\t{value}\n")

        print(f"Reduce task {self.task_id}: Wrote output to {output_file}")
