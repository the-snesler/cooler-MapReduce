"""
Test script to verify map function execution and intermediate file creation.
This helps debug whether map tasks are working correctly.
"""

import os
import sys
import pickle
import unittest
from pathlib import Path

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from worker.task_executor import TaskExecutor


class TestMapExecution(unittest.TestCase):
    """Test map task execution and intermediate file creation."""
    
    def setUp(self):
        """Set up test environment."""
        # Use a test shared directory
        self.test_dir = os.path.join(os.path.dirname(__file__), '..', 'test_map_output')
        os.makedirs(self.test_dir, exist_ok=True)
        
        # Create subdirectories
        for subdir in ['input', 'intermediate', 'output', 'jobs']:
            os.makedirs(os.path.join(self.test_dir, subdir), exist_ok=True)
        
        self.executor = TaskExecutor(self.test_dir)
        
        # Create a simple test job file
        self.job_file = os.path.join(self.test_dir, 'jobs', 'test_wordcount.py')
        with open(self.job_file, 'w') as f:
            f.write("""
def map_fn(key, value):
    \"\"\"Simple word count map function.\"\"\"
    for word in value.split():
        yield (word.lower(), 1)

def reduce_fn(key, values):
    \"\"\"Simple word count reduce function.\"\"\"
    yield (key, sum(values))
""")
        
        # Create test input file
        self.input_file = os.path.join(self.test_dir, 'input', 'test.txt')
        with open(self.input_file, 'w') as f:
            f.write("hello world\n")
            f.write("hello mapreduce\n")
            f.write("test data\n")
    
    def tearDown(self):
        """Clean up test files."""
        import shutil
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_map_execution_creates_files(self):
        """Test that map execution creates intermediate files."""
        print("\n" + "="*60)
        print("TEST: Map Execution Creates Intermediate Files")
        print("="*60)
        
        job_id = "test_job"
        task_id = 0
        num_reduce_tasks = 2
        
        print(f"  Job ID: {job_id}")
        print(f"  Task ID: {task_id}")
        print(f"  Input file: {self.input_file}")
        print(f"  Job file: {self.job_file}")
        print(f"  Number of reduce tasks: {num_reduce_tasks}")
        
        # Execute map task
        intermediate_files = self.executor.execute_map(
            job_id=job_id,
            task_id=task_id,
            input_file=self.input_file,
            job_file_path=self.job_file,
            num_reduce_tasks=num_reduce_tasks
        )
        
        print(f"\n  ✓ Map execution completed!")
        print(f"  Intermediate files created: {len(intermediate_files)}")
        
        # Verify files were created
        self.assertGreater(len(intermediate_files), 0, "No intermediate files created!")
        
        for file_name in intermediate_files:
            file_path = os.path.join(self.executor.intermediate_dir, file_name)
            print(f"    - {file_name}")
            self.assertTrue(os.path.exists(file_path), f"File {file_name} does not exist!")
            self.assertGreater(os.path.getsize(file_path), 0, f"File {file_name} is empty!")
        
        return intermediate_files
    
    def test_intermediate_file_content(self):
        """Test that intermediate files contain correct data."""
        print("\n" + "="*60)
        print("TEST: Intermediate File Content Verification")
        print("="*60)
        
        job_id = "test_job"
        task_id = 0
        num_reduce_tasks = 2
        
        # Execute map task
        intermediate_files = self.executor.execute_map(
            job_id=job_id,
            task_id=task_id,
            input_file=self.input_file,
            job_file_path=self.job_file,
            num_reduce_tasks=num_reduce_tasks
        )
        
        print(f"  Checking {len(intermediate_files)} intermediate files...")
        
        all_data = []
        for file_name in intermediate_files:
            file_path = os.path.join(self.executor.intermediate_dir, file_name)
            
            # Load and inspect the pickled data
            with open(file_path, 'rb') as f:
                data = pickle.load(f)
            
            print(f"\n  File: {file_name}")
            print(f"    Size: {os.path.getsize(file_path)} bytes")
            print(f"    Records: {len(data)}")
            print(f"    Sample data (first 5): {data[:5] if len(data) > 5 else data}")
            
            # Verify data structure
            self.assertIsInstance(data, list, f"File {file_name} should contain a list")
            for item in data:
                self.assertIsInstance(item, tuple, f"Each item in {file_name} should be a tuple")
                self.assertEqual(len(item), 2, f"Each tuple in {file_name} should have 2 elements (key, value)")
            
            all_data.extend(data)
        
        print(f"\n  Total key-value pairs: {len(all_data)}")
        
        # Verify we got expected words
        keys = [key for key, value in all_data]
        expected_words = ['hello', 'world', 'hello', 'mapreduce', 'test', 'data']
        
        print(f"  Unique keys: {set(keys)}")
        print(f"  Expected keys: {set(expected_words)}")
        
        # Check that we have the expected words (case-insensitive)
        keys_lower = [k.lower() for k in keys]
        for word in expected_words:
            self.assertIn(word.lower(), keys_lower, f"Expected word '{word}' not found in map output!")
        
        print(f"\n  ✓ All expected words found in intermediate files!")
    
    def test_partitioning(self):
        """Test that map output is correctly partitioned."""
        print("\n" + "="*60)
        print("TEST: Map Output Partitioning")
        print("="*60)
        
        job_id = "test_job"
        task_id = 0
        num_reduce_tasks = 3  # Use 3 partitions to test partitioning
        
        # Execute map task
        intermediate_files = self.executor.execute_map(
            job_id=job_id,
            task_id=task_id,
            input_file=self.input_file,
            job_file_path=self.job_file,
            num_reduce_tasks=num_reduce_tasks
        )
        
        print(f"  Number of partitions: {num_reduce_tasks}")
        print(f"  Intermediate files created: {len(intermediate_files)}")
        
        # Load all partitions
        partitions = {}
        for file_name in intermediate_files:
            # Extract partition ID from filename: job_id_map_task_id_part_partition_id.pickle
            parts = file_name.split('_part_')
            if len(parts) == 2:
                partition_id = int(parts[1].split('.')[0])
                file_path = os.path.join(self.executor.intermediate_dir, file_name)
                with open(file_path, 'rb') as f:
                    partitions[partition_id] = pickle.load(f)
        
        print(f"\n  Partition distribution:")
        total_records = 0
        for partition_id in sorted(partitions.keys()):
            records = len(partitions[partition_id])
            total_records += records
            print(f"    Partition {partition_id}: {records} records")
        
        print(f"  Total records across all partitions: {total_records}")
        
        # Verify partitioning: same keys should go to same partition
        key_to_partition = {}
        for partition_id, data in partitions.items():
            for key, value in data:
                if key not in key_to_partition:
                    key_to_partition[key] = partition_id
                else:
                    # Same key should be in same partition
                    self.assertEqual(
                        key_to_partition[key], 
                        partition_id,
                        f"Key '{key}' appears in multiple partitions! (expected {key_to_partition[key]}, found {partition_id})"
                    )
        
        print(f"  ✓ Partitioning is consistent (same keys go to same partition)")
        print(f"  Unique keys: {len(key_to_partition)}")


def inspect_intermediate_files(shared_dir: str, job_id: str = None):
    """
    Utility function to inspect intermediate files in the shared directory.
    
    Usage:
        inspect_intermediate_files('/path/to/shared', 'job_123')
    """
    intermediate_dir = os.path.join(shared_dir, 'intermediate')
    
    if not os.path.exists(intermediate_dir):
        print(f"Intermediate directory does not exist: {intermediate_dir}")
        return
    
    # Find all pickle files
    if job_id:
        pattern = f"{job_id}_map_*_part_*.pickle"
    else:
        pattern = "*_map_*_part_*.pickle"
    
    import glob
    files = glob.glob(os.path.join(intermediate_dir, pattern))
    
    if not files:
        print(f"No intermediate files found matching pattern: {pattern}")
        return
    
    print(f"\n{'='*60}")
    print(f"Intermediate Files Inspection")
    print(f"{'='*60}")
    print(f"Directory: {intermediate_dir}")
    print(f"Found {len(files)} file(s)\n")
    
    for file_path in sorted(files):
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        
        print(f"File: {file_name}")
        print(f"  Size: {file_size} bytes")
        
        try:
            with open(file_path, 'rb') as f:
                data = pickle.load(f)
            
            print(f"  Records: {len(data)}")
            if len(data) > 0:
                print(f"  Sample (first 3): {data[:3]}")
                print(f"  Sample (last 3): {data[-3:]}")
                
                # Show unique keys
                keys = [k for k, v in data]
                unique_keys = set(keys)
                print(f"  Unique keys: {len(unique_keys)}")
                if len(unique_keys) <= 10:
                    print(f"    {sorted(unique_keys)}")
        except Exception as e:
            print(f"  ERROR reading file: {e}")
        
        print()


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Test map execution and inspect intermediate files')
    parser.add_argument('--inspect', action='store_true', help='Inspect existing intermediate files')
    parser.add_argument('--shared-dir', type=str, default='shared', help='Shared directory path')
    parser.add_argument('--job-id', type=str, help='Job ID to filter files (for inspect mode)')
    
    args = parser.parse_args()
    
    if args.inspect:
        # Inspect mode
        inspect_intermediate_files(args.shared_dir, args.job_id)
    else:
        # Run tests
        unittest.main(verbosity=2)


