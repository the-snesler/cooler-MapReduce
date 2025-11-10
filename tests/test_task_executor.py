import os
import pickle
import unittest
import shutil
from src.worker.task_executor import TaskExecutor

# Define job functions at module level so they can be pickled
def sample_map_fn(text):
    """Map function for testing."""
    words = text.split()
    result = []
    for word in words:
        result.append((word.lower(), 1))
    return [result]  # Single partition for testing
    
def sample_reduce_fn(key, values):
    """Reduce function for testing."""
    return sum(values)

class TestTaskExecutor(unittest.TestCase):
    def setUp(self):
        # Create test directories
        self.test_dir = os.path.join(os.path.dirname(__file__), 'test_data')
        os.makedirs(self.test_dir, exist_ok=True)
        
        for subdir in ['input', 'intermediate', 'output', 'jobs']:
            os.makedirs(os.path.join(self.test_dir, subdir), exist_ok=True)
        
        self.executor = TaskExecutor(self.test_dir)
        
        # Save job functions
        job_data = {
            'map_fn': sample_map_fn,
            'reduce_fn': sample_reduce_fn
        }
        with open(os.path.join(self.test_dir, 'jobs', 'test_job.pickle'), 'wb') as f:
            pickle.dump(job_data, f)
            
        # Create test input
        with open(os.path.join(self.test_dir, 'input', 'test_input.txt'), 'w') as f:
            f.write("Hello World Hello MapReduce")

    def tearDown(self):
        """Clean up test directories after each test."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_map_execution(self):
        # Test map task execution
        result_files = self.executor.execute_map('test_job', 1, 'test_input.txt')
        
        self.assertEqual(len(result_files), 1)  # Single partition
        
        # Verify intermediate file content
        with open(os.path.join(self.test_dir, 'intermediate', result_files[0]), 'rb') as f:
            mapped_data = pickle.load(f)
            
        expected = [
            ('hello', 1),
            ('world', 1),
            ('hello', 1),
            ('mapreduce', 1)
        ]
        self.assertEqual(mapped_data, expected)

    def test_reduce_execution(self):
        """
        Test the Merge and Reduce logic using a mock Shuffle input.
        Note: This test no longer uses glob.glob; it feeds the file path directly 
        as the "shuffle location" to verify the internal aggregation logic.
        """
        # Create test intermediate data
        intermediate_data = [
            ('hello', 1),
            ('world', 1),
            ('hello', 1),
            ('mapreduce', 1)
        ]
        
        # Define the file name used in this test
        test_file_name = 'test_job_map_1_part_0.pickle'
        os.makedirs(os.path.join(self.test_dir, 'intermediate'), exist_ok=True)
        
        # 1. Create the local intermediate file
        with open(os.path.join(self.test_dir, 'intermediate', test_file_name), 'wb') as f:
            pickle.dump(intermediate_data, f)
        
        # 2. Define the mock shuffle locations pointing to the local file
        # NOTE: The execute_reduce logic will try to fetch this remotely.
        # We must MOCK the gRPC fetch method to read the local file instead.
        
        mock_locations = [
            ('127.0.0.1:0000', test_file_name) # Mock address, real file name
        ]
        
        # 3. MOCK the gRPC fetch helper to read the local file data
        def mock_fetch_file(self_executor, worker_address, file_name):
            # We need the full path here since the mock bypasses the network
            local_path = os.path.join(self_executor.intermediate_dir, file_name)
            with open(local_path, 'rb') as f:
                return f.read()

        # Use patch to temporarily replace the _fetch_file_via_grpc method
        with patch.object(self.executor, '_fetch_file_via_grpc', side_effect=mock_fetch_file) as mock_fetch:
            # Test reduce task execution with the required new argument
            output_file = self.executor.execute_reduce('test_job', 1, 0, mock_locations) # <--- FIXED CALL

        # Verify output file content (rest of logic remains the same)
        self.assertTrue(output_file)
        
        # Verify output file content
        with open(os.path.join(self.test_dir, 'output', output_file), 'r') as f:
            lines = f.readlines()
            
        expected_output = {
            'hello': 2,
            'world': 1,
            'mapreduce': 1
        }
        
        actual_output = {}
        for line in lines:
            key, value = line.strip().split('\t')
            actual_output[key] = int(value)
            
        self.assertEqual(actual_output, expected_output)

    def test_error_handling(self):
        """Test error cases in task execution."""
        # Test handling of missing input file
        with self.assertRaises(Exception):
            self.executor.execute_map('test_job', 1, 'nonexistent.txt')
            
        # Test handling of missing job file
        with self.assertRaises(Exception):
            self.executor.execute_map('nonexistent_job', 1, 'test_input.txt')
            
        # Test handling of invalid intermediate files
        with self.assertRaises(Exception):
            self.executor.execute_reduce('test_job', 1, 999)  # Non-existent partition

    def test_concurrent_tasks(self):
        """Test handling multiple tasks concurrently."""
        # Create multiple input files
        inputs = []
        for i in range(3):
            input_file = os.path.join(self.test_dir, 'input', f'test_input_{i}.txt')
            with open(input_file, 'w') as f:
                f.write(f"test data {i}\n")
            inputs.append(os.path.basename(input_file))

        # Execute multiple map tasks
        results = []
        for i, input_file in enumerate(inputs):
            result = self.executor.execute_map('test_job', i, input_file)
            results.extend(result)

        self.assertEqual(len(results), 3)  # One output per input file
        
    def test_large_input(self):
        """Test handling of large input files."""
        # Create a large input file (1MB)
        large_input = os.path.join(self.test_dir, 'input', 'large_input.txt')
        with open(large_input, 'w') as f:
            for i in range(50000):  # ~20 bytes per line * 50000 = ~1MB
                f.write(f"test data line {i}\n")
                
        # Execute map task on large input
        result_files = self.executor.execute_map('test_job', 1, 'large_input.txt')
        self.assertTrue(result_files)  # Verify task completed
        
        # Check intermediate file was created
        self.assertTrue(os.path.exists(
            os.path.join(self.test_dir, 'intermediate', result_files[0])
        ))

    def test_resource_monitoring(self):
        """Test resource monitoring during task execution."""
        # Create moderate size input
        input_file = os.path.join(self.test_dir, 'input', 'monitor_input.txt')
        with open(input_file, 'w') as f:
            for i in range(1000):
                f.write(f"test data line {i}\n")
                
        # Monitor memory usage during execution
        initial_memory = self.executor.get_memory_usage()
        result_files = self.executor.execute_map('test_job', 1, 'monitor_input.txt')
        final_memory = self.executor.get_memory_usage()
        
        # Verify memory was released after task
        self.assertLessEqual(final_memory, initial_memory * 1.5)  # Allow for some overhead
        
    def test_progress_reporting(self):
        """Test task progress reporting."""
        job_id = 'test_job'
        task_id = 1
        
        # Create input file
        input_file = 'progress_test.txt'
        with open(os.path.join(self.test_dir, 'input', input_file), 'w') as f:
            f.write("Hello World MapReduce Testing Progress")
            
        # Check initial state
        progress, state = self.executor.get_task_progress(job_id, task_id)
        self.assertEqual(progress, 0.0)
        self.assertEqual(state, "UNKNOWN")
        
        # Execute map task
        self.executor.execute_map(job_id, task_id, input_file)
        
        # Check final state
        progress, state = self.executor.get_task_progress(job_id, task_id)
        self.assertEqual(progress, 1.0)
        self.assertEqual(state, "COMPLETED")
        
    def test_cleanup(self):
        """Test intermediate file cleanup."""
        job_id = 'test_job'
        task_id = 1
        
        # Create and execute map task
        input_file = 'cleanup_test.txt'
        with open(os.path.join(self.test_dir, 'input', input_file), 'w') as f:
            f.write("Testing Cleanup Functionality")
            
        result_files = self.executor.execute_map(job_id, task_id, input_file)
        self.assertTrue(result_files)  # Verify files were created
        
        # Verify files exist
        for file_name in result_files:
            self.assertTrue(os.path.exists(
                os.path.join(self.test_dir, 'intermediate', file_name)
            ))
            
        # Clean up task
        self.executor.cleanup_task(job_id, task_id)
        
        # Verify files were removed
        for file_name in result_files:
            self.assertFalse(os.path.exists(
                os.path.join(self.test_dir, 'intermediate', file_name)
            ))

if __name__ == '__main__':
    unittest.main()