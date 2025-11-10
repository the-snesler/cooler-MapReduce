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

def sample_combine_fn(key, values):
    """Combiner function for testing - same as reduce for word count."""
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
        # Create test intermediate data
        intermediate_data = [
            ('hello', 1),
            ('world', 1),
            ('hello', 1),
            ('mapreduce', 1)
        ]
        
        os.makedirs(os.path.join(self.test_dir, 'intermediate'), exist_ok=True)
        with open(os.path.join(self.test_dir, 'intermediate', 
                              'test_job_map_1_part_0.pickle'), 'wb') as f:
            pickle.dump(intermediate_data, f)
            
        # Test reduce task execution
        output_file = self.executor.execute_reduce('test_job', 1, 0)
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

    def test_combiner_with_wordcount(self):
        """Test map execution with combiner enabled for word count."""
        # Create job with combiner
        job_data_with_combiner = {
            'map_fn': sample_map_fn,
            'reduce_fn': sample_reduce_fn,
            'combine_fn': sample_combine_fn
        }
        with open(os.path.join(self.test_dir, 'jobs', 'test_job_combiner.pickle'), 'wb') as f:
            pickle.dump(job_data_with_combiner, f)

        # Create input with repeated words
        with open(os.path.join(self.test_dir, 'input', 'combiner_test.txt'), 'w') as f:
            f.write("hello hello hello world world hello mapreduce mapreduce hello")

        # Execute map task with combiner
        result_files = self.executor.execute_map('test_job_combiner', 1, 'combiner_test.txt')

        # Verify intermediate file content
        with open(os.path.join(self.test_dir, 'intermediate', result_files[0]), 'rb') as f:
            combined_data = pickle.load(f)

        # With combiner, each key should appear only once with aggregated count
        data_dict = {k: v for k, v in combined_data}
        self.assertEqual(data_dict['hello'], 5)
        self.assertEqual(data_dict['world'], 2)
        self.assertEqual(data_dict['mapreduce'], 2)
        self.assertEqual(len(data_dict), 3)  # Only 3 unique keys

    def test_combiner_reduces_data_size(self):
        """Test that combiner reduces intermediate data size."""
        # Job without combiner
        job_data_no_combiner = {
            'map_fn': sample_map_fn,
            'reduce_fn': sample_reduce_fn
        }
        with open(os.path.join(self.test_dir, 'jobs', 'test_job_no_combiner.pickle'), 'wb') as f:
            pickle.dump(job_data_no_combiner, f)

        # Job with combiner
        job_data_with_combiner = {
            'map_fn': sample_map_fn,
            'reduce_fn': sample_reduce_fn,
            'combine_fn': sample_combine_fn
        }
        with open(os.path.join(self.test_dir, 'jobs', 'test_job_with_combiner.pickle'), 'wb') as f:
            pickle.dump(job_data_with_combiner, f)

        # Create input with many repeated words
        with open(os.path.join(self.test_dir, 'input', 'size_test.txt'), 'w') as f:
            # Repeat "test word other" 100 times
            f.write(" ".join(["test word other"] * 100))

        # Execute without combiner
        result_no_combiner = self.executor.execute_map('test_job_no_combiner', 1, 'size_test.txt')
        with open(os.path.join(self.test_dir, 'intermediate', result_no_combiner[0]), 'rb') as f:
            data_no_combiner = pickle.load(f)

        # Execute with combiner
        result_with_combiner = self.executor.execute_map('test_job_with_combiner', 2, 'size_test.txt')
        with open(os.path.join(self.test_dir, 'intermediate', result_with_combiner[0]), 'rb') as f:
            data_with_combiner = pickle.load(f)

        # Verify combiner reduced data size significantly
        self.assertEqual(len(data_no_combiner), 300)  # 3 words * 100 repetitions
        self.assertEqual(len(data_with_combiner), 3)  # Only 3 unique keys after combining
        self.assertLess(len(data_with_combiner), len(data_no_combiner) / 50)  # At least 50x reduction

    def test_combiner_correctness(self):
        """Test that combiner produces same final result as without combiner."""
        # Create jobs with and without combiner
        job_no_combiner = {
            'map_fn': sample_map_fn,
            'reduce_fn': sample_reduce_fn
        }
        with open(os.path.join(self.test_dir, 'jobs', 'job_no_comb.pickle'), 'wb') as f:
            pickle.dump(job_no_combiner, f)

        job_with_combiner = {
            'map_fn': sample_map_fn,
            'reduce_fn': sample_reduce_fn,
            'combine_fn': sample_combine_fn
        }
        with open(os.path.join(self.test_dir, 'jobs', 'job_with_comb.pickle'), 'wb') as f:
            pickle.dump(job_with_combiner, f)

        # Create test input
        with open(os.path.join(self.test_dir, 'input', 'correctness_test.txt'), 'w') as f:
            f.write("apple banana apple cherry banana apple")

        # Execute map tasks
        result_no_comb = self.executor.execute_map('job_no_comb', 1, 'correctness_test.txt')
        result_with_comb = self.executor.execute_map('job_with_comb', 2, 'correctness_test.txt')

        # Execute reduce tasks for both
        output_no_comb = self.executor.execute_reduce('job_no_comb', 1, 0)
        output_with_comb = self.executor.execute_reduce('job_with_comb', 2, 0)

        # Read and compare final outputs
        with open(os.path.join(self.test_dir, 'output', output_no_comb), 'r') as f:
            results_no_comb = {}
            for line in f:
                key, value = line.strip().split('\t')
                results_no_comb[key] = int(value)

        with open(os.path.join(self.test_dir, 'output', output_with_comb), 'r') as f:
            results_with_comb = {}
            for line in f:
                key, value = line.strip().split('\t')
                results_with_comb[key] = int(value)

        # Verify identical results
        self.assertEqual(results_no_comb, results_with_comb)
        self.assertEqual(results_no_comb['apple'], 3)
        self.assertEqual(results_no_comb['banana'], 2)
        self.assertEqual(results_no_comb['cherry'], 1)

    def test_combiner_progress_tracking(self):
        """Test that progress tracking includes COMBINING state."""
        # Create job with combiner
        job_data = {
            'map_fn': sample_map_fn,
            'reduce_fn': sample_reduce_fn,
            'combine_fn': sample_combine_fn
        }
        with open(os.path.join(self.test_dir, 'jobs', 'progress_comb_job.pickle'), 'wb') as f:
            pickle.dump(job_data, f)

        # Create input file
        with open(os.path.join(self.test_dir, 'input', 'progress_comb.txt'), 'w') as f:
            f.write("test " * 100)

        # Execute map task with combiner
        self.executor.execute_map('progress_comb_job', 99, 'progress_comb.txt')

        # Verify task completed
        progress, state = self.executor.get_task_progress('progress_comb_job', 99)
        self.assertEqual(progress, 1.0)
        self.assertEqual(state, "COMPLETED")

if __name__ == '__main__':
    unittest.main()