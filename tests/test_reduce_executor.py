#!/usr/bin/env python3
"""
Unit and integration tests for ReduceExecutor
Tests reduce task execution, intermediate file reading, key grouping, and output generation
"""

import sys
import os
import tempfile
import shutil
import json

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'worker'))

from worker.reduce_executor import ReduceExecutor


class TestReduceExecutor:
    """Test suite for ReduceExecutor"""

    @classmethod
    def setup_class(cls):
        """Create temporary directories for testing"""
        cls.temp_dir = tempfile.mkdtemp()
        cls.intermediate_dir = os.path.join(cls.temp_dir, "intermediate")
        cls.output_dir = os.path.join(cls.temp_dir, "output")
        cls.mapreduce_file = os.path.join(cls.temp_dir, "wordcount.py")

        os.makedirs(cls.intermediate_dir, exist_ok=True)
        os.makedirs(cls.output_dir, exist_ok=True)

        # Create a simple wordcount mapreduce file
        with open(cls.mapreduce_file, 'w') as f:
            f.write("""
def map_function(key, value):
    '''Map function for word count'''
    for word in value.split():
        yield (word.lower(), 1)

def reduce_function(key, values):
    '''Reduce function for word count'''
    yield (key, sum(values))
""")

    @classmethod
    def teardown_class(cls):
        """Clean up temporary directories"""
        if os.path.exists(cls.temp_dir):
            shutil.rmtree(cls.temp_dir)

    def test_read_and_group_intermediate_files(self):
        """Test reading and grouping intermediate files"""
        # Create test intermediate files
        intermediate_files = []
        for i in range(3):
            filepath = os.path.join(self.intermediate_dir, f"map-{i}-reduce-0.txt")
            intermediate_files.append(filepath)

            with open(filepath, 'w') as f:
                f.write(json.dumps({'key': 'hello', 'value': 1}) + '\n')
                f.write(json.dumps({'key': 'world', 'value': 1}) + '\n')
                f.write(json.dumps({'key': 'hello', 'value': 1}) + '\n')

        executor = ReduceExecutor(
            task_id=0,
            partition_id=0,
            intermediate_files=intermediate_files,
            map_reduce_file=self.mapreduce_file,
            output_path=self.output_dir,
            job_id="test-job-1"
        )

        key_groups = executor._read_and_group_intermediate()

        assert 'hello' in key_groups
        assert 'world' in key_groups
        assert len(key_groups['hello']) == 6  # 2 from each of 3 files
        assert len(key_groups['world']) == 3  # 1 from each of 3 files
        print("✓ Intermediate files read and grouped correctly")

    def test_malformed_json_handling(self):
        """Test that malformed JSON lines are skipped gracefully"""
        intermediate_file = os.path.join(self.intermediate_dir, "malformed.txt")

        with open(intermediate_file, 'w') as f:
            f.write(json.dumps({'key': 'good', 'value': 1}) + '\n')
            f.write('This is not JSON\n')  # Malformed line
            f.write(json.dumps({'key': 'good', 'value': 1}) + '\n')
            f.write('{invalid json}\n')  # Malformed line

        executor = ReduceExecutor(
            task_id=1,
            partition_id=0,
            intermediate_files=[intermediate_file],
            map_reduce_file=self.mapreduce_file,
            output_path=self.output_dir,
            job_id="test-job-2"
        )

        key_groups = executor._read_and_group_intermediate()

        assert 'good' in key_groups
        assert len(key_groups['good']) == 2  # Only valid lines counted
        print("✓ Malformed JSON lines handled gracefully")

    def test_missing_file_handling(self):
        """Test that missing intermediate files are handled gracefully"""
        intermediate_files = [
            os.path.join(self.intermediate_dir, "exists.txt"),
            os.path.join(self.intermediate_dir, "missing.txt"),
        ]

        # Create only one file
        with open(intermediate_files[0], 'w') as f:
            f.write(json.dumps({'key': 'test', 'value': 1}) + '\n')

        executor = ReduceExecutor(
            task_id=2,
            partition_id=0,
            intermediate_files=intermediate_files,
            map_reduce_file=self.mapreduce_file,
            output_path=self.output_dir,
            job_id="test-job-3"
        )

        key_groups = executor._read_and_group_intermediate()

        assert 'test' in key_groups
        assert len(key_groups['test']) == 1
        print("✓ Missing intermediate files handled gracefully")

    def test_reduce_function_execution(self):
        """Test reduce function execution with sorted key processing"""
        # Create test intermediate files
        intermediate_file = os.path.join(self.intermediate_dir, "reduce-test.txt")

        with open(intermediate_file, 'w') as f:
            f.write(json.dumps({'key': 'zebra', 'value': 1}) + '\n')
            f.write(json.dumps({'key': 'apple', 'value': 1}) + '\n')
            f.write(json.dumps({'key': 'banana', 'value': 1}) + '\n')
            f.write(json.dumps({'key': 'apple', 'value': 1}) + '\n')

        executor = ReduceExecutor(
            task_id=3,
            partition_id=0,
            intermediate_files=[intermediate_file],
            map_reduce_file=self.mapreduce_file,
            output_path=self.output_dir,
            job_id="test-job-4"
        )

        result = executor.execute()

        assert result['success'] == True
        assert result['execution_time_ms'] > 0
        assert result['error_message'] == ''

        # Verify output file exists and has correct format
        output_file = os.path.join(self.output_dir, "part-0.txt")
        assert os.path.exists(output_file)

        # Read and verify output
        with open(output_file, 'r') as f:
            lines = f.readlines()

        assert len(lines) == 3  # 3 unique keys

        # Verify keys are sorted
        keys = [line.split('\t')[0] for line in lines]
        assert keys == ['apple', 'banana', 'zebra']

        # Verify values
        values = [int(line.split('\t')[1].strip()) for line in lines]
        assert values == [2, 1, 1]  # apple appears twice

        print("✓ Reduce function executed correctly with sorted output")

    def test_output_file_format(self):
        """Test output file is written in correct tab-separated format"""
        intermediate_file = os.path.join(self.intermediate_dir, "format-test.txt")

        with open(intermediate_file, 'w') as f:
            f.write(json.dumps({'key': 'key1', 'value': 10}) + '\n')
            f.write(json.dumps({'key': 'key2', 'value': 20}) + '\n')

        executor = ReduceExecutor(
            task_id=4,
            partition_id=5,  # Test specific partition ID
            intermediate_files=[intermediate_file],
            map_reduce_file=self.mapreduce_file,
            output_path=self.output_dir,
            job_id="test-job-5"
        )

        executor.execute()

        output_file = os.path.join(self.output_dir, "part-5.txt")
        assert os.path.exists(output_file)

        with open(output_file, 'r') as f:
            for line in f:
                parts = line.strip().split('\t')
                assert len(parts) == 2  # key and value separated by tab

        print("✓ Output file format is correct (tab-separated)")

    def test_empty_intermediate_files(self):
        """Test reduce task with empty intermediate files"""
        intermediate_file = os.path.join(self.intermediate_dir, "empty.txt")

        # Create empty file
        open(intermediate_file, 'w').close()

        executor = ReduceExecutor(
            task_id=5,
            partition_id=0,
            intermediate_files=[intermediate_file],
            map_reduce_file=self.mapreduce_file,
            output_path=self.output_dir,
            job_id="test-job-6"
        )

        result = executor.execute()

        assert result['success'] == True

        # Verify output file exists but is empty
        output_file = os.path.join(self.output_dir, "part-0.txt")
        assert os.path.exists(output_file)

        with open(output_file, 'r') as f:
            content = f.read()
            assert content == ''

        print("✓ Empty intermediate files handled correctly")

    def test_error_handling(self):
        """Test error handling when reduce function fails"""
        # Create a mapreduce file with a reduce function that raises an error
        bad_mapreduce_file = os.path.join(self.temp_dir, "bad_reduce.py")

        with open(bad_mapreduce_file, 'w') as f:
            f.write("""
def map_function(key, value):
    yield (key, value)

def reduce_function(key, values):
    raise ValueError("Intentional error for testing")
""")

        intermediate_file = os.path.join(self.intermediate_dir, "error-test.txt")

        with open(intermediate_file, 'w') as f:
            f.write(json.dumps({'key': 'test', 'value': 1}) + '\n')

        executor = ReduceExecutor(
            task_id=6,
            partition_id=0,
            intermediate_files=[intermediate_file],
            map_reduce_file=bad_mapreduce_file,
            output_path=self.output_dir,
            job_id="test-job-7"
        )

        result = executor.execute()

        assert result['success'] == False
        assert len(result['error_message']) > 0
        assert result['execution_time_ms'] >= 0

        print("✓ Error handling works correctly")

    def test_key_type_conversion(self):
        """Test that different key types are converted to strings correctly"""
        intermediate_file = os.path.join(self.intermediate_dir, "types-test.txt")

        with open(intermediate_file, 'w') as f:
            f.write(json.dumps({'key': 1, 'value': 10}) + '\n')
            f.write(json.dumps({'key': '1', 'value': 20}) + '\n')
            f.write(json.dumps({'key': 2.5, 'value': 30}) + '\n')

        executor = ReduceExecutor(
            task_id=7,
            partition_id=0,
            intermediate_files=[intermediate_file],
            map_reduce_file=self.mapreduce_file,
            output_path=self.output_dir,
            job_id="test-job-8"
        )

        key_groups = executor._read_and_group_intermediate()

        # Keys should be converted to strings
        assert '1' in key_groups
        assert '2.5' in key_groups
        # Both integer 1 and string '1' should be grouped together
        assert len(key_groups['1']) == 2

        print("✓ Key type conversion works correctly")


def run_tests():
    """Run all tests"""
    test_suite = TestReduceExecutor()

    print("=" * 60)
    print("ReduceExecutor Unit Tests")
    print("=" * 60)

    try:
        TestReduceExecutor.setup_class()

        test_suite.test_read_and_group_intermediate_files()
        test_suite.test_malformed_json_handling()
        test_suite.test_missing_file_handling()
        test_suite.test_reduce_function_execution()
        test_suite.test_output_file_format()
        test_suite.test_empty_intermediate_files()
        test_suite.test_error_handling()
        test_suite.test_key_type_conversion()

        print("\n" + "=" * 60)
        print("All ReduceExecutor tests passed!")
        print("=" * 60)
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        TestReduceExecutor.teardown_class()


if __name__ == "__main__":
    run_tests()
