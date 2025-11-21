"""
Unit tests for ReduceExecutor
"""

import pytest
import sys
import os
import json
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../worker'))

from reduce_executor import ReduceExecutor


class TestReduceExecutorGrouping:
    """Tests for key grouping functionality"""

    def test_groups_values_by_key(self, temp_dir):
        """Test that values are correctly grouped by key"""
        # Create test intermediate files
        intermediate_dir = os.path.join(temp_dir, 'intermediate')
        os.makedirs(intermediate_dir)

        file1 = os.path.join(intermediate_dir, 'map-0-reduce-0.txt')
        with open(file1, 'w') as f:
            f.write(json.dumps({'key': 'apple', 'value': 1}) + '\n')
            f.write(json.dumps({'key': 'banana', 'value': 1}) + '\n')
            f.write(json.dumps({'key': 'apple', 'value': 1}) + '\n')

        file2 = os.path.join(intermediate_dir, 'map-1-reduce-0.txt')
        with open(file2, 'w') as f:
            f.write(json.dumps({'key': 'apple', 'value': 1}) + '\n')
            f.write(json.dumps({'key': 'cherry', 'value': 1}) + '\n')

        with patch('reduce_executor.FunctionLoader'):
            executor = ReduceExecutor(
                task_id=0,
                partition_id=0,
                intermediate_files=[file1, file2],
                map_reduce_file='examples/wordcount.py',
                output_path=os.path.join(temp_dir, 'output'),
                job_id='test-job'
            )

            key_groups = executor._read_and_group_intermediate()

            assert 'apple' in key_groups
            assert len(key_groups['apple']) == 3  # Three occurrences
            assert 'banana' in key_groups
            assert len(key_groups['banana']) == 1
            assert 'cherry' in key_groups
            assert len(key_groups['cherry']) == 1

    def test_handles_empty_intermediate_files(self, temp_dir):
        """Test handling of empty intermediate files"""
        intermediate_dir = os.path.join(temp_dir, 'intermediate')
        os.makedirs(intermediate_dir)

        empty_file = os.path.join(intermediate_dir, 'map-0-reduce-0.txt')
        with open(empty_file, 'w') as f:
            pass  # Create empty file

        with patch('reduce_executor.FunctionLoader'):
            executor = ReduceExecutor(
                task_id=0,
                partition_id=0,
                intermediate_files=[empty_file],
                map_reduce_file='examples/wordcount.py',
                output_path=os.path.join(temp_dir, 'output'),
                job_id='test-job'
            )

            key_groups = executor._read_and_group_intermediate()
            assert len(key_groups) == 0

    def test_handles_missing_intermediate_files(self, temp_dir):
        """Test handling of non-existent intermediate files"""
        with patch('reduce_executor.FunctionLoader'):
            executor = ReduceExecutor(
                task_id=0,
                partition_id=0,
                intermediate_files=['/nonexistent/file.txt'],
                map_reduce_file='examples/wordcount.py',
                output_path=os.path.join(temp_dir, 'output'),
                job_id='test-job'
            )

            key_groups = executor._read_and_group_intermediate()
            assert len(key_groups) == 0

    def test_skips_malformed_json_lines(self, temp_dir):
        """Test that malformed JSON lines are skipped"""
        intermediate_dir = os.path.join(temp_dir, 'intermediate')
        os.makedirs(intermediate_dir)

        file1 = os.path.join(intermediate_dir, 'map-0-reduce-0.txt')
        with open(file1, 'w') as f:
            f.write(json.dumps({'key': 'apple', 'value': 1}) + '\n')
            f.write('invalid json line\n')
            f.write(json.dumps({'key': 'banana', 'value': 1}) + '\n')
            f.write(json.dumps({'missing': 'key_field'}) + '\n')

        with patch('reduce_executor.FunctionLoader'):
            executor = ReduceExecutor(
                task_id=0,
                partition_id=0,
                intermediate_files=[file1],
                map_reduce_file='examples/wordcount.py',
                output_path=os.path.join(temp_dir, 'output'),
                job_id='test-job'
            )

            key_groups = executor._read_and_group_intermediate()

            # Should only have valid entries
            assert len(key_groups) == 2
            assert 'apple' in key_groups
            assert 'banana' in key_groups


class TestReduceExecutorOutput:
    """Tests for output generation"""

    def test_output_is_sorted_by_key(self, temp_dir):
        """Test that reduce output is sorted alphabetically by key"""
        intermediate_dir = os.path.join(temp_dir, 'intermediate')
        os.makedirs(intermediate_dir)

        file1 = os.path.join(intermediate_dir, 'map-0-reduce-0.txt')
        with open(file1, 'w') as f:
            f.write(json.dumps({'key': 'zebra', 'value': 1}) + '\n')
            f.write(json.dumps({'key': 'apple', 'value': 1}) + '\n')
            f.write(json.dumps({'key': 'mango', 'value': 1}) + '\n')

        def mock_reduce(key, values):
            yield (key, len(values))

        mock_loader = Mock()
        mock_loader.get_reduce_function.return_value = mock_reduce

        with patch('reduce_executor.FunctionLoader', return_value=mock_loader):
            executor = ReduceExecutor(
                task_id=0,
                partition_id=0,
                intermediate_files=[file1],
                map_reduce_file='examples/wordcount.py',
                output_path=os.path.join(temp_dir, 'output'),
                job_id='test-job'
            )

            result = executor.execute()
            assert result['success'] is True

            # Read output and verify sorting
            output_file = os.path.join(temp_dir, 'output', 'part-0.txt')
            with open(output_file, 'r') as f:
                lines = f.readlines()

            keys = [line.split('\t')[0] for line in lines]
            assert keys == sorted(keys), "Output should be sorted by key"
            assert keys == ['apple', 'mango', 'zebra']

    def test_output_format_is_tab_separated(self, temp_dir):
        """Test that output uses tab-separated format"""
        intermediate_dir = os.path.join(temp_dir, 'intermediate')
        os.makedirs(intermediate_dir)

        file1 = os.path.join(intermediate_dir, 'map-0-reduce-0.txt')
        with open(file1, 'w') as f:
            f.write(json.dumps({'key': 'test', 'value': 42}) + '\n')

        def mock_reduce(key, values):
            yield (key, sum(values))

        mock_loader = Mock()
        mock_loader.get_reduce_function.return_value = mock_reduce

        with patch('reduce_executor.FunctionLoader', return_value=mock_loader):
            executor = ReduceExecutor(
                task_id=0,
                partition_id=0,
                intermediate_files=[file1],
                map_reduce_file='examples/wordcount.py',
                output_path=os.path.join(temp_dir, 'output'),
                job_id='test-job'
            )

            result = executor.execute()
            assert result['success'] is True

            output_file = os.path.join(temp_dir, 'output', 'part-0.txt')
            with open(output_file, 'r') as f:
                line = f.readline().strip()

            assert '\t' in line
            key, value = line.split('\t')
            assert key == 'test'
            assert value == '42'

    def test_creates_output_directory_if_not_exists(self, temp_dir):
        """Test that output directory is created if it doesn't exist"""
        intermediate_dir = os.path.join(temp_dir, 'intermediate')
        os.makedirs(intermediate_dir)

        file1 = os.path.join(intermediate_dir, 'map-0-reduce-0.txt')
        with open(file1, 'w') as f:
            f.write(json.dumps({'key': 'test', 'value': 1}) + '\n')

        def mock_reduce(key, values):
            yield (key, len(values))

        mock_loader = Mock()
        mock_loader.get_reduce_function.return_value = mock_reduce

        output_path = os.path.join(temp_dir, 'new_output_dir')
        assert not os.path.exists(output_path)

        with patch('reduce_executor.FunctionLoader', return_value=mock_loader):
            executor = ReduceExecutor(
                task_id=0,
                partition_id=0,
                intermediate_files=[file1],
                map_reduce_file='examples/wordcount.py',
                output_path=output_path,
                job_id='test-job'
            )

            result = executor.execute()
            assert result['success'] is True
            assert os.path.exists(output_path)


class TestReduceExecutorExecution:
    """Tests for overall execution"""

    def test_successful_execution_returns_correct_result(self, temp_dir):
        """Test that successful execution returns proper result dict"""
        intermediate_dir = os.path.join(temp_dir, 'intermediate')
        os.makedirs(intermediate_dir)

        file1 = os.path.join(intermediate_dir, 'map-0-reduce-0.txt')
        with open(file1, 'w') as f:
            f.write(json.dumps({'key': 'word', 'value': 1}) + '\n')

        def mock_reduce(key, values):
            yield (key, sum(values))

        mock_loader = Mock()
        mock_loader.get_reduce_function.return_value = mock_reduce

        with patch('reduce_executor.FunctionLoader', return_value=mock_loader):
            executor = ReduceExecutor(
                task_id=0,
                partition_id=0,
                intermediate_files=[file1],
                map_reduce_file='examples/wordcount.py',
                output_path=os.path.join(temp_dir, 'output'),
                job_id='test-success'
            )

            result = executor.execute()

            assert result['success'] is True
            assert 'execution_time_ms' in result
            assert result['execution_time_ms'] > 0
            assert result['error_message'] == ''

    def test_execution_failure_returns_error_result(self, temp_dir):
        """Test that execution failure returns proper error result"""
        intermediate_dir = os.path.join(temp_dir, 'intermediate')
        os.makedirs(intermediate_dir)

        file1 = os.path.join(intermediate_dir, 'map-0-reduce-0.txt')
        with open(file1, 'w') as f:
            f.write(json.dumps({'key': 'word', 'value': 1}) + '\n')

        def mock_reduce_error(key, values):
            raise ValueError("Test error")

        mock_loader = Mock()
        mock_loader.get_reduce_function.return_value = mock_reduce_error

        with patch('reduce_executor.FunctionLoader', return_value=mock_loader):
            executor = ReduceExecutor(
                task_id=0,
                partition_id=0,
                intermediate_files=[file1],
                map_reduce_file='examples/wordcount.py',
                output_path=os.path.join(temp_dir, 'output'),
                job_id='test-error'
            )

            result = executor.execute()

            assert result['success'] is False
            assert 'execution_time_ms' in result
            assert result['error_message'] != ''
            assert 'Test error' in result['error_message']

    def test_reduce_aggregates_values_correctly(self, temp_dir):
        """Test that reduce correctly aggregates all values for a key"""
        intermediate_dir = os.path.join(temp_dir, 'intermediate')
        os.makedirs(intermediate_dir)

        # Create multiple files with same key
        for i in range(3):
            filepath = os.path.join(intermediate_dir, f'map-{i}-reduce-0.txt')
            with open(filepath, 'w') as f:
                f.write(json.dumps({'key': 'count', 'value': 10}) + '\n')

        def mock_reduce(key, values):
            yield (key, sum(values))

        mock_loader = Mock()
        mock_loader.get_reduce_function.return_value = mock_reduce

        intermediate_files = [
            os.path.join(intermediate_dir, f'map-{i}-reduce-0.txt')
            for i in range(3)
        ]

        with patch('reduce_executor.FunctionLoader', return_value=mock_loader):
            executor = ReduceExecutor(
                task_id=0,
                partition_id=0,
                intermediate_files=intermediate_files,
                map_reduce_file='examples/wordcount.py',
                output_path=os.path.join(temp_dir, 'output'),
                job_id='test-aggregate'
            )

            result = executor.execute()
            assert result['success'] is True

            # Verify aggregation
            output_file = os.path.join(temp_dir, 'output', 'part-0.txt')
            with open(output_file, 'r') as f:
                line = f.readline().strip()

            key, value = line.split('\t')
            assert key == 'count'
            assert int(value) == 30  # 10 * 3
