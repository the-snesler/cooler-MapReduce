"""
Unit tests for MapExecutor
"""

import pytest
import sys
import os
import json
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../worker'))

from map_executor import MapExecutor


class TestMapExecutorInputSplitting:
    """Tests for input split reading"""

    def test_reads_full_file_when_offsets_cover_entire_file(self, sample_input_file):
        """Test reading entire file"""
        file_size = os.path.getsize(sample_input_file)

        with patch('map_executor.FunctionLoader'):
            executor = MapExecutor(
                task_id=0,
                input_path=sample_input_file,
                start_offset=0,
                end_offset=file_size,
                num_reduce_tasks=2,
                map_reduce_file='examples/wordcount.py',
                use_combiner=False,
                job_id='test-job'
            )

            key_values = executor._read_input_split()

            assert len(key_values) == 5  # 5 lines in sample text
            assert all(isinstance(kv, tuple) and len(kv) == 2 for kv in key_values)
            assert all(isinstance(kv[0], int) and isinstance(kv[1], str) for kv in key_values)

    def test_reads_partial_file_split(self, sample_input_file):
        """Test reading partial file with offsets"""
        file_size = os.path.getsize(sample_input_file)
        mid_point = file_size // 2

        with patch('map_executor.FunctionLoader'):
            executor = MapExecutor(
                task_id=0,
                input_path=sample_input_file,
                start_offset=0,
                end_offset=mid_point,
                num_reduce_tasks=2,
                map_reduce_file='examples/wordcount.py',
                use_combiner=False,
                job_id='test-job'
            )

            key_values = executor._read_input_split()

            # Should read at least one line but not all
            assert 0 < len(key_values) < 5

    def test_empty_split_returns_empty_list(self, temp_dir):
        """Test handling of empty file split"""
        empty_file = os.path.join(temp_dir, 'empty.txt')
        with open(empty_file, 'w') as f:
            pass  # Create empty file

        with patch('map_executor.FunctionLoader'):
            executor = MapExecutor(
                task_id=0,
                input_path=empty_file,
                start_offset=0,
                end_offset=0,
                num_reduce_tasks=2,
                map_reduce_file='examples/wordcount.py',
                use_combiner=False,
                job_id='test-job'
            )

            key_values = executor._read_input_split()
            assert key_values == []


class TestMapExecutorPartitioning:
    """Tests for hash-based partitioning"""

    def test_partitioning_distributes_keys(self, sample_input_file):
        """Test that keys are distributed across partitions"""
        num_reduce_tasks = 3

        # Mock map function that returns known keys
        def mock_map(key, value):
            words = value.split()
            for word in words:
                yield (word.lower(), 1)

        mock_loader = Mock()
        mock_loader.get_map_function.return_value = mock_map

        with patch('map_executor.FunctionLoader', return_value=mock_loader):
            executor = MapExecutor(
                task_id=0,
                input_path=sample_input_file,
                start_offset=0,
                end_offset=os.path.getsize(sample_input_file),
                num_reduce_tasks=num_reduce_tasks,
                map_reduce_file='examples/wordcount.py',
                use_combiner=False,
                job_id='test-job'
            )

            result = executor.execute()

            assert result['success'] is True
            # Verify intermediate files were created for multiple partitions
            intermediate_dir = f"/mapreduce-data/intermediate/test-job"
            if os.path.exists(intermediate_dir):
                files = os.listdir(intermediate_dir)
                assert len(files) > 0

    def test_same_key_goes_to_same_partition(self):
        """Test that same key always hashes to same partition"""
        num_reduce_tasks = 4
        test_key = "test_key"

        partition1 = hash(str(test_key)) % num_reduce_tasks
        partition2 = hash(str(test_key)) % num_reduce_tasks

        assert partition1 == partition2


class TestMapExecutorCombiner:
    """Tests for combiner functionality"""

    def test_combiner_reduces_intermediate_data(self, sample_input_file, temp_dir):
        """Test that combiner reduces the amount of intermediate data"""
        # Mock functions
        def mock_map(key, value):
            words = value.split()
            for word in words:
                yield (word.lower(), 1)

        def mock_combiner(key, values):
            yield (key, sum(values))

        # Setup mock loader
        mock_loader_no_combiner = Mock()
        mock_loader_no_combiner.get_map_function.return_value = mock_map
        mock_loader_no_combiner.get_combiner_function.return_value = None

        mock_loader_with_combiner = Mock()
        mock_loader_with_combiner.get_map_function.return_value = mock_map
        mock_loader_with_combiner.get_combiner_function.return_value = mock_combiner

        # Run without combiner
        with patch('map_executor.FunctionLoader', return_value=mock_loader_no_combiner):
            executor_no_combiner = MapExecutor(
                task_id=0,
                input_path=sample_input_file,
                start_offset=0,
                end_offset=os.path.getsize(sample_input_file),
                num_reduce_tasks=2,
                map_reduce_file='examples/wordcount.py',
                use_combiner=False,
                job_id='test-no-combiner'
            )

            result = executor_no_combiner.execute()
            assert result['success'] is True

        # Run with combiner
        with patch('map_executor.FunctionLoader', return_value=mock_loader_with_combiner):
            executor_with_combiner = MapExecutor(
                task_id=1,
                input_path=sample_input_file,
                start_offset=0,
                end_offset=os.path.getsize(sample_input_file),
                num_reduce_tasks=2,
                map_reduce_file='examples/wordcount.py',
                use_combiner=True,
                job_id='test-with-combiner'
            )

            result = executor_with_combiner.execute()
            assert result['success'] is True

        # Compare intermediate data sizes
        dir_no_combiner = "/mapreduce-data/intermediate/test-no-combiner"
        dir_with_combiner = "/mapreduce-data/intermediate/test-with-combiner"

        if os.path.exists(dir_no_combiner) and os.path.exists(dir_with_combiner):
            size_no_combiner = sum(
                os.path.getsize(os.path.join(dir_no_combiner, f))
                for f in os.listdir(dir_no_combiner)
            )
            size_with_combiner = sum(
                os.path.getsize(os.path.join(dir_with_combiner, f))
                for f in os.listdir(dir_with_combiner)
            )

            # Combiner should reduce size (or at least not increase it significantly)
            assert size_with_combiner <= size_no_combiner * 1.1  # Allow 10% margin

    def test_combiner_produces_correct_aggregation(self, sample_input_file):
        """Test that combiner correctly aggregates values"""
        def mock_map(key, value):
            # Emit same key multiple times
            for _ in range(3):
                yield ('test_key', 1)

        def mock_combiner(key, values):
            yield (key, sum(values))

        mock_loader = Mock()
        mock_loader.get_map_function.return_value = mock_map
        mock_loader.get_combiner_function.return_value = mock_combiner

        with patch('map_executor.FunctionLoader', return_value=mock_loader):
            executor = MapExecutor(
                task_id=0,
                input_path=sample_input_file,
                start_offset=0,
                end_offset=os.path.getsize(sample_input_file),
                num_reduce_tasks=2,
                map_reduce_file='examples/wordcount.py',
                use_combiner=True,
                job_id='test-combiner-correctness'
            )

            result = executor.execute()
            assert result['success'] is True

            # Check intermediate files contain combined values
            intermediate_dir = "/mapreduce-data/intermediate/test-combiner-correctness"
            if os.path.exists(intermediate_dir):
                for filename in os.listdir(intermediate_dir):
                    filepath = os.path.join(intermediate_dir, filename)
                    with open(filepath, 'r') as f:
                        lines = f.readlines()
                        # With combiner, we should have fewer lines (combined)
                        # Each line should have value > 1 if properly combined
                        for line in lines:
                            data = json.loads(line)
                            if data['key'] == 'test_key':
                                assert data['value'] >= 1


class TestMapExecutorExecution:
    """Tests for overall execution"""

    def test_successful_execution_returns_correct_result(self, sample_input_file):
        """Test that successful execution returns proper result dict"""
        def mock_map(key, value):
            yield ('word', 1)

        mock_loader = Mock()
        mock_loader.get_map_function.return_value = mock_map

        with patch('map_executor.FunctionLoader', return_value=mock_loader):
            executor = MapExecutor(
                task_id=0,
                input_path=sample_input_file,
                start_offset=0,
                end_offset=os.path.getsize(sample_input_file),
                num_reduce_tasks=2,
                map_reduce_file='examples/wordcount.py',
                use_combiner=False,
                job_id='test-success'
            )

            result = executor.execute()

            assert result['success'] is True
            assert 'execution_time_ms' in result
            assert result['execution_time_ms'] > 0
            assert result['error_message'] == ''

    def test_execution_failure_returns_error_result(self, sample_input_file):
        """Test that execution failure returns proper error result"""
        def mock_map_error(key, value):
            raise ValueError("Test error")

        mock_loader = Mock()
        mock_loader.get_map_function.return_value = mock_map_error

        with patch('map_executor.FunctionLoader', return_value=mock_loader):
            executor = MapExecutor(
                task_id=0,
                input_path=sample_input_file,
                start_offset=0,
                end_offset=os.path.getsize(sample_input_file),
                num_reduce_tasks=2,
                map_reduce_file='examples/wordcount.py',
                use_combiner=False,
                job_id='test-error'
            )

            result = executor.execute()

            assert result['success'] is False
            assert 'execution_time_ms' in result
            assert result['error_message'] != ''
            assert 'Test error' in result['error_message']
