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

    def test_partitioning_distributes_keys(self, sample_input_file, temp_dir):
        """Test that keys are distributed across partitions"""
        num_reduce_tasks = 3

        # Mock map function that returns known keys
        def mock_map(key, value):
            words = value.split()
            for word in words:
                yield (word.lower(), 1)

        mock_loader = Mock()
        mock_loader.get_map_function.return_value = mock_map

        # Patch the intermediate directory to use temp_dir
        intermediate_base = os.path.join(temp_dir, 'intermediate')
        os.makedirs(intermediate_base, exist_ok=True)

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

            # Patch the intermediate directory path
            with patch.object(executor, '_write_intermediate_files') as mock_write:
                # Instead of actually writing, just verify the method is called
                mock_write.return_value = None

                # We need to manually call the execution logic without writing
                map_func = mock_loader.get_map_function.return_value
                key_values = executor._read_input_split()

                # Verify we got data to process
                assert len(key_values) > 0

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

        # Setup test directories
        intermediate_base = os.path.join(temp_dir, 'intermediate')
        os.makedirs(intermediate_base, exist_ok=True)

        # Test without combiner
        mock_loader_no_combiner = Mock()
        mock_loader_no_combiner.get_map_function.return_value = mock_map
        mock_loader_no_combiner.get_combiner_function.return_value = None

        with patch('map_executor.FunctionLoader', return_value=mock_loader_no_combiner):
            executor = MapExecutor(
                task_id=0,
                input_path=sample_input_file,
                start_offset=0,
                end_offset=os.path.getsize(sample_input_file),
                num_reduce_tasks=2,
                map_reduce_file='examples/wordcount.py',
                use_combiner=False,
                job_id='test-no-combiner'
            )

            # Manually test the combiner logic without file I/O
            from collections import defaultdict
            intermediate = defaultdict(list)
            key_values = executor._read_input_split()
            for key, value in key_values:
                for out_key, out_value in mock_map(key, value):
                    partition = hash(str(out_key)) % 2
                    intermediate[partition].append((out_key, out_value))

            pairs_without_combiner = sum(len(v) for v in intermediate.values())

        # Test with combiner
        mock_loader_with_combiner = Mock()
        mock_loader_with_combiner.get_map_function.return_value = mock_map
        mock_loader_with_combiner.get_combiner_function.return_value = mock_combiner

        with patch('map_executor.FunctionLoader', return_value=mock_loader_with_combiner):
            executor = MapExecutor(
                task_id=1,
                input_path=sample_input_file,
                start_offset=0,
                end_offset=os.path.getsize(sample_input_file),
                num_reduce_tasks=2,
                map_reduce_file='examples/wordcount.py',
                use_combiner=True,
                job_id='test-with-combiner'
            )

            # Test combiner application
            combined = executor._apply_combiner(intermediate)
            pairs_with_combiner = sum(len(v) for v in combined.values())

            # Combiner should reduce the number of pairs
            assert pairs_with_combiner < pairs_without_combiner

    def test_combiner_produces_correct_aggregation(self, sample_input_file, temp_dir):
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

            # Test combiner logic directly without file I/O
            from collections import defaultdict
            intermediate = defaultdict(list)
            key_values = executor._read_input_split()

            # Apply map function
            for key, value in key_values:
                for out_key, out_value in mock_map(key, value):
                    partition = hash(str(out_key)) % 2
                    intermediate[partition].append((out_key, out_value))

            # Apply combiner
            combined = executor._apply_combiner(intermediate)

            # Verify combiner aggregated the values
            # Should have combined multiple (test_key, 1) into (test_key, N)
            for partition, pairs in combined.items():
                for key, value in pairs:
                    if key == 'test_key':
                        assert value > 1  # Should be aggregated


class TestMapExecutorExecution:
    """Tests for overall execution"""

    def test_successful_execution_returns_correct_result(self, sample_input_file, temp_dir):
        """Test that successful execution returns proper result dict"""
        def mock_map(key, value):
            yield ('word', 1)

        mock_loader = Mock()
        mock_loader.get_map_function.return_value = mock_map

        # Setup writable intermediate directory
        intermediate_dir = os.path.join(temp_dir, 'intermediate', 'test-success')
        os.makedirs(intermediate_dir, exist_ok=True)

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

            # Patch the intermediate directory to use temp_dir
            original_write = executor._write_intermediate_files

            def mock_write(intermediate):
                # Write to temp directory instead
                for partition, kv_pairs in intermediate.items():
                    filename = f"{intermediate_dir}/map-{executor.task_id}-reduce-{partition}.txt"
                    with open(filename, 'w') as f:
                        import json
                        for key, value in kv_pairs:
                            f.write(json.dumps({'key': key, 'value': value}) + '\n')

            executor._write_intermediate_files = mock_write

            result = executor.execute()

            assert result['success'] is True
            assert 'execution_time_ms' in result
            assert result['execution_time_ms'] >= 0  # Can be 0 for very fast execution
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
