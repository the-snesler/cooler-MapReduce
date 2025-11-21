"""
Unit tests for FunctionLoader
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../worker'))

from function_loader import FunctionLoader


class TestFunctionLoaderBasics:
    """Tests for basic loading functionality"""

    def test_loads_valid_mapreduce_file(self, wordcount_job_file):
        """Test loading a valid map/reduce file"""
        loader = FunctionLoader(wordcount_job_file)
        module = loader.load_module()

        assert module is not None
        assert hasattr(module, 'map_function')
        assert hasattr(module, 'reduce_function')
        assert hasattr(module, 'combiner_function')

    def test_raises_error_for_nonexistent_file(self):
        """Test that loading non-existent file raises FileNotFoundError"""
        loader = FunctionLoader('/nonexistent/file.py')

        with pytest.raises(FileNotFoundError):
            loader.load_module()

    def test_get_map_function_loads_module_automatically(self, wordcount_job_file):
        """Test that get_map_function loads module if not already loaded"""
        loader = FunctionLoader(wordcount_job_file)
        # Don't call load_module manually
        map_func = loader.get_map_function()

        assert callable(map_func)
        assert loader.module is not None

    def test_get_reduce_function_loads_module_automatically(self, wordcount_job_file):
        """Test that get_reduce_function loads module if not already loaded"""
        loader = FunctionLoader(wordcount_job_file)
        # Don't call load_module manually
        reduce_func = loader.get_reduce_function()

        assert callable(reduce_func)
        assert loader.module is not None


class TestFunctionLoaderMapFunction:
    """Tests for map function loading"""

    def test_returns_callable_map_function(self, wordcount_job_file):
        """Test that map function is callable"""
        loader = FunctionLoader(wordcount_job_file)
        map_func = loader.get_map_function()

        assert callable(map_func)

    def test_map_function_works_correctly(self, wordcount_job_file):
        """Test that loaded map function produces correct output"""
        loader = FunctionLoader(wordcount_job_file)
        map_func = loader.get_map_function()

        # Test with sample input
        results = list(map_func(0, "hello world hello"))

        assert len(results) > 0
        assert all(isinstance(r, tuple) and len(r) == 2 for r in results)
        # Should emit (word, 1) pairs
        assert ('hello', 1) in results
        assert ('world', 1) in results

    def test_raises_error_when_map_function_missing(self, temp_dir):
        """Test error when module doesn't define map_function"""
        # Create Python file without map_function
        invalid_file = os.path.join(temp_dir, 'invalid.py')
        with open(invalid_file, 'w') as f:
            f.write("def some_other_function():\n    pass\n")

        loader = FunctionLoader(invalid_file)

        with pytest.raises(AttributeError, match="map_function"):
            loader.get_map_function()


class TestFunctionLoaderReduceFunction:
    """Tests for reduce function loading"""

    def test_returns_callable_reduce_function(self, wordcount_job_file):
        """Test that reduce function is callable"""
        loader = FunctionLoader(wordcount_job_file)
        reduce_func = loader.get_reduce_function()

        assert callable(reduce_func)

    def test_reduce_function_works_correctly(self, wordcount_job_file):
        """Test that loaded reduce function produces correct output"""
        loader = FunctionLoader(wordcount_job_file)
        reduce_func = loader.get_reduce_function()

        # Test with sample input
        results = list(reduce_func('hello', [1, 1, 1]))

        assert len(results) == 1
        assert results[0] == ('hello', 3)

    def test_raises_error_when_reduce_function_missing(self, temp_dir):
        """Test error when module doesn't define reduce_function"""
        # Create Python file without reduce_function
        invalid_file = os.path.join(temp_dir, 'invalid.py')
        with open(invalid_file, 'w') as f:
            f.write("def map_function(k, v):\n    yield (k, v)\n")

        loader = FunctionLoader(invalid_file)

        with pytest.raises(AttributeError, match="reduce_function"):
            loader.get_reduce_function()


class TestFunctionLoaderCombinerFunction:
    """Tests for combiner function loading"""

    def test_returns_combiner_function_when_defined(self, wordcount_job_file):
        """Test that combiner function is returned when explicitly defined"""
        loader = FunctionLoader(wordcount_job_file)
        combiner_func = loader.get_combiner_function()

        assert combiner_func is not None
        assert callable(combiner_func)

    def test_returns_reduce_function_as_default_combiner(self, temp_dir):
        """Test that reduce function is used as combiner when combiner not defined"""
        # Create file with map and reduce but no explicit combiner
        job_file = os.path.join(temp_dir, 'no_combiner.py')
        with open(job_file, 'w') as f:
            f.write("""
def map_function(key, value):
    yield (key, value)

def reduce_function(key, values):
    yield (key, sum(values))
""")

        loader = FunctionLoader(job_file)
        combiner_func = loader.get_combiner_function()
        reduce_func = loader.get_reduce_function()

        # Should return reduce function as combiner
        assert combiner_func is reduce_func

    def test_returns_none_when_no_combiner_or_reduce(self, temp_dir):
        """Test that None is returned when neither combiner nor reduce exists"""
        # Create file with only map function
        job_file = os.path.join(temp_dir, 'map_only.py')
        with open(job_file, 'w') as f:
            f.write("def map_function(key, value):\n    yield (key, value)\n")

        loader = FunctionLoader(job_file)
        combiner_func = loader.get_combiner_function()

        assert combiner_func is None

    def test_combiner_function_works_correctly(self, wordcount_job_file):
        """Test that loaded combiner function produces correct output"""
        loader = FunctionLoader(wordcount_job_file)
        combiner_func = loader.get_combiner_function()

        # Test with sample input
        results = list(combiner_func('word', [1, 1, 1, 1]))

        assert len(results) == 1
        assert results[0] == ('word', 4)


class TestFunctionLoaderModuleCaching:
    """Tests for module caching behavior"""

    def test_module_loaded_only_once(self, wordcount_job_file):
        """Test that module is loaded only once and cached"""
        loader = FunctionLoader(wordcount_job_file)

        # Load module twice
        module1 = loader.load_module()
        module2 = loader.load_module()

        # Should return same module instance
        assert module1 is module2
        assert loader.module is module1

    def test_multiple_function_calls_use_same_module(self, wordcount_job_file):
        """Test that multiple get_* calls don't reload module"""
        loader = FunctionLoader(wordcount_job_file)

        map_func = loader.get_map_function()
        module_after_map = loader.module

        reduce_func = loader.get_reduce_function()
        module_after_reduce = loader.module

        combiner_func = loader.get_combiner_function()
        module_after_combiner = loader.module

        # All should use same module instance
        assert module_after_map is module_after_reduce
        assert module_after_reduce is module_after_combiner
