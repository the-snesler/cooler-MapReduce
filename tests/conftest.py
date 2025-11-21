"""
Pytest configuration and shared fixtures
"""

import pytest
import os
import tempfile
import shutil


@pytest.fixture
def temp_dir():
    """Create temporary directory for test files"""
    dirpath = tempfile.mkdtemp()
    yield dirpath
    shutil.rmtree(dirpath)


@pytest.fixture
def sample_text():
    """Sample text for testing"""
    return """The quick brown fox jumps over the lazy dog.
The dog was really lazy.
The fox was very quick and brown.
Quick brown foxes are amazing animals.
Lazy dogs sleep all day."""


@pytest.fixture
def sample_input_file(temp_dir, sample_text):
    """Create a sample input file for testing"""
    filepath = os.path.join(temp_dir, 'input.txt')
    with open(filepath, 'w') as f:
        f.write(sample_text)
    return filepath


@pytest.fixture
def wordcount_job_file():
    """Path to word count example job file"""
    return 'examples/wordcount.py'


@pytest.fixture
def inverted_index_job_file():
    """Path to inverted index example job file"""
    return 'examples/inverted_index.py'
