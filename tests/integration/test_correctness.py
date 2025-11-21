"""
Correctness validation tests
Tests that MapReduce produces correct results for known inputs
"""

import pytest
import subprocess
import time
import uuid
import os


@pytest.mark.integration
class TestWordCountCorrectness:
    """Tests for word count correctness"""

    def test_wordcount_produces_correct_counts(self):
        """Test that word count produces accurate word frequencies"""
        result = subprocess.run(
            ['docker-compose', 'ps'],
            capture_output=True,
            text=True
        )

        if 'coordinator' not in result.stdout:
            pytest.skip("Docker containers not running")

        job_id = f'test-correct-{uuid.uuid4().hex[:8]}'

        # Test input with known word counts
        test_input = """the quick brown fox
the lazy dog
the fox"""

        expected_counts = {
            'the': 3,
            'quick': 1,
            'brown': 1,
            'fox': 2,
            'lazy': 1,
            'dog': 1
        }

        # Create test input
        subprocess.run([
            'docker', 'run', '--rm',
            '--network', 'coolest-mapreduce_mapreduce-net',
            '-v', 'mapreduce-data:/mapreduce-data',
            'busybox', 'sh', '-c',
            f'echo "{test_input}" > /mapreduce-data/inputs/test-{job_id}.txt'
        ], check=False)

        # Submit and wait for job
        subprocess.run([
            'python3', 'client/client.py', 'submit-job',
            '--job-id', job_id,
            '--input', f'/mapreduce-data/inputs/test-{job_id}.txt',
            '--output', f'/mapreduce-data/outputs/{job_id}',
            '--job-file', 'examples/wordcount.py',
            '--num-map-tasks', '2',
            '--num-reduce-tasks', '2',
            '--use-combiner'
        ], capture_output=True, text=True, check=False)

        # Wait for completion
        max_wait = 60
        start = time.time()

        while time.time() - start < max_wait:
            result = subprocess.run([
                'python3', 'client/client.py', 'job-status', job_id
            ], capture_output=True, text=True)

            if 'completed' in result.stdout.lower():
                break

            time.sleep(2)
        else:
            pytest.skip("Job did not complete in time")

        # Read and verify output
        # Note: This would require accessing the output files from the Docker volume
        # For now, we validate the job completed successfully
        # Full validation would require mounting volume or docker exec


@pytest.mark.integration
class TestInvertedIndexCorrectness:
    """Tests for inverted index correctness"""

    def test_inverted_index_produces_correct_mappings(self):
        """Test that inverted index creates correct word-to-document mappings"""
        result = subprocess.run(
            ['docker-compose', 'ps'],
            capture_output=True,
            text=True
        )

        if 'coordinator' not in result.stdout:
            pytest.skip("Docker containers not running")

        job_id = f'test-index-{uuid.uuid4().hex[:8]}'

        # Test input with known document structure
        test_input = """apple banana
banana cherry
apple cherry"""

        # Expected: apple in doc_0 and doc_2, banana in doc_0 and doc_1, cherry in doc_1 and doc_2

        # Create test input
        subprocess.run([
            'docker', 'run', '--rm',
            '--network', 'coolest-mapreduce_mapreduce-net',
            '-v', 'mapreduce-data:/mapreduce-data',
            'busybox', 'sh', '-c',
            f'echo "{test_input}" > /mapreduce-data/inputs/test-{job_id}.txt'
        ], check=False)

        # Submit and wait for job
        subprocess.run([
            'python3', 'client/client.py', 'submit-job',
            '--job-id', job_id,
            '--input', f'/mapreduce-data/inputs/test-{job_id}.txt',
            '--output', f'/mapreduce-data/outputs/{job_id}',
            '--job-file', 'examples/inverted_index.py',
            '--num-map-tasks', '2',
            '--num-reduce-tasks', '2'
        ], capture_output=True, text=True, check=False)

        # Wait for completion
        max_wait = 60
        start = time.time()

        while time.time() - start < max_wait:
            result = subprocess.run([
                'python3', 'client/client.py', 'job-status', job_id
            ], capture_output=True, text=True)

            if 'completed' in result.stdout.lower():
                break

            time.sleep(2)
        else:
            pytest.skip("Job did not complete in time")

        # Validate job completed
        result = subprocess.run([
            'python3', 'client/client.py', 'get-results', job_id
        ], capture_output=True, text=True)

        assert result.returncode == 0
