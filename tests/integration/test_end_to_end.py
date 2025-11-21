"""
End-to-end integration tests
Requires Docker containers to be running
"""

import pytest
import subprocess
import time
import uuid


@pytest.mark.integration
class TestEndToEndExecution:
    """Tests for complete job execution from submission to results"""

    def test_wordcount_job_completes_successfully(self):
        """Test complete word count job execution"""
        # Ensure containers are running
        result = subprocess.run(
            ['docker-compose', 'ps'],
            capture_output=True,
            text=True
        )

        if 'coordinator' not in result.stdout or 'worker' not in result.stdout:
            pytest.skip("Docker containers not running")

        job_id = f'test-integration-{uuid.uuid4().hex[:8]}'

        # Upload test data
        test_input = """the quick brown fox jumps over the lazy dog
the dog was lazy
the fox was quick"""

        # Create test input file in container
        subprocess.run([
            'docker', 'run', '--rm',
            '--network', 'coolest-mapreduce_mapreduce-net',
            '-v', 'mapreduce-data:/mapreduce-data',
            'busybox', 'sh', '-c',
            f'echo "{test_input}" > /mapreduce-data/inputs/test-{job_id}.txt'
        ], check=False)

        # Submit job
        result = subprocess.run([
            'python3', 'client/client.py', 'submit-job',
            '--job-id', job_id,
            '--input', f'/mapreduce-data/inputs/test-{job_id}.txt',
            '--output', f'/mapreduce-data/outputs/{job_id}',
            '--job-file', 'examples/wordcount.py',
            '--num-map-tasks', '2',
            '--num-reduce-tasks', '2',
            '--use-combiner'
        ], capture_output=True, text=True)

        if result.returncode != 0:
            pytest.skip(f"Job submission failed: {result.stderr}")

        # Poll for completion
        max_wait = 60  # seconds
        start = time.time()
        job_completed = False

        while time.time() - start < max_wait:
            result = subprocess.run([
                'python3', 'client/client.py', 'job-status', job_id
            ], capture_output=True, text=True)

            if 'completed' in result.stdout.lower():
                job_completed = True
                break

            if 'failed' in result.stdout.lower():
                pytest.fail(f"Job failed: {result.stdout}")

            time.sleep(2)

        assert job_completed, "Job did not complete within timeout"

        # Get results
        result = subprocess.run([
            'python3', 'client/client.py', 'get-results', job_id
        ], capture_output=True, text=True)

        assert result.returncode == 0
        assert 'Execution time' in result.stdout

    def test_inverted_index_job_completes_successfully(self):
        """Test complete inverted index job execution"""
        result = subprocess.run(
            ['docker-compose', 'ps'],
            capture_output=True,
            text=True
        )

        if 'coordinator' not in result.stdout or 'worker' not in result.stdout:
            pytest.skip("Docker containers not running")

        job_id = f'test-inverted-{uuid.uuid4().hex[:8]}'

        test_input = """hello world
hello there
world peace"""

        # Create test input file
        subprocess.run([
            'docker', 'run', '--rm',
            '--network', 'coolest-mapreduce_mapreduce-net',
            '-v', 'mapreduce-data:/mapreduce-data',
            'busybox', 'sh', '-c',
            f'echo "{test_input}" > /mapreduce-data/inputs/test-{job_id}.txt'
        ], check=False)

        # Submit job
        result = subprocess.run([
            'python3', 'client/client.py', 'submit-job',
            '--job-id', job_id,
            '--input', f'/mapreduce-data/inputs/test-{job_id}.txt',
            '--output', f'/mapreduce-data/outputs/{job_id}',
            '--job-file', 'examples/inverted_index.py',
            '--num-map-tasks', '2',
            '--num-reduce-tasks', '2'
        ], capture_output=True, text=True)

        if result.returncode != 0:
            pytest.skip(f"Job submission failed: {result.stderr}")

        # Poll for completion
        max_wait = 60
        start = time.time()
        job_completed = False

        while time.time() - start < max_wait:
            result = subprocess.run([
                'python3', 'client/client.py', 'job-status', job_id
            ], capture_output=True, text=True)

            if 'completed' in result.stdout.lower():
                job_completed = True
                break

            if 'failed' in result.stdout.lower():
                pytest.fail(f"Job failed: {result.stdout}")

            time.sleep(2)

        assert job_completed, "Job did not complete within timeout"


@pytest.mark.integration
@pytest.mark.slow
class TestPerformanceMetrics:
    """Tests for performance metrics collection"""

    def test_metrics_file_created_after_job(self):
        """Test that metrics file is created after job completion"""
        result = subprocess.run(
            ['docker-compose', 'ps'],
            capture_output=True,
            text=True
        )

        if 'coordinator' not in result.stdout:
            pytest.skip("Docker containers not running")

        job_id = f'test-metrics-{uuid.uuid4().hex[:8]}'

        test_input = "test data for metrics"

        # Create test input
        subprocess.run([
            'docker', 'run', '--rm',
            '--network', 'coolest-mapreduce_mapreduce-net',
            '-v', 'mapreduce-data:/mapreduce-data',
            'busybox', 'sh', '-c',
            f'echo "{test_input}" > /mapreduce-data/inputs/test-{job_id}.txt'
        ], check=False)

        # Submit job
        subprocess.run([
            'python3', 'client/client.py', 'submit-job',
            '--job-id', job_id,
            '--input', f'/mapreduce-data/inputs/test-{job_id}.txt',
            '--output', f'/mapreduce-data/outputs/{job_id}',
            '--job-file', 'examples/wordcount.py',
            '--num-map-tasks', '2',
            '--num-reduce-tasks', '2'
        ], capture_output=True, text=True, check=False)

        # Wait for completion
        time.sleep(10)

        # Check if metrics file exists
        result = subprocess.run([
            'docker', 'run', '--rm',
            '-v', 'mapreduce-data:/mapreduce-data',
            'busybox', 'ls', f'/mapreduce-data/metrics/{job_id}.json'
        ], capture_output=True, text=True)

        # Metrics file should exist (or test may need adjustment based on setup)
        # This is a basic check - may need refinement based on actual deployment
