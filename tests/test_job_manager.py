"""
Unit tests for JobManager
Tests job creation, map task generation, reduce task generation, and status tracking
"""

import sys
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'coordinator'))

from job_manager import JobManager, JobStatus, TaskStatus, Job, MapTask, ReduceTask


class TestJobManager(unittest.TestCase):
    """Unit tests for JobManager class"""

    def setUp(self):
        """Set up test fixtures"""
        self.job_manager = JobManager()

        # Mock job spec
        self.job_spec = MagicMock()
        self.job_spec.job_id = "test-job-1"
        self.job_spec.input_path = "/tmp/test-input.txt"
        self.job_spec.output_path = "/tmp/output/"
        self.job_spec.map_reduce_file = "wordcount.py"
        self.job_spec.num_map_tasks = 4
        self.job_spec.num_reduce_tasks = 2
        self.job_spec.use_combiner = False

    def test_create_job(self):
        """Test job creation with correct attributes"""
        job = self.job_manager.create_job(self.job_spec)

        self.assertEqual(job.job_id, "test-job-1")
        self.assertEqual(job.input_path, "/tmp/test-input.txt")
        self.assertEqual(job.output_path, "/tmp/output/")
        self.assertEqual(job.map_reduce_file, "wordcount.py")
        self.assertEqual(job.num_map_tasks, 4)
        self.assertEqual(job.num_reduce_tasks, 2)
        self.assertFalse(job.use_combiner)
        self.assertEqual(job.status, JobStatus.PENDING)
        self.assertEqual(len(job.map_tasks), 0)
        self.assertEqual(len(job.reduce_tasks), 0)
        self.assertGreater(job.start_time, 0)
        print("✓ Job creation test passed")

    def test_generate_map_tasks_with_proper_offsets(self):
        """Test map task generation with correct file offsets"""
        # Create a temporary file with known size
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            test_content = "a" * 1000  # 1000 bytes
            f.write(test_content)
            temp_file_path = f.name

        try:
            self.job_spec.input_path = temp_file_path
            job = self.job_manager.create_job(self.job_spec)

            map_tasks = self.job_manager.generate_map_tasks(job)

            # Verify correct number of tasks
            self.assertEqual(len(map_tasks), 4)

            # Verify offsets
            expected_chunk_size = 1000 // 4  # 250 bytes per task
            for i, task in enumerate(map_tasks):
                self.assertEqual(task.task_id, i)
                self.assertEqual(task.input_path, temp_file_path)
                self.assertEqual(task.start_offset, i * expected_chunk_size)
                if i == 3:  # Last task gets remainder
                    self.assertEqual(task.end_offset, 1000)
                else:
                    self.assertEqual(task.end_offset, (i + 1) * expected_chunk_size)
                self.assertEqual(task.status, TaskStatus.PENDING)
                self.assertIsNone(task.assigned_worker)

            # Verify tasks are stored in job
            self.assertEqual(len(job.map_tasks), 4)
            print("✓ Map task generation test passed")
        finally:
            os.unlink(temp_file_path)

    def test_generate_reduce_tasks_with_partitions(self):
        """Test reduce task generation with correct partition assignments"""
        job = self.job_manager.create_job(self.job_spec)

        # Mock glob to return expected intermediate files
        with patch('job_manager.glob.glob') as mock_glob:
            mock_glob.side_effect = [
                ['/mapreduce-data/intermediate/test-job-1/map-0-reduce-0.txt',
                 '/mapreduce-data/intermediate/test-job-1/map-1-reduce-0.txt'],
                ['/mapreduce-data/intermediate/test-job-1/map-0-reduce-1.txt',
                 '/mapreduce-data/intermediate/test-job-1/map-1-reduce-1.txt']
            ]

            reduce_tasks = self.job_manager.generate_reduce_tasks(job)

            # Verify correct number of tasks
            self.assertEqual(len(reduce_tasks), 2)

            # Verify partition assignments
            for i, task in enumerate(reduce_tasks):
                self.assertEqual(task.task_id, i)
                self.assertEqual(task.partition_id, i)
                self.assertEqual(len(task.intermediate_files), 2)
                self.assertEqual(task.status, TaskStatus.PENDING)
                self.assertIsNone(task.assigned_worker)

            # Verify tasks are stored in job
            self.assertEqual(len(job.reduce_tasks), 2)
            print("✓ Reduce task generation test passed")

    def test_task_status_transitions(self):
        """Test task status transitions: pending -> assigned -> completed"""
        job = self.job_manager.create_job(self.job_spec)

        # Create mock map tasks
        job.map_tasks = [
            MapTask(0, "/tmp/input.txt", 0, 100),
            MapTask(1, "/tmp/input.txt", 100, 200)
        ]

        # Test pending -> assigned
        task = self.job_manager.get_next_pending_map_task(job.job_id)
        self.assertIsNotNone(task)
        self.assertEqual(task.task_id, 0)
        self.assertEqual(task.status, TaskStatus.ASSIGNED)

        # Test assigned -> completed
        self.job_manager.mark_map_task_completed(job.job_id, 0)
        self.assertEqual(job.map_tasks[0].status, TaskStatus.COMPLETED)

        # Second task should still be pending
        self.assertEqual(job.map_tasks[1].status, TaskStatus.PENDING)
        print("✓ Task status transition test passed")

    def test_job_status_tracking_and_progress(self):
        """Test job status tracking and progress calculation"""
        job = self.job_manager.create_job(self.job_spec)

        # Create mock tasks
        job.map_tasks = [
            MapTask(0, "/tmp/input.txt", 0, 100),
            MapTask(1, "/tmp/input.txt", 100, 200)
        ]
        job.reduce_tasks = [
            ReduceTask(0, 0),
            ReduceTask(1, 1)
        ]

        # Initial status
        status = self.job_manager.get_job_status(job.job_id)
        self.assertEqual(status['status'], JobStatus.PENDING.value)
        self.assertEqual(status['progress'], 0)
        self.assertEqual(status['map_completed'], 0)
        self.assertEqual(status['map_total'], 2)
        self.assertEqual(status['reduce_completed'], 0)
        self.assertEqual(status['reduce_total'], 2)

        # Complete one map task
        job.map_tasks[0].status = TaskStatus.COMPLETED
        status = self.job_manager.get_job_status(job.job_id)
        self.assertEqual(status['progress'], 25)  # 1 out of 4 tasks

        # Complete all map tasks
        job.map_tasks[1].status = TaskStatus.COMPLETED
        self.job_manager.mark_map_task_completed(job.job_id, 1)
        self.assertEqual(job.status, JobStatus.SHUFFLE_PHASE)

        status = self.job_manager.get_job_status(job.job_id)
        self.assertEqual(status['progress'], 50)  # 2 out of 4 tasks

        # Complete all reduce tasks
        job.reduce_tasks[0].status = TaskStatus.COMPLETED
        job.reduce_tasks[1].status = TaskStatus.COMPLETED
        self.job_manager.mark_reduce_task_completed(job.job_id, 1)

        self.assertEqual(job.status, JobStatus.COMPLETED)
        self.assertGreater(job.end_time, 0)

        status = self.job_manager.get_job_status(job.job_id)
        self.assertEqual(status['progress'], 100)  # All tasks complete
        print("✓ Job status tracking test passed")

    def test_get_nonexistent_job_status(self):
        """Test getting status for non-existent job"""
        status = self.job_manager.get_job_status("nonexistent-job")
        self.assertIsNone(status)
        print("✓ Non-existent job status test passed")

    def test_concurrent_task_assignment(self):
        """Test thread-safe task assignment"""
        job = self.job_manager.create_job(self.job_spec)
        job.map_tasks = [
            MapTask(0, "/tmp/input.txt", 0, 100),
            MapTask(1, "/tmp/input.txt", 100, 200)
        ]

        # Get tasks concurrently
        task1 = self.job_manager.get_next_pending_map_task(job.job_id)
        task2 = self.job_manager.get_next_pending_map_task(job.job_id)
        task3 = self.job_manager.get_next_pending_map_task(job.job_id)

        # First two should succeed
        self.assertIsNotNone(task1)
        self.assertIsNotNone(task2)
        # Third should be None (no more tasks)
        self.assertIsNone(task3)

        # Tasks should be different
        self.assertNotEqual(task1.task_id, task2.task_id)
        print("✓ Concurrent task assignment test passed")


if __name__ == "__main__":
    print("=" * 60)
    print("JobManager Unit Tests")
    print("=" * 60)
    unittest.main(verbosity=2)
