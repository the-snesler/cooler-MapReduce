"""
Integration tests for Week 2 task scheduling and execution.
"""

import unittest
import sys
import os
import time
import threading
import grpc
from datetime import datetime
import logging

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import coordinator_pb2
import coordinator_pb2_grpc
import worker_pb2
import worker_pb2_grpc
from coordinator.server import CoordinatorServicer, Task, TaskScheduler, JobState

class TestTaskScheduling(unittest.TestCase):
    """Test task scheduling functionality."""
    
    def setUp(self):
        self.coordinator = CoordinatorServicer()
    
    def test_task_creation(self):
        """Test task creation with proper metadata."""
        task = Task(
            task_id="test_job_map_1",
            job_id="test_job",
            task_type="MAP",
            input_path="/shared/input/test.txt",
            output_path="/shared/output/test",
            partition_id=0,
            num_reducers=2
        )
        
        self.assertEqual(task.task_id, "test_job_map_1")
        self.assertEqual(task.status, "PENDING")
        self.assertIsNone(task.assigned_worker)
        self.assertEqual(task.retries, 0)
    
    def test_task_state_transitions(self):
        """Test task state transitions."""
        task = Task(
            task_id="test_job_map_1",
            job_id="test_job",
            task_type="MAP",
            input_path="/shared/input/test.txt",
            output_path="/shared/output/test"
        )
        
        # Test assignment
        task.assign_to_worker("worker-1")
        self.assertEqual(task.status, "IN_PROGRESS")
        self.assertEqual(task.assigned_worker, "worker-1")
        self.assertIsNotNone(task.start_time)
        
        # Test completion
        task.complete()
        self.assertEqual(task.status, "COMPLETED")
        self.assertIsNotNone(task.end_time)
        
        # Test failure
        task = Task("test_job_map_2", "test_job", "MAP", "/input", "/output")
        self.assertTrue(task.fail("Test error"))
        self.assertEqual(task.status, "FAILED")
        self.assertEqual(task.error_message, "Test error")
    
    def test_job_state_progress(self):
        """Test job state progress tracking."""
        request = coordinator_pb2.JobRequest(
            input_path="/shared/input/test.txt",
            output_path="/shared/output/test",
            job_file_path="/shared/jobs/test.py",
            num_map_tasks=4,
            num_reduce_tasks=2
        )
        
        job_state = JobState("test_job", request)
        
        # Add some map tasks
        for i in range(4):
            task = Task(f"test_job_map_{i}", "test_job", "MAP", "/input", "/output")
            job_state.map_tasks[task.task_id] = task
        
        # Complete 2 map tasks
        for task_id in list(job_state.map_tasks.keys())[:2]:
            job_state.map_tasks[task_id].status = "COMPLETED"
        
        job_state.update_progress()
        self.assertEqual(job_state.completed_map_tasks, 2)
        self.assertEqual(job_state.phase, "MAP")
        
        # Complete remaining map tasks
        for task_id in list(job_state.map_tasks.keys())[2:]:
            job_state.map_tasks[task_id].status = "COMPLETED"
        
        job_state.update_progress()
        self.assertEqual(job_state.completed_map_tasks, 4)
        self.assertEqual(job_state.phase, "REDUCE")
        self.assertEqual(job_state.status, "REDUCING")

class TestTaskScheduler(unittest.TestCase):
    """Test task scheduler functionality."""
    
    def setUp(self):
        self.coordinator = CoordinatorServicer()
        self.scheduler = TaskScheduler(self.coordinator)
    
    def test_priority_queue(self):
        """Test task priority queue ordering."""
        # Create job request and state
        request = coordinator_pb2.JobRequest(
            input_path="/input",
            output_path="/output",
            job_file_path="/jobs/test.py",
            num_map_tasks=2,
            num_reduce_tasks=1
        )
        job_state = JobState("job1", request)
        self.coordinator.jobs["job1"] = job_state
        
        # Create tasks with different priorities
        task1 = Task("job1_map_1", "job1", "MAP", "/input", "/output")
        task2 = Task("job1_map_2", "job1", "MAP", "/input", "/output")
        task2.retries = 1  # Should get higher priority
        
        # Add tasks to scheduler
        self.scheduler.add_task(task2)
        self.scheduler.add_task(task1)
        
        # Check order (task2 should come first due to retry)
        with self.scheduler.lock:
            first_task = self.scheduler.pending_tasks.get().task
            second_task = self.scheduler.pending_tasks.get().task
        
        self.assertEqual(first_task.task_id, "job1_map_2")
        self.assertEqual(second_task.task_id, "job1_map_1")
    
    def test_worker_performance_tracking(self):
        """Test worker performance scoring."""
        worker_id = "worker-1"
        available_slots = 4
        cpu_usage = 50  # 50% CPU usage
        
        # Initialize worker state
        worker_info = {
            'status': "IDLE",
            'available_slots': available_slots,
            'last_heartbeat': time.time(),
            'cpu_usage': cpu_usage,
            'task_history': [(1, 2.5)],  # Example task history: (task_id, duration)
            'performance_score': 1.0
        }
        
        with self.coordinator.task_scheduler.lock:
            self.coordinator.workers[worker_id] = worker_info
        
        # Update worker through heartbeat
        self.coordinator.handle_worker_heartbeat(
            worker_id=worker_id,
            status="IDLE",
            available_slots=available_slots,
            cpu_usage=cpu_usage
        )
        
        # Verify worker state
        worker_info = self.coordinator.workers[worker_id]
        self.assertEqual(worker_info["status"], "IDLE")
        self.assertEqual(worker_info["available_slots"], available_slots)
        self.assertEqual(worker_info["cpu_usage"], cpu_usage)
        self.assertGreater(worker_info["performance_score"], 0)

if __name__ == '__main__':
    unittest.main()