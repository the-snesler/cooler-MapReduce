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
from unittest.mock import MagicMock, patch # <--- ADDED: for mocking gRPC communication

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import coordinator_pb2
import coordinator_pb2_grpc
import worker_pb2
import worker_pb2_grpc
from coordinator.server import CoordinatorServicer, Task, TaskScheduler, JobState

# --- Mocking Helper for Shuffle Test ---
# We need to simulate the Worker reporting task completion and its output files.
# Since we can't fully run a gRPC server here, we'll mock the Coordinator's 
# response to a Task Assignment so we can verify the Reduce Task details later.
# You will need to implement a ReportTaskCompletion method in your CoordinatorServicer 
# in the coordinator/server.py file for this test to work in a real environment.

# Updated mock_assign_task (requires self argument to be the TaskScheduler instance)
def mock_assign_task(scheduler_instance, worker_id, assignment, task):
    """Mock the task assignment to prevent gRPC error and track assigned task details."""
    # 1. Update Task status (original logic)
    task.assigned_worker = worker_id
    task.status = "IN_PROGRESS"
    
    # 2. **CRITICAL FIX**: Decrease the worker's available slots in the Coordinator's state.
    # We must access the coordinator through the scheduler instance.
    with scheduler_instance.lock:
        if worker_id in scheduler_instance.coordinator_servicer.workers:
            scheduler_instance.coordinator_servicer.workers[worker_id]['available_slots'] -= 1
            if scheduler_instance.coordinator_servicer.workers[worker_id]['available_slots'] == 0:
                scheduler_instance.coordinator_servicer.workers[worker_id]['status'] = "BUSY"
# ---------------------------------------

class TestTaskScheduling(unittest.TestCase):
    # ... (Keep existing methods: test_task_creation, test_task_state_transitions, test_job_state_progress) ...
    
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
        
        # Set up test directory and intermediate files
        test_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                               "shared/intermediate/test_job")
        os.makedirs(test_dir, exist_ok=True)
        
        # Create intermediate files
        intermediate_files = [
            os.path.join(test_dir, f"map_{i}.txt") for i in range(4)
        ]
        for file in intermediate_files:
            with open(file, "w") as f:
                f.write("test data")
        
        # Start in map phase
        job_state.transition_to_map_phase()
        
        # Add some map tasks
        for i in range(4):
            task = Task(f"test_job_map_{i}", "test_job", "MAP", "/input", "/output")
            job_state.map_tasks[task.task_id] = task
        
        # Set intermediate files for validation
        job_state.intermediate_files = intermediate_files
        
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
        
        # Clean up test files
        import shutil
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
        self.assertEqual(job_state.status, "REDUCING")


class TestTaskScheduler(unittest.TestCase):
    """Test task scheduler functionality and the Shuffle setup."""
    
    def setUp(self):
        # Patch the function that sends gRPC assignments to prevent errors
        self.patcher = patch.object(TaskScheduler, '_send_assignment_to_worker', new=mock_assign_task)
        self.patcher.start()
        
        self.coordinator = CoordinatorServicer()
        self.scheduler = self.coordinator.task_scheduler
        
        # Create a mock job for testing
        mock_request = MagicMock(
            input_path="/input/data.txt",
            output_path="/output/results",
            job_file_path="/jobs/job.py",
            num_map_tasks=2,
            num_reduce_tasks=1
        )
        self.job_id = "job-shuffle-test"
        self.job_state = JobState(self.job_id, mock_request)
        self.coordinator.jobs[self.job_id] = self.job_state
        
        # Initialize workers
        self.coordinator.handle_worker_heartbeat("worker-1", "IDLE", 5, 20)
        self.coordinator.workers["worker-1"]["address"] = "worker-1-addr:50052"
        self.coordinator.handle_worker_heartbeat("worker-2", "IDLE", 5, 30)
        self.coordinator.workers["worker-2"]["address"] = "worker-2-addr:50053"
        
    def tearDown(self):
        self.patcher.stop()

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
            'performance_score': 1.0,
            'address': "worker-1-addr:50052" # Must be present
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
    
    def test_shuffle_setup(self):
        """
        Test the Coordinator's core role in the Shuffle: collecting Map outputs
        and correctly configuring the Reduce task input.
        """
        job_id = self.job_id
        num_reducers = self.job_state.num_reduce_tasks # 1
        
        # 1. Simulate Map Task Completion Reports (The Missing Link)
        
        if not hasattr(self.job_state, 'intermediate_file_locations'):
             # Create the structure the coordinator should use: {partition_id: List[(address, file)]}
             self.job_state.intermediate_file_locations = {i: [] for i in range(num_reducers)}
             
        # Mock the function that would extract the partition ID from the filename
        def get_partition_id(file_name):
            return int(file_name.split('_part_')[-1].split('.')[0])
        
        # --- Worker 1 (Map 0) reports its files for all partitions ---
        # NOTE: Must create a Task object mock for the job state tracking to pass
        self.job_state.map_tasks["map_0"] = MagicMock(status="COMPLETED")
        
        map_0_files = [
            f"{job_id}_map_0_part_0.pickle" # File for Reducer 0
        ]
        
        for file in map_0_files:
            partition = get_partition_id(file)
            self.job_state.intermediate_file_locations[partition].append(
                (self.coordinator.workers["worker-1"]["address"], file)
            )

        # --- Worker 2 (Map 1) reports its files for all partitions ---
        self.job_state.map_tasks["map_1"] = MagicMock(status="COMPLETED")
        
        map_1_files = [
            f"{job_id}_map_1_part_0.pickle" # File for Reducer 0
        ]
        
        for file in map_1_files:
            partition = get_partition_id(file)
            self.job_state.intermediate_file_locations[partition].append(
                (self.coordinator.workers["worker-2"]["address"], file)
            )
            
        # 2. Simulate Map Phase Completion and Transition to Reduce Phase
        
        # FIX: Transition to MAPPING first, and set the counter
        self.job_state.transition_to_map_phase() # <--- ADDED: Necessary to enter MAPPING state
        self.job_state.completed_map_tasks = self.job_state.num_map_tasks # Sets counter to 2
        
        # Mock intermediate file validation to succeed
        self.job_state.validate_intermediate_files = MagicMock(return_value=True) 

        # Force the transition (This should now pass the 'maps not complete' check)
        self.job_state.transition_to_reduce_phase()
        
        # 3. Verify Reduce Task Creation and Shuffle Input Setup
        self.assertEqual(self.job_state.status, "REDUCING")
        self.assertEqual(len(self.job_state.reduce_tasks), num_reducers)
        
        reduce_task_id = f"{job_id}_reduce_0"
        reduce_task = self.job_state.reduce_tasks[reduce_task_id]
        
        self.assertEqual(reduce_task.task_type, "REDUCE")
        self.assertEqual(reduce_task.partition_id, 0)
        
        # Check if the Reduce Task has all the necessary input locations for Partition 0
        self.assertTrue(hasattr(reduce_task, 'shuffle_input_locations'), 
                        "Task object must have 'shuffle_input_locations' attribute")
                        
        shuffle_locations = sorted(reduce_task.shuffle_input_locations)
        
        expected_locations = sorted([
            (self.coordinator.workers["worker-1"]["address"], f"{job_id}_map_0_part_0.pickle"),
            (self.coordinator.workers["worker-2"]["address"], f"{job_id}_map_1_part_0.pickle")
        ])

        self.assertEqual(shuffle_locations, expected_locations,
                         "Reduce task must be assigned file locations from all Map Workers.")
                         
        # 4. Simulate Scheduler assigning the Reduce Task
        with self.scheduler.lock:
            self.scheduler.add_task(reduce_task)
            self.scheduler._assign_pending_tasks()
        
        self.assertEqual(reduce_task.assigned_worker, "worker-1")


if __name__ == '__main__':
    unittest.main()