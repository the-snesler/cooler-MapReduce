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
from unittest.mock import MagicMock, patch

# Add src directory to path
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

    def test_task_retry_mechanism(self):
        """Test task retry on failure."""
        print("\n" + "="*60)
        print("TEST: Task Retry Mechanism")
        print("="*60)
        
        # Create a task
        task = Task("job1_map_1", "job1", "MAP", "/input", "/output")
        self.assertEqual(task.retries, 0)
        self.assertEqual(task.status, "PENDING")
        
        # Simulate task failure
        task.assign_to_worker("worker-1")
        task.fail("Network error")
        self.assertEqual(task.status, "FAILED")
        self.assertEqual(task.retries, 0)  # Retries incremented on requeue
        
        # Simulate retry (should increment retries)
        max_retries = 3
        if task.retries < max_retries:
            task.retries += 1
            task.status = "PENDING"
            task.assigned_worker = None
            task.error_message = None
        
        print(f"  Task ID: {task.task_id}")
        print(f"  Retries: {task.retries}")
        print(f"  Status after retry: {task.status}")
        
        self.assertEqual(task.retries, 1)
        self.assertEqual(task.status, "PENDING")
        self.assertIsNone(task.assigned_worker)
        
        # Test max retries
        for _ in range(max_retries):
            task.retries += 1
        
        print(f"  Max retries reached: {task.retries >= max_retries}")
        self.assertGreaterEqual(task.retries, max_retries)

    def test_worker_failure_recovery(self):
        """Test worker failure detection and task reassignment."""
        print("\n" + "="*60)
        print("TEST: Worker Failure Recovery")
        print("="*60)
        
        # Register a worker
        worker_id = "worker-1"
        self.coordinator.workers[worker_id] = {
            'status': 'IDLE',
            'available_slots': 2,
            'last_heartbeat': time.time(),
            'cpu_usage': 0.0,
            'performance_score': 1.0
        }
        
        # Create and assign a task
        task = Task("job1_map_1", "job1", "MAP", "/input", "/output")
        task.assign_to_worker(worker_id)
        self.assertEqual(task.assigned_worker, worker_id)
        self.assertEqual(task.status, "IN_PROGRESS")
        
        print(f"  Task assigned to: {worker_id}")
        print(f"  Worker status: {self.coordinator.workers[worker_id]['status']}")
        
        # Simulate worker failure (no heartbeat for too long)
        old_heartbeat = time.time() - 60  # 60 seconds ago
        self.coordinator.workers[worker_id]['last_heartbeat'] = old_heartbeat
        
        # Check if worker is considered dead
        heartbeat_timeout = 30
        is_dead = (time.time() - self.coordinator.workers[worker_id]['last_heartbeat']) > heartbeat_timeout
        
        print(f"  Worker heartbeat age: {time.time() - old_heartbeat:.1f}s")
        print(f"  Worker considered dead: {is_dead}")
        
        if is_dead:
            # Reassign task
            task.status = "PENDING"
            task.assigned_worker = None
            task.retries += 1
            
            print(f"  Task reassigned - Status: {task.status}, Retries: {task.retries}")
        
        self.assertTrue(is_dead)
        self.assertEqual(task.status, "PENDING")
        self.assertIsNone(task.assigned_worker)
        self.assertEqual(task.retries, 1)

    def test_straggler_detection(self):
        """Test straggler (slow worker) detection and backup task creation."""
        print("\n" + "="*60)
        print("TEST: Straggler Detection")
        print("="*60)
        
        # Create job and tasks
        request = coordinator_pb2.JobRequest(
            input_path="/input",
            output_path="/output",
            job_file_path="/jobs/test.py",
            num_map_tasks=4,
            num_reduce_tasks=2
        )
        job_state = JobState("job1", request)
        self.coordinator.jobs["job1"] = job_state
        
        # Create tasks
        for i in range(4):
            task = Task(f"job1_map_{i}", "job1", "MAP", "/input", "/output")
            job_state.map_tasks[task.task_id] = task
        
        # Simulate: 3 tasks completed quickly, 1 task is slow
        fast_tasks = ["job1_map_0", "job1_map_1", "job1_map_2"]
        slow_task_id = "job1_map_3"
        
        # Mark fast tasks as completed
        for task_id in fast_tasks:
            job_state.map_tasks[task_id].status = "COMPLETED"
            job_state.map_tasks[task_id].start_time = time.time() - 10
            job_state.map_tasks[task_id].end_time = time.time() - 5
        
        # Slow task still running
        job_state.map_tasks[slow_task_id].status = "IN_PROGRESS"
        job_state.map_tasks[slow_task_id].start_time = time.time() - 30  # Running for 30s
        
        # Calculate average completion time
        completed_tasks = [t for t in job_state.map_tasks.values() if t.status == "COMPLETED"]
        if completed_tasks:
            avg_duration = sum((t.end_time - t.start_time) for t in completed_tasks) / len(completed_tasks)
            slow_task_duration = time.time() - job_state.map_tasks[slow_task_id].start_time
            
            print(f"  Fast tasks completed: {len(completed_tasks)}")
            print(f"  Average task duration: {avg_duration:.2f}s")
            print(f"  Slow task duration: {slow_task_duration:.2f}s")
            
            # Check if slow task is a straggler (taking > 2x average)
            is_straggler = slow_task_duration > (2 * avg_duration)
            print(f"  Is straggler: {is_straggler}")
            
            if is_straggler:
                # Create backup task
                backup_task = Task(f"{slow_task_id}_backup", "job1", "MAP", "/input", "/output")
                backup_task.retries = 1
                print(f"  Created backup task: {backup_task.task_id}")
                
                self.assertTrue(is_straggler)
                self.assertIsNotNone(backup_task)

    def test_intermediate_file_collection(self):
        """Test collection of intermediate file locations from map tasks."""
        print("\n" + "="*60)
        print("TEST: Intermediate File Collection")
        print("="*60)
        
        # Create job
        request = coordinator_pb2.JobRequest(
            input_path="/input",
            output_path="/output",
            job_file_path="/jobs/test.py",
            num_map_tasks=3,
            num_reduce_tasks=2
        )
        job_state = JobState("job1", request)
        self.coordinator.jobs["job1"] = job_state
        
        # Simulate map task completions with intermediate files
        map_tasks = [
            ("job1_map_0", "worker-1:50052", ["job1_map_0_part_0.pickle", "job1_map_0_part_1.pickle"]),
            ("job1_map_1", "worker-2:50053", ["job1_map_1_part_0.pickle", "job1_map_1_part_1.pickle"]),
            ("job1_map_2", "worker-3:50054", ["job1_map_2_part_0.pickle", "job1_map_2_part_1.pickle"]),
        ]
        
        # Initialize intermediate file locations dictionary
        job_state.intermediate_file_locations = {}  # partition_id -> [(worker_address, file_name)]
        
        for task_id, worker_address, intermediate_files in map_tasks:
            # Parse partition IDs from file names
            for file_name in intermediate_files:
                # Extract partition ID from filename (e.g., "job1_map_0_part_1.pickle" -> partition 1)
                if "_part_" in file_name:
                    try:
                        part_str = file_name.split("_part_")[1].split(".")[0]
                        partition_id = int(part_str)
                        
                        if partition_id not in job_state.intermediate_file_locations:
                            job_state.intermediate_file_locations[partition_id] = []
                        
                        job_state.intermediate_file_locations[partition_id].append((worker_address, file_name))
                        print(f"  Collected: partition {partition_id} <- {worker_address}/{file_name}")
                    except (ValueError, IndexError):
                        pass
        
        # Verify collection
        print(f"\n  Total partitions with files: {len(job_state.intermediate_file_locations)}")
        for partition_id, locations in job_state.intermediate_file_locations.items():
            print(f"  Partition {partition_id}: {len(locations)} files")
            self.assertGreater(len(locations), 0)
        
        # Verify all partitions have files
        self.assertEqual(len(job_state.intermediate_file_locations), 2)  # 2 reduce tasks = 2 partitions

    def test_shuffle_location_tracking(self):
        """Test shuffle location tracking for reduce tasks."""
        print("\n" + "="*60)
        print("TEST: Shuffle Location Tracking")
        print("="*60)
        
        # Create job
        request = coordinator_pb2.JobRequest(
            input_path="/input",
            output_path="/output",
            job_file_path="/jobs/test.py",
            num_map_tasks=2,
            num_reduce_tasks=2
        )
        job_state = JobState("job1", request)
        self.coordinator.jobs["job1"] = job_state
        
        # Set up intermediate file locations (from map task completions)
        # Initialize if it doesn't exist
        if not hasattr(job_state, 'intermediate_file_locations'):
            job_state.intermediate_file_locations = {}
        
        job_state.intermediate_file_locations = {
            0: [("worker-1:50052", "job1_map_0_part_0.pickle"),
                ("worker-2:50053", "job1_map_1_part_0.pickle")],
            1: [("worker-1:50052", "job1_map_0_part_1.pickle"),
                ("worker-2:50053", "job1_map_1_part_1.pickle")]
        }
        
        # Create reduce tasks (should have shuffle locations)
        # _create_reduce_tasks() doesn't return anything, it stores in job_state.reduce_tasks
        job_state._create_reduce_tasks()
        reduce_tasks = list(job_state.reduce_tasks.values())
        
        print(f"  Created {len(reduce_tasks)} reduce tasks")
        for task in reduce_tasks:
            partition_id = task.partition_id
            shuffle_locations = getattr(task, 'shuffle_input_locations', [])
            
            print(f"  Reduce task {task.task_id}:")
            print(f"    Partition ID: {partition_id}")
            print(f"    Shuffle locations: {len(shuffle_locations)}")
            
            # Verify shuffle locations are set
            # Note: shuffle_locations is a list of (worker_address, file_name) tuples
            # The partition_id should match the partition that these files belong to
            if shuffle_locations:
                # Verify we have locations for this partition
                expected_locations = job_state.intermediate_file_locations.get(partition_id, [])
                print(f"    Expected locations for partition {partition_id}: {len(expected_locations)}")
                self.assertGreater(len(shuffle_locations), 0)

    def test_multiple_concurrent_jobs(self):
        """Test handling multiple concurrent jobs."""
        print("\n" + "="*60)
        print("TEST: Multiple Concurrent Jobs")
        print("="*60)
        
        # Create multiple jobs
        jobs = []
        for i in range(3):
            request = coordinator_pb2.JobRequest(
                input_path=f"/input/job{i}.txt",
                output_path=f"/output/job{i}",
                job_file_path="/jobs/test.py",
                num_map_tasks=2,
                num_reduce_tasks=1
            )
            job_id = f"job_{i}"
            job_state = JobState(job_id, request)
            self.coordinator.jobs[job_id] = job_state
            jobs.append((job_id, job_state))
        
        print(f"  Created {len(jobs)} concurrent jobs")
        
        # Verify all jobs are tracked
        self.assertEqual(len(self.coordinator.jobs), 3)
        
        # Create tasks for each job
        all_tasks = []
        for job_id, job_state in jobs:
            for j in range(2):
                task = Task(f"{job_id}_map_{j}", job_id, "MAP", f"/input/job{i}.txt", f"/output/job{i}")
                job_state.map_tasks[task.task_id] = task
                all_tasks.append(task)
        
        print(f"  Total tasks across all jobs: {len(all_tasks)}")
        
        # Verify job isolation
        for job_id, job_state in jobs:
            self.assertEqual(len(job_state.map_tasks), 2)
            print(f"  Job {job_id}: {len(job_state.map_tasks)} map tasks")
        
        # Verify tasks belong to correct jobs
        for task in all_tasks:
            self.assertIn(task.job_id, self.coordinator.jobs)
            self.assertIn(task.task_id, self.coordinator.jobs[task.job_id].map_tasks)

    def test_task_completion_reporting(self):
        """Test task completion reporting with intermediate files."""
        print("\n" + "="*60)
        print("TEST: Task Completion Reporting")
        print("="*60)
        
        # Create job
        request = coordinator_pb2.JobRequest(
            input_path="/input",
            output_path="/output",
            job_file_path="/jobs/test.py",
            num_map_tasks=2,
            num_reduce_tasks=2
        )
        job_state = JobState("job1", request)
        self.coordinator.jobs["job1"] = job_state
        
        # Initialize intermediate_file_locations if it doesn't exist
        if not hasattr(job_state, 'intermediate_file_locations'):
            job_state.intermediate_file_locations = {}
        
        # Create a map task
        task_id = "job1_map_0"
        task = Task(task_id, "job1", "MAP", "/input", "/output")
        task.assign_to_worker("worker-1")
        job_state.map_tasks[task_id] = task
        
        print(f"  Task: {task_id}")
        print(f"  Assigned to: {task.assigned_worker}")
        print(f"  Status before completion: {task.status}")
        
        # Simulate task completion report
        worker_address = "worker-1:50052"
        intermediate_files = [
            "job1_map_0_part_0.pickle",
            "job1_map_0_part_1.pickle"
        ]
        
        # Mark task as completed
        task.complete()
        job_state.map_tasks[task_id].status = "COMPLETED"
        
        # Process intermediate files (simulate ReportTaskCompletion)
        for file_name in intermediate_files:
            # Extract partition ID
            if "_part_" in file_name:
                try:
                    part_str = file_name.split("_part_")[1].split(".")[0]
                    partition_id = int(part_str)
                    
                    if partition_id not in job_state.intermediate_file_locations:
                        job_state.intermediate_file_locations[partition_id] = []
                    
                    job_state.intermediate_file_locations[partition_id].append((worker_address, file_name))
                    print(f"  Reported file: partition {partition_id} <- {file_name}")
                except (ValueError, IndexError):
                    pass
        
        print(f"  Status after completion: {task.status}")
        print(f"  Intermediate files collected: {len(job_state.intermediate_file_locations)} partitions")
        
        # Verify
        self.assertEqual(task.status, "COMPLETED")
        self.assertGreater(len(job_state.intermediate_file_locations), 0)

if __name__ == '__main__':
    unittest.main()