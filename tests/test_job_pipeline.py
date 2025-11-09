"""
Unit tests for MapReduce job pipeline management.
"""

import unittest
import os
import time
from datetime import datetime
from unittest.mock import MagicMock, patch
from src.coordinator.server import JobState, CoordinatorServicer

class TestJobPipeline(unittest.TestCase):
    def setUp(self):
        # Create mock job request
        self.mock_request = MagicMock()
        self.mock_request.input_path = "test_input.txt"
        self.mock_request.output_path = "test_output.txt"
        self.mock_request.job_file_path = "test_job.py"
        self.mock_request.num_map_tasks = 3
        self.mock_request.num_reduce_tasks = 2
        
        # Initialize job state
        self.job = JobState("test-job-1", self.mock_request)
        
        # Set up test directory structure
        self.test_dir = "shared/intermediate/test_job_1"
        os.makedirs(self.test_dir, exist_ok=True)
        
        # Create temporary intermediate files for testing
        self.intermediate_files = [
            f"{self.test_dir}/map1.txt",
            f"{self.test_dir}/map2.txt",
            f"{self.test_dir}/map3.txt"
        ]
        for file in self.intermediate_files:
            with open(file, "w") as f:
                f.write("test data")
                
    def tearDown(self):
        # Clean up test directory and all its contents
        import shutil
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_job_initial_state(self):
        """Test job initialization."""
        self.assertEqual(self.job.status, "SUBMITTED")
        self.assertEqual(self.job.phase, "MAP")
        self.assertEqual(self.job.completed_map_tasks, 0)
        self.assertEqual(self.job.completed_reduce_tasks, 0)
        
    def test_map_phase_transition(self):
        """Test transition to map phase."""
        self.job.transition_to_map_phase()
        self.assertEqual(self.job.status, "MAPPING")
        self.assertEqual(self.job.phase, "MAP")
        
        # Should not allow transitioning to map phase again
        with self.assertRaises(ValueError):
            self.job.transition_to_map_phase()
            
    def test_reduce_phase_transition(self):
        """Test transition to reduce phase."""
        # Start in map phase
        self.job.transition_to_map_phase()
        self.assertEqual(self.job.status, "MAPPING")
        
        # Try transition before maps are complete
        with self.assertRaises(ValueError):
            self.job.transition_to_reduce_phase()
        
        # Set up map tasks and intermediate files
        for i in range(self.job.num_map_tasks):
            task_id = f"map_{i}"
            self.job.map_tasks[task_id] = MagicMock(status="COMPLETED")
        
        self.job.intermediate_files = self.intermediate_files
        
        # Update completed map tasks count
        self.job.completed_map_tasks = self.job.num_map_tasks
        
        # Now should transition successfully
        self.job.transition_to_reduce_phase()
        self.assertEqual(self.job.status, "REDUCING")
        self.assertEqual(self.job.phase, "REDUCE")
        
    def test_job_completion(self):
        """Test job completion process."""
        self.job.transition_to_map_phase()
        self.job.completed_map_tasks = self.job.num_map_tasks
        self.job.intermediate_files = self.intermediate_files
        self.job.transition_to_reduce_phase()
        
        # Should not mark complete before reduces are done
        with self.assertRaises(ValueError):
            self.job.mark_completed()
            
        # Complete all reduce tasks
        self.job.completed_reduce_tasks = self.job.num_reduce_tasks
        
        # Now should complete successfully
        self.job.mark_completed()
        self.assertEqual(self.job.status, "COMPLETED")
        self.assertIsNotNone(self.job.completion_time)
        
    def test_intermediate_file_validation(self):
        """Test intermediate file validation."""
        # Initially no files
        self.assertFalse(self.job.validate_intermediate_files())
        
        # Add files and validate
        self.job.intermediate_files = self.intermediate_files
        self.assertTrue(self.job.validate_intermediate_files())
        
        # Remove a file and validate again
        os.remove(self.intermediate_files[0])
        self.assertFalse(self.job.validate_intermediate_files())
        
    def test_job_failure_handling(self):
        """Test job failure scenarios."""
        error_msg = "Test error message"
        self.job.mark_failed(error_msg)
        
        self.assertEqual(self.job.status, "FAILED")
        self.assertEqual(self.job.error_message, error_msg)
        self.assertIsNotNone(self.job.completion_time)
        
        # Verify no state changes after failure
        self.job.update_progress()
        self.assertEqual(self.job.status, "FAILED")
        
    def test_job_cancellation(self):
        """Test job cancellation."""
        # Can cancel a running job
        self.assertTrue(self.job.cancel())
        self.assertEqual(self.job.status, "CANCELLED")
        self.assertIsNotNone(self.job.completion_time)
        
        # Cannot cancel completed job
        job2 = JobState("test-job-2", self.mock_request)
        job2.status = "COMPLETED"
        self.assertFalse(job2.cancel())
        
    def test_auto_phase_transitions(self):
        """Test automatic phase transitions during progress updates."""
        self.job.transition_to_map_phase()
        
        # Simulate map task completions
        for i in range(self.job.num_map_tasks):
            task = MagicMock()
            task.status = "COMPLETED"
            self.job.map_tasks[f"map_{i}"] = task
            
        self.job.intermediate_files = self.intermediate_files
        self.job.update_progress()
        self.assertEqual(self.job.status, "REDUCING")
        
        # Simulate reduce task completions
        for i in range(self.job.num_reduce_tasks):
            task = MagicMock()
            task.status = "COMPLETED"
            self.job.reduce_tasks[f"reduce_{i}"] = task
            
        self.job.update_progress()
        self.assertEqual(self.job.status, "COMPLETED")