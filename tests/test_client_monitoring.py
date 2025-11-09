"""
Unit tests for MapReduce client monitoring functionality.
"""

import unittest
from unittest.mock import MagicMock, patch
import io
import sys
from datetime import datetime

from src.client.monitoring import (
    format_duration,
    format_progress_bar,
    monitor_job_progress,
    cancel_job,
    list_active_tasks,
    show_resource_usage
)

class TestClientMonitoring(unittest.TestCase):
    def setUp(self):
        self.mock_stub = MagicMock()
        
    def test_format_duration(self):
        """Test duration formatting for different time ranges."""
        self.assertEqual(format_duration(30), "30.0s")
        self.assertEqual(format_duration(90), "1m 30.0s")
        self.assertEqual(format_duration(3675), "1h 1m 15.0s")
        
    def test_format_progress_bar(self):
        """Test progress bar generation."""
        # Test empty progress
        bar = format_progress_bar(0, 10, width=20)
        self.assertEqual(bar, "[░░░░░░░░░░░░░░░░░░░░] 0.0%")
        
        # Test partial progress
        bar = format_progress_bar(5, 10, width=20)
        self.assertEqual(bar, "[██████████░░░░░░░░░░] 50.0%")
        
        # Test complete progress
        bar = format_progress_bar(10, 10, width=20)
        self.assertEqual(bar, "[████████████████████] 100.0%")
        
    def test_monitor_job_progress_completed(self):
        """Test job monitoring for a completed job."""
        # Mock job status responses
        self.mock_stub.GetJobStatus.side_effect = [
            MagicMock(
                job_id="test-job-1",
                status="COMPLETED",
                phase="REDUCE",
                completed_map_tasks=3,
                total_map_tasks=3,
                completed_reduce_tasks=2,
                total_reduce_tasks=2,
                error_message=""
            )
        ]
        
        # Capture stdout
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            monitor_job_progress(self.mock_stub, "test-job-1", interval=0.1)
            output = captured_output.getvalue()
            
            # Verify output contains expected information
            self.assertIn("Job ID: test-job-1", output)
            self.assertIn("Status: COMPLETED", output)
            self.assertIn("Runtime:", output)
            
        finally:
            sys.stdout = sys.__stdout__
            
    def test_monitor_job_progress_with_error(self):
        """Test job monitoring when job fails."""
        error_msg = "Worker node failure"
        self.mock_stub.GetJobStatus.side_effect = [
            MagicMock(
                job_id="test-job-2",
                status="FAILED",
                phase="MAP",
                completed_map_tasks=1,
                total_map_tasks=3,
                error_message=error_msg
            )
        ]
        
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            monitor_job_progress(self.mock_stub, "test-job-2", interval=0.1)
            output = captured_output.getvalue()
            
            self.assertIn("Status: FAILED", output)
            self.assertIn(error_msg, output)
            
        finally:
            sys.stdout = sys.__stdout__
            
    def test_cancel_job(self):
        """Test job cancellation."""
        # Test successful cancellation
        self.mock_stub.CancelJob.return_value = MagicMock(success=True)
        
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            cancel_job(self.mock_stub, "test-job-3")
            output = captured_output.getvalue()
            
            self.assertIn("Successfully cancelled", output)
            self.mock_stub.CancelJob.assert_called_once()
            
        finally:
            sys.stdout = sys.__stdout__
            
    def test_list_active_tasks(self):
        """Test listing active tasks."""
        mock_map_task = MagicMock(
            task_id="map_1",
            worker_id="worker_1",
            status="RUNNING",
            progress=0.75
        )
        
        mock_reduce_task = MagicMock(
            task_id="reduce_1",
            worker_id="worker_2",
            status="RUNNING",
            progress=0.5
        )
        
        self.mock_stub.ListActiveTasks.return_value = MagicMock(
            current_phase="MAP",
            map_tasks=[mock_map_task],
            reduce_tasks=[mock_reduce_task]
        )
        
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            list_active_tasks(self.mock_stub, "test-job-4")
            output = captured_output.getvalue()
            
            self.assertIn("map_1", output)
            self.assertIn("reduce_1", output)
            self.assertIn("75.0%", output)
            self.assertIn("50.0%", output)
            
        finally:
            sys.stdout = sys.__stdout__
            
    def test_show_resource_usage(self):
        """Test resource usage display."""
        mock_worker_stat = MagicMock(
            worker_id="worker_1",
            cpu_usage=75.5,
            memory_mb=1024.5,
            network_io_mb=50.2,
            disk_io_mb=100.8
        )
        
        self.mock_stub.GetResourceUsage.return_value = MagicMock(
            total_cpu_time=3600,
            peak_memory_mb=2048.0,
            worker_stats=[mock_worker_stat]
        )
        
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            show_resource_usage(self.mock_stub, "test-job-5")
            output = captured_output.getvalue()
            
            self.assertIn("1h 0m 0.0s", output)  # Total CPU time
            self.assertIn("2048.0 MB", output)   # Peak memory
            self.assertIn("75.5%", output)       # CPU usage
            self.assertIn("1024.5 MB", output)   # Memory usage
            self.assertIn("50.2 MB", output)     # Network I/O
            self.assertIn("100.8 MB", output)    # Disk I/O
            
        finally:
            sys.stdout = sys.__stdout__

if __name__ == '__main__':
    unittest.main()