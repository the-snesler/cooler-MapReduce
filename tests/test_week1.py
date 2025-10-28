"""
Basic integration tests for Week 1 infrastructure.
Tests the core gRPC communication between components.
"""

import unittest
import grpc
import sys
import os
import time
import subprocess
import signal

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import coordinator_pb2
import coordinator_pb2_grpc


class TestWeek1Infrastructure(unittest.TestCase):
    """Tests for Week 1 core infrastructure."""
    
    test_job_id = None
    
    @classmethod
    def setUpClass(cls):
        """Start coordinator server for tests."""
        cls.coordinator_process = subprocess.Popen(
            ['python', 'src/coordinator/server.py'],
            cwd=os.path.join(os.path.dirname(__file__), '..'),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        # Give the server time to start
        time.sleep(2)
    
    @classmethod
    def tearDownClass(cls):
        """Stop coordinator server."""
        cls.coordinator_process.send_signal(signal.SIGINT)
        cls.coordinator_process.wait(timeout=5)
    
    def test_01_coordinator_connection(self):
        """Test that we can connect to the coordinator."""
        with grpc.insecure_channel('localhost:50051') as channel:
            # Wait for channel to be ready
            grpc.channel_ready_future(channel).result(timeout=5)
            self.assertTrue(True, "Successfully connected to coordinator")
    
    def test_02_submit_job(self):
        """Test job submission."""
        with grpc.insecure_channel('localhost:50051') as channel:
            stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
            
            request = coordinator_pb2.JobRequest(
                input_path='/shared/input/test.txt',
                output_path='/shared/output/test',
                job_file_path='/shared/jobs/test.py',
                num_map_tasks=4,
                num_reduce_tasks=2
            )
            
            response = stub.SubmitJob(request)
            
            self.assertIsNotNone(response.job_id)
            self.assertEqual(response.status, 'SUBMITTED')
            
            # Store job_id for next tests
            TestWeek1Infrastructure.test_job_id = response.job_id
    
    def test_03_get_job_status(self):
        """Test getting job status."""
        self.assertIsNotNone(TestWeek1Infrastructure.test_job_id, "Job must be submitted first")
        
        with grpc.insecure_channel('localhost:50051') as channel:
            stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
            
            request = coordinator_pb2.JobStatusRequest(
                job_id=TestWeek1Infrastructure.test_job_id
            )
            
            response = stub.GetJobStatus(request)
            
            self.assertEqual(response.job_id, TestWeek1Infrastructure.test_job_id)
            self.assertEqual(response.status, 'SUBMITTED')
            self.assertEqual(response.total_map_tasks, 4)
            self.assertEqual(response.total_reduce_tasks, 2)
    
    def test_04_list_jobs(self):
        """Test listing all jobs."""
        with grpc.insecure_channel('localhost:50051') as channel:
            stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
            
            request = coordinator_pb2.Empty()
            
            response = stub.ListJobs(request)
            
            self.assertGreater(len(response.jobs), 0)
            # Verify our test job is in the list
            job_ids = [job.job_id for job in response.jobs]
            self.assertIn(TestWeek1Infrastructure.test_job_id, job_ids)
    
    def test_05_get_job_results(self):
        """Test getting job results."""
        self.assertIsNotNone(TestWeek1Infrastructure.test_job_id, "Job must be submitted first")
        
        with grpc.insecure_channel('localhost:50051') as channel:
            stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
            
            request = coordinator_pb2.JobResultsRequest(
                job_id=TestWeek1Infrastructure.test_job_id
            )
            
            response = stub.GetJobResults(request)
            
            self.assertEqual(response.job_id, TestWeek1Infrastructure.test_job_id)
            self.assertEqual(response.status, 'SUBMITTED')
            # No output files yet since job hasn't run
            self.assertEqual(len(response.output_files), 0)


if __name__ == '__main__':
    unittest.main()
