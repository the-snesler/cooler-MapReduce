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
        """Start coordinator server for tests and create test files."""
        # Check if coordinator is already running (e.g., from Docker)
        import socket
        coordinator_running = False
        try:
            test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            test_socket.settimeout(1)
            result = test_socket.connect_ex(('localhost', 50051))
            test_socket.close()
            if result == 0:
                # Port is in use - check if it's actually a coordinator
                try:
                    import grpc
                    channel = grpc.insecure_channel('localhost:50051')
                    grpc.channel_ready_future(channel).result(timeout=2)
                    channel.close()
                    coordinator_running = True
                    print("Using existing coordinator on port 50051")
                except:
                    # Not a coordinator or not responding, we'll start our own
                    pass
        except Exception as e:
            print(f"Warning: Could not check for existing coordinator: {e}")
        
        cls.coordinator_running_externally = coordinator_running
        
        print("\n" + "="*60)
        print("SETUP: Creating test files and starting coordinator")
        print("="*60)
        
        # Create test files needed for tests
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        shared_dir = os.path.join(base_dir, 'shared')
        print(f"Base directory: {base_dir}")
        
        # Create input test file
        input_dir = os.path.join(shared_dir, 'input')
        os.makedirs(input_dir, exist_ok=True)
        test_input_file = os.path.join(input_dir, 'test.txt')
        print(f"Creating input file: {test_input_file}")
        with open(test_input_file, 'w') as f:
            f.write("hello world\n")
            f.write("hello mapreduce\n")
            f.write("test data\n")
        print(f"  ✓ Created input file with 3 lines")
        
        # Create job test file
        jobs_dir = os.path.join(shared_dir, 'jobs')
        os.makedirs(jobs_dir, exist_ok=True)
        test_job_file = os.path.join(jobs_dir, 'test.py')
        print(f"Creating job file: {test_job_file}")
        with open(test_job_file, 'w') as f:
            f.write("def map_fn(key, value):\n")
            f.write("    for word in value.split():\n")
            f.write("        yield (word, 1)\n")
            f.write("\n")
            f.write("def reduce_fn(key, values):\n")
            f.write("    yield (key, sum(values))\n")
        print(f"  ✓ Created job file with map_fn and reduce_fn")
        
        # Create output directory
        output_dir = os.path.join(shared_dir, 'output')
        os.makedirs(output_dir, exist_ok=True)
        print(f"  ✓ Created output directory")
        
        # Start coordinator server only if not already running
        if not coordinator_running:
            print(f"\nStarting local coordinator server...")
            # Use absolute path to coordinator script to ensure it runs from correct location
            coordinator_script = os.path.join(base_dir, 'src', 'coordinator', 'server.py')
            print(f"  Coordinator script: {coordinator_script}")
            cls.coordinator_process = subprocess.Popen(
                ['python3', coordinator_script],
                cwd=base_dir,  # Set working directory explicitly
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=dict(os.environ, PYTHONPATH=base_dir)  # Ensure Python can find modules
            )
            # Give the server time to start
            print("  Waiting for coordinator to start...")
            time.sleep(2)
            
            # Check if coordinator started successfully
            if cls.coordinator_process.poll() is not None:
                # Process already terminated
                stdout, stderr = cls.coordinator_process.communicate()
                print("✗ Coordinator failed to start!")
                print("STDOUT:", stdout.decode() if stdout else "")
                print("STDERR:", stderr.decode() if stderr else "")
                raise RuntimeError("Coordinator server failed to start")
            print("  ✓ Coordinator started successfully")
        else:
            cls.coordinator_process = None  # No process to manage
            print(f"\n✓ Using existing coordinator (Docker or external)")
        
        # Store base_dir for use in tests
        cls.base_dir = base_dir
        print("\n" + "="*60)
        print("SETUP COMPLETE - Starting tests...")
        print("="*60)
    
    @classmethod
    def tearDownClass(cls):
        """Stop coordinator server."""
        # Only stop if we started it ourselves
        if not getattr(cls, 'coordinator_running_externally', False) and cls.coordinator_process:
            cls.coordinator_process.send_signal(signal.SIGINT)
            cls.coordinator_process.wait(timeout=5)
    
    def test_01_coordinator_connection(self):
        """Test that we can connect to the coordinator."""
        print("\n" + "="*60)
        print("TEST 1: Coordinator Connection")
        print("="*60)
        print("Attempting to connect to coordinator at localhost:50051...")
        
        with grpc.insecure_channel('localhost:50051') as channel:
            # Wait for channel to be ready
            grpc.channel_ready_future(channel).result(timeout=5)
            print("✓ Successfully connected to coordinator!")
            self.assertTrue(True, "Successfully connected to coordinator")
    
    def test_02_submit_job(self):
        """Test job submission."""
        print("\n" + "="*60)
        print("TEST 2: Submit Job")
        print("="*60)
        
        with grpc.insecure_channel('localhost:50051') as channel:
            stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
            
            # Determine path format based on whether coordinator is in Docker
            # If coordinator is external (Docker), use /shared paths
            # Otherwise use absolute host paths
            if getattr(TestWeek1Infrastructure, 'coordinator_running_externally', False):
                # Coordinator is in Docker - use /shared paths
                input_path = '/shared/input/test.txt'
                output_path = '/shared/output/test'
                job_file_path = '/shared/jobs/test.py'
                print("Using Docker coordinator - paths: /shared/...")
            else:
                # Coordinator is local - use absolute paths
                base_dir = TestWeek1Infrastructure.base_dir
                input_path = os.path.abspath(os.path.join(base_dir, 'shared', 'input', 'test.txt'))
                output_path = os.path.abspath(os.path.join(base_dir, 'shared', 'output', 'test'))
                job_file_path = os.path.abspath(os.path.join(base_dir, 'shared', 'jobs', 'test.py'))
                print(f"Using local coordinator - base_dir: {base_dir}")
            
            print(f"Input path: {input_path}")
            print(f"Output path: {output_path}")
            print(f"Job file path: {job_file_path}")
            
            # Verify files exist before submitting (only for local paths)
            if not getattr(TestWeek1Infrastructure, 'coordinator_running_externally', False):
                # For local coordinator, verify files exist
                print(f"Checking file existence...")
                print(f"  Input file exists: {os.path.exists(input_path)}")
                print(f"  Job file exists: {os.path.exists(job_file_path)}")
                self.assertTrue(os.path.exists(input_path), f"Input file must exist: {input_path}")
                self.assertTrue(os.path.exists(job_file_path), f"Job file must exist: {job_file_path}")
            # For Docker coordinator, files should be accessible via /shared mount
            
            request = coordinator_pb2.JobRequest(
                input_path=input_path,
                output_path=output_path,
                job_file_path=job_file_path,
                num_map_tasks=4,
                num_reduce_tasks=2
            )
            
            print(f"\nSubmitting job with:")
            print(f"  Map tasks: {request.num_map_tasks}")
            print(f"  Reduce tasks: {request.num_reduce_tasks}")
            
            try:
                response = stub.SubmitJob(request)
                print(f"\n✓ Job submitted successfully!")
                print(f"  Job ID: {response.job_id}")
                print(f"  Status: {response.status}")
            except grpc.RpcError as e:
                print(f"\n✗ Job submission failed!")
                print(f"  Error code: {e.code()}")
                print(f"  Error details: {e.details()}")
                # If it fails, check coordinator logs
                if hasattr(TestWeek1Infrastructure, 'coordinator_process'):
                    proc = TestWeek1Infrastructure.coordinator_process
                    # Read available output
                    import select
                    import fcntl
                    if proc.stderr:
                        try:
                            # Make non-blocking
                            flags = fcntl.fcntl(proc.stderr, fcntl.F_GETFL)
                            fcntl.fcntl(proc.stderr, fcntl.F_SETFL, flags | os.O_NONBLOCK)
                            stderr_data = proc.stderr.read(4096)
                            if stderr_data:
                                print("\n=== Coordinator STDERR (last 4KB) ===")
                                print(stderr_data.decode('utf-8', errors='ignore'))
                        except Exception as read_err:
                            print(f"Could not read stderr: {read_err}")
                print(f"\nTest process CWD: {os.getcwd()}")
                print(f"File exists from test: {os.path.exists(input_path)}")
                raise
            
            self.assertIsNotNone(response.job_id)
            # Coordinator returns 'SUBMITTED' in response, but job state is 'MAPPING'
            self.assertIn(response.status, ['SUBMITTED', 'MAPPING'])
            
            # Store job_id for next tests
            TestWeek1Infrastructure.test_job_id = response.job_id
            print(f"  Stored job_id for subsequent tests")
    
    def test_03_get_job_status(self):
        """Test getting job status."""
        print("\n" + "="*60)
        print("TEST 3: Get Job Status")
        print("="*60)
        
        self.assertIsNotNone(TestWeek1Infrastructure.test_job_id, "Job must be submitted first")
        job_id = TestWeek1Infrastructure.test_job_id
        print(f"Querying status for job: {job_id}")
        
        with grpc.insecure_channel('localhost:50051') as channel:
            stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
            
            request = coordinator_pb2.JobStatusRequest(
                job_id=job_id
            )
            
            response = stub.GetJobStatus(request)
            
            print(f"\n✓ Job status retrieved:")
            print(f"  Job ID: {response.job_id}")
            print(f"  Status: {response.status}")
            print(f"  Map tasks: {response.completed_map_tasks}/{response.total_map_tasks}")
            print(f"  Reduce tasks: {response.completed_reduce_tasks}/{response.total_reduce_tasks}")
            if response.error_message:
                print(f"  Error: {response.error_message}")
            
            self.assertEqual(response.job_id, job_id)
            # Job status could be SUBMITTED or MAPPING depending on timing
            self.assertIn(response.status, ['SUBMITTED', 'MAPPING'])
            self.assertEqual(response.total_map_tasks, 4)
            self.assertEqual(response.total_reduce_tasks, 2)
    
    def test_04_list_jobs(self):
        """Test listing all jobs."""
        print("\n" + "="*60)
        print("TEST 4: List All Jobs")
        print("="*60)
        print("Requesting list of all jobs...")
        
        with grpc.insecure_channel('localhost:50051') as channel:
            stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
            
            request = coordinator_pb2.Empty()
            
            response = stub.ListJobs(request)
            
            print(f"\n✓ Retrieved {len(response.jobs)} job(s):")
            for i, job in enumerate(response.jobs, 1):
                marker = " ← Our test job" if job.job_id == TestWeek1Infrastructure.test_job_id else ""
                print(f"  {i}. Job ID: {job.job_id}, Status: {job.status}, Submitted: {job.submit_time}{marker}")
            
            self.assertGreater(len(response.jobs), 0)
            # Verify our test job is in the list
            job_ids = [job.job_id for job in response.jobs]
            self.assertIn(TestWeek1Infrastructure.test_job_id, job_ids)
            print(f"\n✓ Test job found in list!")
    
    def test_05_get_job_results(self):
        """Test getting job results."""
        print("\n" + "="*60)
        print("TEST 5: Get Job Results")
        print("="*60)
        
        self.assertIsNotNone(TestWeek1Infrastructure.test_job_id, "Job must be submitted first")
        job_id = TestWeek1Infrastructure.test_job_id
        print(f"Requesting results for job: {job_id}")
        
        with grpc.insecure_channel('localhost:50051') as channel:
            stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
            
            request = coordinator_pb2.JobResultsRequest(
                job_id=job_id
            )
            
            response = stub.GetJobResults(request)
            
            print(f"\n✓ Job results retrieved:")
            print(f"  Job ID: {response.job_id}")
            print(f"  Status: {response.status}")
            print(f"  Output files: {len(response.output_files)}")
            if response.output_files:
                print("  Output file list:")
                for i, output_file in enumerate(response.output_files, 1):
                    print(f"    {i}. {output_file}")
            else:
                print("  (No output files yet - job may still be running)")
            
            self.assertEqual(response.job_id, job_id)
            # Job status could be SUBMITTED, MAPPING, or other states depending on timing
            self.assertIn(response.status, ['SUBMITTED', 'MAPPING', 'REDUCING', 'COMPLETED'])
            # No output files yet since job hasn't run
            self.assertEqual(len(response.output_files), 0)


if __name__ == '__main__':
    unittest.main()
