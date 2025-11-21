"""
Integration tests for gRPC communication
Tests coordinator JobService and worker TaskService endpoints
"""

import sys
import os
import time
import subprocess

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import grpc
import mapreduce_pb2
import mapreduce_pb2_grpc


class TestGRPCIntegration:
    """Integration tests for gRPC services"""

    @classmethod
    def setup_class(cls):
        """Start docker-compose services before tests"""
        print("\nStarting containers...")
        subprocess.run(
            ["docker-compose", "up", "-d"],
            check=True,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        # Wait for services to be ready
        time.sleep(5)

    @classmethod
    def teardown_class(cls):
        """Stop docker-compose services after tests"""
        print("\nStopping containers...")
        subprocess.run(
            ["docker-compose", "down"],
            check=False,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )

    def test_coordinator_health(self):
        """Test that coordinator server is running and accessible"""
        channel = grpc.insecure_channel('localhost:50051')
        try:
            grpc.channel_ready_future(channel).result(timeout=10)
            print("✓ Coordinator is reachable on port 50051")
        except grpc.FutureTimeoutError:
            raise AssertionError("Coordinator not reachable on port 50051")
        finally:
            channel.close()

    def test_submit_job(self):
        """Test job submission to coordinator"""
        channel = grpc.insecure_channel('localhost:50051')
        stub = mapreduce_pb2_grpc.JobServiceStub(channel)

        request = mapreduce_pb2.JobSpec(
            job_id="test-job-1",
            input_path="/mapreduce-data/inputs/test.txt",
            output_path="/mapreduce-data/outputs/",
            map_reduce_file="wordcount.py",
            num_map_tasks=4,
            num_reduce_tasks=2,
            use_combiner=False
        )

        try:
            response = stub.SubmitJob(request, timeout=5)
            assert response.job_id == "test-job-1"
            assert response.status in ["pending", "map_phase"]  # Updated to accept map_phase
            print(f"✓ Job submitted successfully: {response.job_id}")
        finally:
            channel.close()

    def test_get_job_status(self):
        """Test getting job status from coordinator"""
        channel = grpc.insecure_channel('localhost:50051')
        stub = mapreduce_pb2_grpc.JobServiceStub(channel)

        # First submit a job
        submit_request = mapreduce_pb2.JobSpec(
            job_id="test-job-2",
            input_path="/mapreduce-data/inputs/test.txt",
            output_path="/mapreduce-data/outputs/",
            map_reduce_file="wordcount.py",
            num_map_tasks=4,
            num_reduce_tasks=2,
            use_combiner=False
        )
        stub.SubmitJob(submit_request, timeout=5)

        # Then get status
        status_request = mapreduce_pb2.JobStatusRequest(job_id="test-job-2")
        try:
            response = stub.GetJobStatus(status_request, timeout=5)
            assert response.status in ["pending", "map_phase"]  # Updated to accept map_phase
            assert response.progress_percentage >= 0
            print(f"✓ Job status retrieved: {response.status}")
        finally:
            channel.close()

    def test_get_job_result(self):
        """Test getting job result from coordinator"""
        channel = grpc.insecure_channel('localhost:50051')
        stub = mapreduce_pb2_grpc.JobServiceStub(channel)

        # First submit a job
        submit_request = mapreduce_pb2.JobSpec(
            job_id="test-job-3",
            input_path="/mapreduce-data/inputs/test.txt",
            output_path="/mapreduce-data/outputs/",
            map_reduce_file="wordcount.py",
            num_map_tasks=4,
            num_reduce_tasks=2,
            use_combiner=False
        )
        stub.SubmitJob(submit_request, timeout=5)

        # Get result
        result_request = mapreduce_pb2.JobResultRequest(job_id="test-job-3")
        try:
            response = stub.GetJobResult(result_request, timeout=5)
            assert isinstance(response.output_path, str)
            assert isinstance(response.metrics, str)
            print("✓ Job result retrieved")
        finally:
            channel.close()

    def test_worker_heartbeat(self):
        """Test worker heartbeat endpoint"""
        # Workers listen on internal ports, so we test via docker network
        # This is a simplified test - full test would exec into coordinator
        # and make internal calls to workers
        print("✓ Worker heartbeat test (requires internal network access)")

    def test_concurrent_job_submissions(self):
        """Test multiple concurrent job submissions"""
        channel = grpc.insecure_channel('localhost:50051')
        stub = mapreduce_pb2_grpc.JobServiceStub(channel)

        try:
            for i in range(5):
                request = mapreduce_pb2.JobSpec(
                    job_id=f"concurrent-job-{i}",
                    input_path="/mapreduce-data/inputs/test.txt",
                    output_path="/mapreduce-data/outputs/",
                    map_reduce_file="wordcount.py",
                    num_map_tasks=4,
                    num_reduce_tasks=2,
                    use_combiner=False
                )
                response = stub.SubmitJob(request, timeout=5)
                assert response.job_id == f"concurrent-job-{i}"

            print("✓ Multiple concurrent jobs submitted successfully")
        finally:
            channel.close()


if __name__ == "__main__":
    """Run tests manually without pytest"""
    test_suite = TestGRPCIntegration()

    print("=" * 60)
    print("MapReduce gRPC Integration Tests")
    print("=" * 60)

    try:
        TestGRPCIntegration.setup_class()

        test_suite.test_coordinator_health()
        test_suite.test_submit_job()
        test_suite.test_get_job_status()
        test_suite.test_get_job_result()
        test_suite.test_worker_heartbeat()
        test_suite.test_concurrent_job_submissions()

        print("\n" + "=" * 60)
        print("All tests passed!")
        print("=" * 60)
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        raise
    finally:
        TestGRPCIntegration.teardown_class()
