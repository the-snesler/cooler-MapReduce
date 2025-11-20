"""
gRPC Client Utilities
Provides helper functions for creating gRPC stubs to communicate with coordinator and workers
"""

import grpc
import mapreduce_pb2
import mapreduce_pb2_grpc


def get_job_service_stub(coordinator_host, timeout=10):
    """
    Create a JobService stub for client-coordinator communication

    Args:
        coordinator_host: Host address in format 'host:port' (e.g., 'coordinator:50051')
        timeout: Connection timeout in seconds (default: 10)

    Returns:
        JobServiceStub: gRPC stub for JobService

    Raises:
        grpc.RpcError: If connection fails
    """
    try:
        channel = grpc.insecure_channel(
            coordinator_host,
            options=[
                ('grpc.max_send_message_length', 100 * 1024 * 1024),
                ('grpc.max_receive_message_length', 100 * 1024 * 1024),
            ]
        )
        # Wait for channel to be ready
        grpc.channel_ready_future(channel).result(timeout=timeout)
        return mapreduce_pb2_grpc.JobServiceStub(channel)
    except grpc.FutureTimeoutError:
        raise ConnectionError(f"Failed to connect to coordinator at {coordinator_host} within {timeout}s")
    except Exception as e:
        raise ConnectionError(f"Error connecting to coordinator at {coordinator_host}: {e}")


def get_task_service_stub(worker_host, timeout=10):
    """
    Create a TaskService stub for coordinator-worker communication

    Args:
        worker_host: Host address in format 'host:port' (e.g., 'worker-1:50052')
        timeout: Connection timeout in seconds (default: 10)

    Returns:
        TaskServiceStub: gRPC stub for TaskService

    Raises:
        grpc.RpcError: If connection fails
    """
    try:
        channel = grpc.insecure_channel(
            worker_host,
            options=[
                ('grpc.max_send_message_length', 100 * 1024 * 1024),
                ('grpc.max_receive_message_length', 100 * 1024 * 1024),
            ]
        )
        # Wait for channel to be ready
        grpc.channel_ready_future(channel).result(timeout=timeout)
        return mapreduce_pb2_grpc.TaskServiceStub(channel)
    except grpc.FutureTimeoutError:
        raise ConnectionError(f"Failed to connect to worker at {worker_host} within {timeout}s")
    except Exception as e:
        raise ConnectionError(f"Error connecting to worker at {worker_host}: {e}")
