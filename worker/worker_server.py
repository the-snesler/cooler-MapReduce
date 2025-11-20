#!/usr/bin/env python3
"""
MapReduce Worker Server
Implements TaskService for coordinator-worker communication
"""

import os
import time
import grpc
from concurrent import futures
import mapreduce_pb2
import mapreduce_pb2_grpc


class TaskServiceImpl(mapreduce_pb2_grpc.TaskServiceServicer):
    """Implementation of TaskService for handling coordinator task assignments"""

    def __init__(self, worker_id):
        self.worker_id = worker_id

    def AssignMapTask(self, request, context):
        """Handle map task assignment from coordinator"""
        print(f"Worker {self.worker_id}: Received map task {request.task_id}")
        return mapreduce_pb2.TaskResult(
            success=True,
            error_message="",
            execution_time_ms=0
        )

    def AssignReduceTask(self, request, context):
        """Handle reduce task assignment from coordinator"""
        print(f"Worker {self.worker_id}: Received reduce task {request.task_id}")
        return mapreduce_pb2.TaskResult(
            success=True,
            error_message="",
            execution_time_ms=0
        )

    def Heartbeat(self, request, context):
        """Respond to heartbeat from coordinator"""
        return mapreduce_pb2.WorkerStatus(is_available=True)


def serve(port, worker_id):
    """Start the worker gRPC server"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    mapreduce_pb2_grpc.add_TaskServiceServicer_to_server(
        TaskServiceImpl(worker_id), server
    )
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"Worker {worker_id} gRPC server started on port {port}")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


def main():
    """Start the worker process"""
    worker_id = os.environ.get('WORKER_ID', 'unknown')
    coordinator_host = os.environ.get('COORDINATOR_HOST', 'coordinator:50051')
    port = int(os.environ.get('WORKER_PORT', 50052))

    print(f"Worker {worker_id} starting...")
    print(f"Coordinator host: {coordinator_host}")

    serve(port, worker_id)


if __name__ == '__main__':
    main()
