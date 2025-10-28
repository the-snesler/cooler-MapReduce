"""
Worker server for MapReduce framework.
Executes map and reduce tasks assigned by the coordinator.
"""

import grpc
from concurrent import futures
import sys
import os
import uuid
import time
import threading
import logging

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import coordinator_pb2
import coordinator_pb2_grpc
import worker_pb2
import worker_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WorkerServicer(worker_pb2_grpc.WorkerServiceServicer):
    """Implementation of WorkerService."""
    
    def __init__(self, worker_id, coordinator_address):
        self.worker_id = worker_id
        self.coordinator_address = coordinator_address
        self.status = "IDLE"
        self.available_slots = 1
        self.current_tasks = {}
        logger.info(f"Worker {worker_id} initialized")
    
    def Heartbeat(self, request, context):
        """Receive heartbeat from coordinator (currently not used in this direction)."""
        return worker_pb2.HeartbeatResponse(acknowledged=True)
    
    def AssignTask(self, request, context):
        """Receive task assignment from coordinator."""
        task_id = request.task_id
        task_type = request.task_type
        
        logger.info(f"Worker {self.worker_id} received task assignment: {task_id} ({task_type})")
        
        # Check if we can accept the task
        if self.available_slots <= 0:
            logger.warning(f"Worker {self.worker_id} has no available slots")
            return worker_pb2.TaskAck(task_id=task_id, accepted=False)
        
        # Accept the task
        self.current_tasks[task_id] = {
            'status': 'PENDING',
            'task_type': task_type,
            'job_id': request.job_id,
            'input_path': request.input_path,
            'output_path': request.output_path,
            'job_file_path': request.job_file_path,
            'partition_id': request.partition_id,
            'num_reduce_tasks': request.num_reduce_tasks
        }
        
        self.available_slots -= 1
        self.status = "BUSY" if self.available_slots == 0 else "IDLE"
        
        # TODO: Start task execution in background thread
        
        return worker_pb2.TaskAck(task_id=task_id, accepted=True)
    
    def GetTaskStatus(self, request, context):
        """Get status of a specific task."""
        task_id = request.task_id
        
        if task_id not in self.current_tasks:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Task {task_id} not found")
            return worker_pb2.TaskStatusResponse()
        
        task_info = self.current_tasks[task_id]
        
        return worker_pb2.TaskStatusResponse(
            task_id=task_id,
            status=task_info['status'],
            error_message=task_info.get('error_message', '')
        )


class HeartbeatSender:
    """Sends periodic heartbeats to the coordinator."""
    
    def __init__(self, worker_id, worker_servicer, coordinator_address, interval=5):
        self.worker_id = worker_id
        self.worker_servicer = worker_servicer
        self.coordinator_address = coordinator_address
        self.interval = interval
        self.running = False
        self.thread = None
    
    def start(self):
        """Start sending heartbeats."""
        self.running = True
        self.thread = threading.Thread(target=self._send_heartbeats, daemon=True)
        self.thread.start()
        logger.info(f"Heartbeat sender started for worker {self.worker_id}")
    
    def stop(self):
        """Stop sending heartbeats."""
        self.running = False
        if self.thread:
            self.thread.join()
    
    def _send_heartbeats(self):
        """Send heartbeat messages to coordinator."""
        while self.running:
            try:
                # TODO: Implement actual heartbeat sending when coordinator has heartbeat endpoint
                logger.debug(f"Worker {self.worker_id} heartbeat: status={self.worker_servicer.status}, slots={self.worker_servicer.available_slots}")
            except Exception as e:
                logger.error(f"Error sending heartbeat: {e}")
            
            time.sleep(self.interval)


def serve(worker_id=None, coordinator_address='localhost:50051', port=50052):
    """Start the worker gRPC server."""
    if worker_id is None:
        worker_id = f"worker-{uuid.uuid4().hex[:8]}"
    
    worker_servicer = WorkerServicer(worker_id, coordinator_address)
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    worker_pb2_grpc.add_WorkerServiceServicer_to_server(worker_servicer, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logger.info(f"Worker {worker_id} server started on port {port}")
    
    # Start heartbeat sender
    heartbeat_sender = HeartbeatSender(worker_id, worker_servicer, coordinator_address)
    heartbeat_sender.start()
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info(f"Worker {worker_id} server shutting down")
        heartbeat_sender.stop()
        server.stop(0)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='MapReduce Worker Server')
    parser.add_argument('--worker-id', type=str, help='Unique worker identifier')
    parser.add_argument('--coordinator', type=str, default='localhost:50051',
                        help='Coordinator address (default: localhost:50051)')
    parser.add_argument('--port', type=int, default=50052,
                        help='Worker server port (default: 50052)')
    
    args = parser.parse_args()
    serve(worker_id=args.worker_id, coordinator_address=args.coordinator, port=args.port)
