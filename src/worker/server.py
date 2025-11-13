"""
Worker server for MapReduce framework.
# Executes map and reduce tasks assigned by the coordinator.
Handles communication and task management.
"""

import os
import sys
import uuid
import time
import grpc
import glob
import pickle
import psutil
import logging
import threading
import importlib.util
from concurrent import futures
from typing import Dict, Optional

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import coordinator_pb2
import coordinator_pb2_grpc
import worker_pb2
import worker_pb2_grpc
from worker.task_executor import TaskExecutor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WorkerServer:
    """Worker server that handles task execution and coordination."""
    
    def __init__(self, shared_dir: str, coordinator_address: str, max_workers: int = 4):
        self.worker_id = str(uuid.uuid4())
        self.shared_dir = shared_dir
        self.coordinator_address = coordinator_address
        self.max_workers = max_workers
        self.task_executor = TaskExecutor(shared_dir)
        self.server_port = None  # Will be set when server starts
        
        # Track running tasks
        self.tasks: Dict[str, Dict] = {}
        self.tasks_lock = threading.Lock()
        
        # Initialize server and servicer
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
        self.servicer = WorkerServicer(self)
        worker_pb2_grpc.add_WorkerServiceServicer_to_server(self.servicer, self.server)
        
        # Start heartbeat thread
        self.stop_heartbeat = threading.Event()
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop)
        
    def start(self, port: int = 50051):
        """Start the worker server and heartbeat thread."""
        addr = f'[::]:{port}'
        self.server.add_insecure_port(addr)
        self.server.start()
        self.server_port = port
        logger.info(f"Worker server started on {addr}")
        
        self.heartbeat_thread.start()
        logger.info("Heartbeat thread started")
        
    def stop(self):
        """Stop the worker server and heartbeat thread."""
        self.stop_heartbeat.set()
        self.heartbeat_thread.join()
        
        self.server.stop(0)
        logger.info("Worker server stopped")
        
    def _heartbeat_loop(self):
        """Send periodic heartbeats to coordinator."""
        import coordinator_pb2
        import coordinator_pb2_grpc
        
        channel = grpc.insecure_channel(self.coordinator_address)
        stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
        
        try:
            while not self.stop_heartbeat.is_set():
                try:
                    with self.tasks_lock:
                        status = "BUSY" if self.tasks else "IDLE"
                        available_slots = max(0, self.max_workers - len(self.tasks))
                    
                    # Get CPU usage
                    import psutil
                    cpu_usage = psutil.cpu_percent(interval=0.1)
                    
                    request = coordinator_pb2.HeartbeatRequest(
                        worker_id=self.worker_id,
                        status=status,
                        available_slots=available_slots,
                        cpu_usage=cpu_usage
                    )
                    
                    response = stub.Heartbeat(request, timeout=5)
                    if not response.acknowledged:
                        logger.warning("Heartbeat not acknowledged by coordinator")
                        
                except grpc.RpcError as e:
                    logger.error(f"Failed to send heartbeat: {e}")
                    # Try to reconnect on error
                    try:
                        channel.close()
                    except:
                        pass
                    channel = grpc.insecure_channel(self.coordinator_address)
                    stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
                    
                time.sleep(5)  # Heartbeat interval
        finally:
            try:
                channel.close()
            except:
                pass
            
    def get_task_status(self, task_id: str) -> Optional[Dict]:
        """Get status of a specific task."""
        with self.tasks_lock:
            return self.tasks.get(task_id)
            
class WorkerServicer(worker_pb2_grpc.WorkerServiceServicer):
    """Implementation of worker gRPC service."""
    
    def __init__(self, worker_server: WorkerServer):
        self.worker = worker_server
        
    def AssignTask(self, request: worker_pb2.TaskAssignment, 
                  context: grpc.ServicerContext) -> worker_pb2.TaskAck:
        """Handle task assignment from coordinator."""
        try:
            # Register task
            with self.worker.tasks_lock:
                if len(self.worker.tasks) >= self.worker.max_workers:
                    return worker_pb2.TaskAck(
                        task_id=request.task_id,
                        accepted=False
                    )
                    
                self.worker.tasks[request.task_id] = {
                    'status': 'PENDING',
                    'type': request.task_type,
                    'job_id': request.job_id,
                    'input_path': request.input_path,
                    'output_path': request.output_path,

                        # store shuffle locations from the request
                    'shuffle_locations': [(loc.worker_address, loc.file_name) for loc in request.shuffle_locations] if request.shuffle_locations else []
                }
            
            # Start task execution in background
            threading.Thread(
                target=self._execute_task,
                args=(request,), # request object holds the data
                daemon=True
            ).start()
            
            return worker_pb2.TaskAck(
                task_id=request.task_id,
                accepted=True
            )
            
        except Exception as e:
            logger.error(f"Failed to assign task: {e}")
            return worker_pb2.TaskAck(
                task_id=request.task_id,
                accepted=False
            )
            
    def GetTaskStatus(self, request: worker_pb2.TaskStatusRequest,
                     context: grpc.ServicerContext) -> worker_pb2.TaskStatusResponse:
        """Report status of a task."""
        task = self.worker.get_task_status(request.task_id)
        if not task:
            return worker_pb2.TaskStatusResponse(
                task_id=request.task_id,
                status="UNKNOWN"
            )
            
        return worker_pb2.TaskStatusResponse(
            task_id=request.task_id,
            status=task['status']
        )
        
    def _execute_task(self, request: worker_pb2.TaskAssignment):
        """Execute assigned task using TaskExecutor."""
        intermediate_files = []
        output_file = None
        error_message = None
        
        try:
            # Update task status to RUNNING
            with self.worker.tasks_lock:
                self.worker.tasks[request.task_id]['status'] = 'RUNNING'
            
            if request.task_type == "MAP":
                # Parse task_id to get map task index (format: job_id_map_0)
                task_id_parts = request.task_id.split('_')
                map_index = int(task_id_parts[-1]) if task_id_parts else 0
                
                # Execute map task - need to get input split info from coordinator
                # For now, read the full input file (split handling can be improved)
                intermediate_files = self.worker.task_executor.execute_map(
                    request.job_id,
                    map_index,
                    request.input_path,  # Full path
                    request.job_file_path,
                    request.num_reduce_tasks
                )
                
                # Update task with success status and output
                with self.worker.tasks_lock:
                    self.worker.tasks[request.task_id].update({
                        'status': 'COMPLETED',
                        'output_files': intermediate_files
                    })
                    
            elif request.task_type == "REDUCE":
                # retrieve the necessary shuffle locations for the TaskExecutor
                # we must retrieve the locations stored when the task was accepted
                with self.worker.tasks_lock:
                    task_info = self.worker.tasks[request.task_id]
                    shuffle_locations = task_info.get('shuffle_locations', [])

                # Execute reduce task
                output_file = self.worker.task_executor.execute_reduce(
                    request.job_id,
                    int(request.partition_id),  # Use partition_id as task identifier
                    request.partition_id,
                    shuffle_locations,  # pass the collected locations:list of (worker_address, file_name)
                    request.job_file_path
                )
                
                # Update task with success status and output
                with self.worker.tasks_lock:
                    self.worker.tasks[request.task_id].update({
                        'status': 'COMPLETED',
                        'output_file': output_file
                    })
            
            # Report task completion to coordinator
            self._report_task_completion(
                request.task_id,
                request.job_id,
                request.task_type,
                intermediate_files if request.task_type == "MAP" else [],
                success=True
            )
                    
        except Exception as e:
            logger.error(f"Task execution failed: {e}")
            error_message = str(e)
            with self.worker.tasks_lock:
                self.worker.tasks[request.task_id].update({
                    'status': 'FAILED',
                    'error': error_message
                })
            
            # Report task failure to coordinator
            self._report_task_completion(
                request.task_id,
                request.job_id,
                request.task_type,
                [],
                success=False,
                error_message=error_message
            )
    
    def _report_task_completion(self, task_id: str, job_id: str, task_type: str, 
                                intermediate_files: list, success: bool, error_message: str = ""):
        """Report task completion to coordinator."""
        try:
            # Get worker address - use the worker's own address
            # In Docker, workers are accessible via their service name
            # For local testing, use localhost
            import socket
            hostname = socket.gethostname()
            port = self.worker.server_port if self.worker.server_port else 50052
            
            # Try to get the actual hostname/IP
            try:
                # In Docker, use the service name; otherwise use hostname
                worker_address = f"{hostname}:{port}"
            except:
                worker_address = f"localhost:{port}"
            
            # Import coordinator proto (will be regenerated)
            import coordinator_pb2
            import coordinator_pb2_grpc
            
            with grpc.insecure_channel(self.worker.coordinator_address) as channel:
                stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
                
                report = coordinator_pb2.TaskCompletionReport(
                    task_id=task_id,
                    job_id=job_id,
                    task_type=task_type,
                    worker_id=self.worker.worker_id,
                    worker_address=worker_address,
                    intermediate_files=intermediate_files,
                    success=success,
                    error_message=error_message
                )
                
                response = stub.ReportTaskCompletion(report, timeout=10)
                if response.acknowledged:
                    logger.info(f"Task {task_id} completion reported to coordinator")
                else:
                    logger.warning(f"Task {task_id} completion not acknowledged by coordinator")
                    
        except Exception as e:
            logger.error(f"Failed to report task completion: {e}")

    def FetchIntermediateFile(self, request: worker_pb2.FileRequest, 
                          context: grpc.ServicerContext) -> worker_pb2.FileResponse:
        """Serves an intermediate file to another worker for the shuffle phase."""
        file_name = request.file_name
        
        # Use the WorkerServer's TaskExecutor to determine the file path
        file_path = os.path.join(self.worker.task_executor.intermediate_dir, file_name)
        
        if not os.path.exists(file_path):
            logger.error(f"Intermediate file not found: {file_name}")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"File {file_name} not found on this worker.")
            return worker_pb2.FileResponse(file_data=b'')

        try:
            # Read the binary content of the pickled file
            with open(file_path, 'rb') as f:
                file_data = f.read()
            
            logger.info(f"Served file {file_name} ({len(file_data)} bytes)")
            return worker_pb2.FileResponse(file_data=file_data)
            
        except Exception as e:
            logger.error(f"Error serving file {file_name}: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error reading file: {str(e)}")
            return worker_pb2.FileResponse(file_data=b'')

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


# def serve(worker_id=None, coordinator_address='localhost:50051', port=50052):
#     """Start the worker gRPC server."""
#     if worker_id is None:
#         worker_id = f"worker-{uuid.uuid4().hex[:8]}"
    
#     worker_servicer = WorkerServicer(worker_id, coordinator_address)
    
#     server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
#     worker_pb2_grpc.add_WorkerServiceServicer_to_server(worker_servicer, server)
#     server.add_insecure_port(f'[::]:{port}')
#     server.start()
#     logger.info(f"Worker {worker_id} server started on port {port}")
    
#     # Start heartbeat sender
#     heartbeat_sender = HeartbeatSender(worker_id, worker_servicer, coordinator_address)
#     heartbeat_sender.start()
    
#     try:
#         server.wait_for_termination()
#     except KeyboardInterrupt:
#         logger.info(f"Worker {worker_id} server shutting down")
#         heartbeat_sender.stop()
#         server.stop(0)
def serve(worker_id=None, coordinator_address='localhost:50051', port=50052):
    """Start the worker gRPC server by initializing the WorkerServer application runner."""
    if worker_id is None:
        worker_id = f"worker-{uuid.uuid4().hex[:8]}"
    
    # 1. Define the mandatory shared directory path for the container
    SHARED_DIR = '/shared' 
    
    # 🚀 FIX: Instantiate the top-level WorkerServer class (which holds the config).
    worker_app = WorkerServer( # Renamed variable to worker_app for clarity
        shared_dir=SHARED_DIR, 
        coordinator_address=coordinator_address
    )
    
    # 2. Set the worker ID and start the application.
    # The WorkerServer handles starting its own WorkerServicer and Heartbeat thread internally.
    worker_app.worker_id = worker_id
    worker_app.start(port=port) 
    
    logger.info(f"Worker {worker_id} server started on port {port}")
    
    # 3. Wait for the gRPC server to terminate.
    try:
        # The WorkerServer holds the gRPC server instance and manages its termination.
        worker_app.server.wait_for_termination()
        
    except KeyboardInterrupt:
        logger.info(f"Worker {worker_id} server shutting down")
        worker_app.stop() # Graceful stop handles the heartbeat thread and server shutdown


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
