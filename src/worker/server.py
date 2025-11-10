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
        stub = worker_pb2_grpc.WorkerServiceStub(
            grpc.insecure_channel(self.coordinator_address)
        )
        
        while not self.stop_heartbeat.is_set():
            try:
                with self.tasks_lock:
                    status = "BUSY" if self.tasks else "IDLE"
                    available_slots = max(0, self.max_workers - len(self.tasks))
                
                request = worker_pb2.HeartbeatRequest(
                    worker_id=self.worker_id,
                    status=status,
                    available_slots=available_slots
                )
                
                response = stub.Heartbeat(request)
                if not response.acknowledged:
                    logger.warning("Heartbeat not acknowledged by coordinator")
                    
            except grpc.RpcError as e:
                logger.error(f"Failed to send heartbeat: {e}")
                
            time.sleep(5)  # Heartbeat interval
            
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
                    'shuffle_locations': [(loc.worker_address, loc.file_name) for loc in request.shuffle_locations]
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
        try:
            # Update task status to RUNNING
            with self.worker.tasks_lock:
                self.worker.tasks[request.task_id]['status'] = 'RUNNING'
            
            if request.task_type == "MAP":
                # Execute map task
                intermediate_files = self.worker.task_executor.execute_map(
                    request.job_id,
                    int(request.task_id),
                    os.path.basename(request.input_path)
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
                    int(request.task_id),
                    request.partition_id,
                    shuffle_locations # pass the collected locations: list of (worker_address, file_name
                )
                
                # Update task with success status and output
                with self.worker.tasks_lock:
                    self.worker.tasks[request.task_id].update({
                        'status': 'COMPLETED',
                        'output_file': output_file
                    })
                    
        except Exception as e:
            logger.error(f"Task execution failed: {e}")
            with self.worker.tasks_lock:
                self.worker.tasks[request.task_id].update({
                    'status': 'FAILED',
                    'error': str(e)
                })
        """Load map_fn and reduce_fn from job file."""
        try:
            # Load the module from file path
            spec = importlib.util.spec_from_file_location("job_module", self.job_file_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Verify required functions exist
            if self.task_type == "MAP" and not hasattr(module, "map_fn"):
                raise ValueError("Job file missing map_fn")
            if self.task_type == "REDUCE" and not hasattr(module, "reduce_fn"):
                raise ValueError("Job file missing reduce_fn")
            
            return module
        except Exception as e:
            raise RuntimeError(f"Failed to load job functions: {str(e)}")
    
    def execute_map_task(self):
        """Execute a map task."""
        module = self.load_job_functions()
        
        # Create partition files
        partitions = [[] for _ in range(self.num_reduce_tasks)]
        
        # Process input file
        with open(self.input_path, 'r') as f:
            total_size = os.path.getsize(self.input_path)
            processed_size = 0
            
            for line_num, line in enumerate(f):
                # Update progress
                processed_size += len(line)
                self.progress = (processed_size / total_size) * 100
                
                # Process line through map_fn
                key = line_num
                for out_key, out_value in module.map_fn(key, line.strip()):
                    # Partition output
                    partition = hash(str(out_key)) % self.num_reduce_tasks
                    partitions[partition].append((out_key, out_value))
                
                # Periodically write partitions to disk
                if line_num % 1000 == 0:
                    self._write_partitions(partitions)
                    partitions = [[] for _ in range(self.num_reduce_tasks)]
        
        # Write any remaining partitions
        self._write_partitions(partitions)
        self.progress = 100
    
    def execute_reduce_task(self):
        """Execute a reduce task."""
        module = self.load_job_functions()
        
        # Read all intermediate files for this partition
        all_kvs = {}
        intermediate_pattern = os.path.join(
            os.path.dirname(self.input_path),
            f"*_partition_{self.partition_id}.pkl"
        )
        
        intermediate_files = glob.glob(intermediate_pattern)
        total_files = len(intermediate_files)
        
        for i, filepath in enumerate(intermediate_files):
            with open(filepath, 'rb') as f:
                partition_data = pickle.load(f)
                for key, value in partition_data:
                    if key not in all_kvs:
                        all_kvs[key] = []
                    all_kvs[key].append(value)
            
            self.progress = ((i + 1) / total_files) * 100
        
        # Process each key through reduce_fn
        with open(self.output_path, 'w') as out_f:
            for i, (key, values) in enumerate(sorted(all_kvs.items())):
                for output in module.reduce_fn(key, values):
                    out_f.write(f"{output[0]}\t{output[1]}\n")
        
        self.progress = 100
    
    def _write_partitions(self, partitions):
        """Write partition data to intermediate files."""
        for i, partition in enumerate(partitions):
            if partition:  # Only write non-empty partitions
                path = os.path.join(
                    os.path.dirname(self.output_path),
                    f"{self.task_id}_partition_{i}.pkl"
                )
                with open(path, 'wb') as f:
                    pickle.dump(partition, f)

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
