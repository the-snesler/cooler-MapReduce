"""
Coordinator server for MapReduce framework.
Manages job submission, task distribution, and worker coordination.
"""

import grpc
from concurrent import futures
import sys
import os
import uuid
import time
from datetime import datetime
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


class JobState:
    """Represents the state of a MapReduce job."""
    def __init__(self, job_id, request):
        self.job_id = job_id
        self.input_path = request.input_path
        self.output_path = request.output_path
        self.job_file_path = request.job_file_path
        self.num_map_tasks = request.num_map_tasks
        self.num_reduce_tasks = request.num_reduce_tasks
        self.status = "SUBMITTED"
        self.submit_time = datetime.now().isoformat()
        self.completed_map_tasks = 0
        self.completed_reduce_tasks = 0
        self.error_message = ""


class CoordinatorServicer(coordinator_pb2_grpc.CoordinatorServiceServicer):
    """Implementation of CoordinatorService."""
    
    def __init__(self):
        self.jobs = {}  # job_id -> JobState
        self.workers = {}  # worker_id -> worker info
        logger.info("Coordinator service initialized")
    
    def SubmitJob(self, request, context):
        """Handle job submission from client."""
        job_id = str(uuid.uuid4())
        logger.info(f"Received job submission request: {job_id}")
        logger.info(f"  Input: {request.input_path}")
        logger.info(f"  Output: {request.output_path}")
        logger.info(f"  Job file: {request.job_file_path}")
        logger.info(f"  Map tasks: {request.num_map_tasks}")
        logger.info(f"  Reduce tasks: {request.num_reduce_tasks}")
        
        # Create job state
        job_state = JobState(job_id, request)
        self.jobs[job_id] = job_state
        
        # TODO: Create and schedule tasks
        
        return coordinator_pb2.JobResponse(
            job_id=job_id,
            status="SUBMITTED"
        )
    
    def GetJobStatus(self, request, context):
        """Get the status of a submitted job."""
        job_id = request.job_id
        
        if job_id not in self.jobs:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Job {job_id} not found")
            return coordinator_pb2.JobStatusResponse()
        
        job_state = self.jobs[job_id]
        
        return coordinator_pb2.JobStatusResponse(
            job_id=job_id,
            status=job_state.status,
            total_map_tasks=job_state.num_map_tasks,
            completed_map_tasks=job_state.completed_map_tasks,
            total_reduce_tasks=job_state.num_reduce_tasks,
            completed_reduce_tasks=job_state.completed_reduce_tasks,
            error_message=job_state.error_message
        )
    
    def ListJobs(self, request, context):
        """List all jobs."""
        jobs = []
        for job_id, job_state in self.jobs.items():
            jobs.append(coordinator_pb2.JobInfo(
                job_id=job_id,
                status=job_state.status,
                submit_time=job_state.submit_time
            ))
        
        return coordinator_pb2.JobListResponse(jobs=jobs)
    
    def GetJobResults(self, request, context):
        """Get results of a completed job."""
        job_id = request.job_id
        
        if job_id not in self.jobs:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Job {job_id} not found")
            return coordinator_pb2.JobResultsResponse()
        
        job_state = self.jobs[job_id]
        
        # TODO: Collect output file paths
        output_files = []
        
        return coordinator_pb2.JobResultsResponse(
            job_id=job_id,
            status=job_state.status,
            output_files=output_files
        )


def serve(port=50051):
    """Start the coordinator gRPC server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    coordinator_pb2_grpc.add_CoordinatorServiceServicer_to_server(
        CoordinatorServicer(), server
    )
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logger.info(f"Coordinator server started on port {port}")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Coordinator server shutting down")
        server.stop(0)


if __name__ == '__main__':
    serve()
