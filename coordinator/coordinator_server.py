#!/usr/bin/env python3
"""
MapReduce Coordinator Server
Implements JobService for client-coordinator communication
"""

import grpc
from concurrent import futures
import time
import threading
import mapreduce_pb2
import mapreduce_pb2_grpc
from job_manager import JobManager, JobStatus


class JobServiceImpl(mapreduce_pb2_grpc.JobServiceServicer):
    """Implementation of JobService for handling client requests"""

    def __init__(self):
        self.job_manager = JobManager()
        self.worker_pool = ['worker-1:50052', 'worker-2:50052', 'worker-3:50052', 'worker-4:50052']
        self.worker_index = 0

    def SubmitJob(self, request, context):
        """Handle job submission"""
        try:
            # Create job
            job = self.job_manager.create_job(request)

            # Generate map tasks
            self.job_manager.generate_map_tasks(job)
            job.status = JobStatus.MAP_PHASE

            # Start task assignment in background thread
            threading.Thread(target=self._execute_job, args=(job.job_id,), daemon=True).start()

            print(f"Job submitted: {job.job_id} with {len(job.map_tasks)} map tasks")
            return mapreduce_pb2.JobResponse(
                job_id=job.job_id,
                status=job.status.value
            )
        except Exception as e:
            print(f"Error submitting job: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return mapreduce_pb2.JobResponse()

    def GetJobStatus(self, request, context):
        """Get job status"""
        status = self.job_manager.get_job_status(request.job_id)
        if not status:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Job {request.job_id} not found")
            return mapreduce_pb2.JobStatusResponse()

        return mapreduce_pb2.JobStatusResponse(
            status=status['status'],
            progress_percentage=status['progress']
        )

    def GetJobResult(self, request, context):
        """Get job results"""
        job = self.job_manager.jobs.get(request.job_id)
        if not job:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return mapreduce_pb2.JobResultResponse()

        if job.status != JobStatus.COMPLETED:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details("Job not completed yet")
            return mapreduce_pb2.JobResultResponse()

        metrics = f"Execution time: {job.end_time - job.start_time:.2f}s"
        return mapreduce_pb2.JobResultResponse(
            output_path=job.output_path,
            metrics=metrics
        )

    def _execute_job(self, job_id: str):
        """Execute job by assigning tasks to workers"""
        # Assign map tasks (implemented in Task 6)
        pass

    def _get_next_worker(self) -> str:
        """Round-robin worker selection"""
        worker = self.worker_pool[self.worker_index]
        self.worker_index = (self.worker_index + 1) % len(self.worker_pool)
        return worker


def serve():
    """Start the coordinator gRPC server"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_JobServiceServicer_to_server(JobServiceImpl(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Coordinator gRPC server started on port 50051")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    serve()
