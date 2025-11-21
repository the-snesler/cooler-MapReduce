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
        job = self.job_manager.jobs.get(job_id)
        if not job:
            return

        print(f"Executing job {job_id}")

        # Assign all map tasks
        print(f"Starting map phase for job {job_id}")
        self._assign_map_tasks(job_id)

        # Wait for map phase to complete
        while job.status == JobStatus.MAP_PHASE:
            time.sleep(0.5)

        print(f"Map phase completed for job {job_id}")

        # Generate reduce tasks
        if job.status == JobStatus.SHUFFLE_PHASE:
            print(f"Generating reduce tasks for job {job_id}")
            self.job_manager.generate_reduce_tasks(job)
            job.status = JobStatus.REDUCE_PHASE

            # Assign all reduce tasks
            print(f"Starting reduce phase for job {job_id}")
            self._assign_reduce_tasks(job_id)

        print(f"Job {job_id} completed")

    def _assign_map_tasks(self, job_id: str):
        """Assign all map tasks to workers"""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        job = self.job_manager.jobs.get(job_id)
        if not job:
            return

        def assign_task(task):
            worker_host = self._get_next_worker()
            channel = grpc.insecure_channel(worker_host)
            stub = mapreduce_pb2_grpc.TaskServiceStub(channel)

            request = mapreduce_pb2.MapTaskRequest(
                task_id=task.task_id,
                input_path=task.input_path,
                start_offset=task.start_offset,
                end_offset=task.end_offset,
                num_reduce_tasks=job.num_reduce_tasks,
                map_reduce_file=job.map_reduce_file,
                use_combiner=job.use_combiner,
                job_id=job_id
            )

            try:
                print(f"Assigning map task {task.task_id} to {worker_host}")
                response = stub.AssignMapTask(request, timeout=300)
                if response.success:
                    self.job_manager.mark_map_task_completed(job_id, task.task_id)
                    print(f"Map task {task.task_id} completed in {response.execution_time_ms}ms")
                else:
                    print(f"Map task {task.task_id} failed: {response.error_message}")
                return response
            except Exception as e:
                print(f"Error assigning map task {task.task_id}: {e}")
                return None
            finally:
                channel.close()

        with ThreadPoolExecutor(max_workers=len(self.worker_pool)) as executor:
            futures = [executor.submit(assign_task, task) for task in job.map_tasks]
            for future in as_completed(futures):
                future.result()

    def _assign_reduce_tasks(self, job_id: str):
        """Assign all reduce tasks to workers"""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        job = self.job_manager.jobs.get(job_id)
        if not job:
            return

        def assign_task(task):
            worker_host = self._get_next_worker()
            channel = grpc.insecure_channel(worker_host)
            stub = mapreduce_pb2_grpc.TaskServiceStub(channel)

            request = mapreduce_pb2.ReduceTaskRequest(
                task_id=task.task_id,
                partition_id=task.partition_id,
                intermediate_files=task.intermediate_files,
                map_reduce_file=job.map_reduce_file,
                output_path=job.output_path,
                job_id=job_id
            )

            try:
                print(f"Assigning reduce task {task.task_id} to {worker_host}")
                response = stub.AssignReduceTask(request, timeout=300)
                if response.success:
                    self.job_manager.mark_reduce_task_completed(job_id, task.task_id)
                    print(f"Reduce task {task.task_id} completed in {response.execution_time_ms}ms")
                else:
                    print(f"Reduce task {task.task_id} failed: {response.error_message}")
                return response
            except Exception as e:
                print(f"Error assigning reduce task {task.task_id}: {e}")
                return None
            finally:
                channel.close()

        with ThreadPoolExecutor(max_workers=len(self.worker_pool)) as executor:
            futures = [executor.submit(assign_task, task) for task in job.reduce_tasks]
            for future in as_completed(futures):
                future.result()

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
