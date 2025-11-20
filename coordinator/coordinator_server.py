#!/usr/bin/env python3
"""
MapReduce Coordinator Server
Implements JobService for client-coordinator communication
"""

import grpc
from concurrent import futures
import time
import mapreduce_pb2
import mapreduce_pb2_grpc


class JobServiceImpl(mapreduce_pb2_grpc.JobServiceServicer):
    """Implementation of JobService for handling client requests"""

    def __init__(self):
        self.jobs = {}

    def SubmitJob(self, request, context):
        """Accept job submission and store job spec"""
        self.jobs[request.job_id] = request
        print(f"Job submitted: {request.job_id}")
        return mapreduce_pb2.JobResponse(
            job_id=request.job_id,
            status="pending"
        )

    def GetJobStatus(self, request, context):
        """Return job status and progress"""
        if request.job_id in self.jobs:
            return mapreduce_pb2.JobStatusResponse(
                status="pending",
                progress_percentage=0
            )
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Job {request.job_id} not found")
            return mapreduce_pb2.JobStatusResponse()

    def GetJobResult(self, request, context):
        """Return job result"""
        if request.job_id in self.jobs:
            return mapreduce_pb2.JobResultResponse(
                output_path="",
                metrics=""
            )
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Job {request.job_id} not found")
            return mapreduce_pb2.JobResultResponse()


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
