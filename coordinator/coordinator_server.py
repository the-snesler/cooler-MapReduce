#!/usr/bin/env python3
"""
MapReduce Coordinator Server
Placeholder for development - will be implemented in later phases
"""

import grpc
from concurrent import futures
import time

def serve():
    """Start the coordinator gRPC server"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # TODO: Add service implementation in Phase 1.3
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Coordinator server started on port 50051")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
