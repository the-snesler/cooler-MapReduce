#!/usr/bin/env python3
"""
MapReduce Worker Server
Placeholder for development - will be implemented in later phases
"""

import os
import time

def main():
    """Start the worker process"""
    worker_id = os.environ.get('WORKER_ID', 'unknown')
    coordinator_host = os.environ.get('COORDINATOR_HOST', 'coordinator:50051')

    print(f"Worker {worker_id} starting...")
    print(f"Coordinator host: {coordinator_host}")
    # TODO: Add gRPC client and task execution in Phase 2.2 and 2.3

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print(f"Worker {worker_id} shutting down...")

if __name__ == '__main__':
    main()
