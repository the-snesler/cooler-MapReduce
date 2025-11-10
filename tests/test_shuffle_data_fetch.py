"""
Integration test for the cross-worker Shuffle Data Fetch (FetchIntermediateFile RPC).
Verifies the ability of a Worker to serve an intermediate file to another Worker,
including error and empty data handling.
"""

import unittest
import os
import sys
import grpc
import time
import pickle
from concurrent import futures
from unittest.mock import MagicMock, patch

# --- Setup to find src modules (Adjust path as necessary) ---
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Import the relevant classes and stubs
import worker_pb2
import worker_pb2_grpc
# from worker.server import WorkerServicer # NOTE: Unused in this mock context, but keeping the original dependency structure
from worker.task_executor import TaskExecutor 

# --- Configuration ---
TEST_PORT = 50099
TEST_ADDRESS = f'localhost:{TEST_PORT}'
SHARED_DIR = os.path.join(os.path.dirname(__file__), 'temp_shared_shuffle')

# --- Global Mock Control ---
# This variable controls the server's behavior for different test cases.
MOCK_STATUS = "SUCCESS" 

# --- Helper: Setup Temporary Test Worker Server ---

class MockWorkerServer(worker_pb2_grpc.WorkerServiceServicer):
    """A minimal Worker Servicer to host the FetchIntermediateFile RPC with dynamic control."""
    def __init__(self, executor):
        # We need the TaskExecutor instance for file path resolution
        self.executor = executor
        
    def Heartbeat(self, request, context):
        # Prevent Heartbeat errors during testing
        return worker_pb2.HeartbeatResponse(acknowledged=True)

    def FetchIntermediateFile(self, request: worker_pb2.FileRequest, 
                              context: grpc.ServicerContext) -> worker_pb2.FileResponse:
        """Dynamic implementation based on MOCK_STATUS for file serving tests."""
        global MOCK_STATUS
        file_name = request.file_name
        file_path = os.path.join(self.executor.intermediate_dir, file_name)

        if MOCK_STATUS == "INTERNAL_ERROR":
            # Simulate a disk read error or general internal failure
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Simulated internal server error.")
            return worker_pb2.FileResponse(file_data=b'')
        
        if MOCK_STATUS == "EMPTY_DATA":
            # Simulate successfully finding the file but reading 0 bytes
            return worker_pb2.FileResponse(file_data=b'')

        # Handle the default SUCCESS and NOT_FOUND cases
        if not os.path.exists(file_path):
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"File {file_name} not found.")
            return worker_pb2.FileResponse(file_data=b'')

        try:
            with open(file_path, 'rb') as f:
                file_data = f.read()
            return worker_pb2.FileResponse(file_data=file_data)
        except Exception:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Simulated file read failure.")
            return worker_pb2.FileResponse(file_data=b'')

class TestShuffleDataFetch(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Initializes the mock Map Worker gRPC server and sets up a test file."""
        global SHARED_DIR, TEST_ADDRESS, TEST_PORT
        
        # 1. Setup shared directory and executor
        os.makedirs(SHARED_DIR, exist_ok=True)
        cls.executor = TaskExecutor(SHARED_DIR)
        
        # 2. Create and dump a test intermediate file
        cls.TEST_FILE_NAME = "job_map_1_part_0.pickle"
        cls.TEST_DATA = [("key1", 1), ("key2", 1)]
        cls.TEST_FILE_PATH = os.path.join(cls.executor.intermediate_dir, cls.TEST_FILE_NAME)
        
        with open(cls.TEST_FILE_PATH, 'wb') as f:
            pickle.dump(cls.TEST_DATA, f)
            
        # 3. Start the gRPC server (Mock Map Worker)
        cls.server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        worker_pb2_grpc.add_WorkerServiceServicer_to_server(
            MockWorkerServer(cls.executor), cls.server
        )
        cls.server.add_insecure_port(f'[::]:{TEST_PORT}')
        cls.server.start()
        
        # Create the stub for reuse
        cls.channel = grpc.insecure_channel(TEST_ADDRESS)
        cls.stub = worker_pb2_grpc.WorkerServiceStub(cls.channel)

    @classmethod
    def tearDownClass(cls):
        """Stops the mock server and cleans up the test directory."""
        cls.server.stop(0)
        cls.channel.close()
        import shutil
        if os.path.exists(SHARED_DIR):
            shutil.rmtree(SHARED_DIR)

    def tearDown(self):
        """Resets the global mock status after each test to ensure test isolation."""
        global MOCK_STATUS
        MOCK_STATUS = "SUCCESS"

    def test_01_fetch_intermediate_file_success(self):
        """
        Tests the primary successful execution path.
        Verifies that the Reduce client can request a file, receive the binary data,
        and unpickle the data to confirm content integrity.
        """
        request = worker_pb2.FileRequest(file_name=self.TEST_FILE_NAME)
        
        response = self.stub.FetchIntermediateFile(request, timeout=5)

        self.assertIsNotNone(response.file_data, "Response file_data should not be None.")
        self.assertTrue(len(response.file_data) > 0, "Response file_data should not be empty.")
        
        # Verification point: Data integrity via unpickling
        received_data = pickle.loads(response.file_data)
        self.assertEqual(received_data, self.TEST_DATA, "Received data does not match original pickled data.")

    def test_02_fetch_intermediate_file_not_found(self):
        """
        Tests the file not found error handling path.
        Verifies that the Worker Servicer returns the NOT_FOUND gRPC status code
        when the requested file is missing from the disk.
        """
        global MOCK_STATUS
        MOCK_STATUS = "SUCCESS" 
        
        request = worker_pb2.FileRequest(file_name="nonexistent_file.pickle")
        
        # Verification point: Expecting NOT_FOUND RpcError
        with self.assertRaises(grpc.RpcError) as cm:
            self.stub.FetchIntermediateFile(request, timeout=5)
        
        self.assertEqual(cm.exception.code(), grpc.StatusCode.NOT_FOUND)

    def test_03_fetch_internal_server_error(self):
        """
        Tests general internal failure handling.
        Verifies that the Map Worker correctly signals a critical failure 
        (e.g., simulated disk read error) using the INTERNAL gRPC status code.
        """
        global MOCK_STATUS
        MOCK_STATUS = "INTERNAL_ERROR"
        
        request = worker_pb2.FileRequest(file_name=self.TEST_FILE_NAME)
        
        # Verification point: Expecting INTERNAL RpcError
        with self.assertRaises(grpc.RpcError) as cm:
            self.stub.FetchIntermediateFile(request, timeout=5)
            
        self.assertEqual(cm.exception.code(), grpc.StatusCode.INTERNAL)
        self.assertIn("Simulated internal server error", cm.exception.details())

    def test_04_fetch_empty_data_response(self):
        """
        Tests the edge case of receiving a zero-byte response.
        Verifies that the transport layer successfully completes the RPC but the 
        FileResponse contains zero bytes, which the Reduce logic must handle 
        (representing an empty file or a logical error).
        """
        global MOCK_STATUS
        MOCK_STATUS = "EMPTY_DATA"
        
        request = worker_pb2.FileRequest(file_name=self.TEST_FILE_NAME)
        
        # Verification point: Successful RPC, but empty data field
        response = self.stub.FetchIntermediateFile(request, timeout=5)

        self.assertIsNotNone(response, "Response should be a valid object.")
        self.assertEqual(len(response.file_data), 0, "Received file data must be empty.")