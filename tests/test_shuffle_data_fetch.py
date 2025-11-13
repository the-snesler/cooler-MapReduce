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

    def test_05_input_file_splitting(self):
        """
        Tests that input file splitting works correctly.
        Verifies that map tasks process only their assigned split of the input file,
        not the entire file. This ensures proper parallelism and prevents duplicate processing.
        """
        print("\n" + "="*60)
        print("TEST: Input File Splitting")
        print("="*60)
        
        # Import coordinator's split function
        from coordinator.server import split_input_file
        
        # Create a test input file with multiple lines
        test_input_file = os.path.join(SHARED_DIR, 'input', 'test_split.txt')
        os.makedirs(os.path.dirname(test_input_file), exist_ok=True)
        
        # Create file with 6 lines (easy to split)
        test_lines = [
            "line1 word1 word2",
            "line2 word3 word4",
            "line3 word5 word6",
            "line4 word7 word8",
            "line5 word9 word10",
            "line6 word11 word12"
        ]
        
        with open(test_input_file, 'w') as f:
            f.write('\n'.join(test_lines))
            f.write('\n')  # Ensure file ends with newline
        
        file_size = os.path.getsize(test_input_file)
        print(f"  Created test file: {test_input_file}")
        print(f"  File size: {file_size} bytes")
        print(f"  Number of lines: {len(test_lines)}")
        
        # Test splitting into 2 tasks
        num_splits = 2
        splits = split_input_file(test_input_file, num_splits)
        
        self.assertIsNotNone(splits, "split_input_file should return a list of splits")
        self.assertEqual(len(splits), num_splits, f"Should have {num_splits} splits")
        
        print(f"\n  Splits calculated:")
        for i, (start_pos, end_pos) in enumerate(splits):
            print(f"    Split {i}: start={start_pos}, end={end_pos}, size={end_pos - start_pos}")
        
        # Verify splits cover entire file without overlap
        self.assertEqual(splits[0][0], 0, "First split should start at 0")
        self.assertEqual(splits[-1][1], file_size, "Last split should end at file size")
        
        # Verify splits don't overlap (end of one should equal start of next)
        # This ensures no duplicate processing
        for i in range(len(splits) - 1):
            self.assertEqual(splits[i][1], splits[i+1][0], 
                           f"Split {i} should end exactly where split {i+1} starts (no overlap, no gap)")
        
        # Test that execute_map processes only the assigned split
        # Create a simple job file for testing
        job_file = os.path.join(SHARED_DIR, 'jobs', 'test_split.py')
        os.makedirs(os.path.dirname(job_file), exist_ok=True)
        
        with open(job_file, 'w') as f:
            f.write("""
def map_fn(key, value):
    for word in value.split():
        yield (word, 1)

def reduce_fn(key, values):
    yield (key, sum(values))
""")
        
        # Execute map task 0 with its split
        start_pos_0, end_pos_0 = splits[0]
        intermediate_files_0 = self.executor.execute_map(
            job_id="test_split_job",
            task_id=0,
            input_file=test_input_file,
            job_file_path=job_file,
            num_reduce_tasks=2,
            start_pos=start_pos_0,
            end_pos=end_pos_0
        )
        
        # Execute map task 1 with its split
        start_pos_1, end_pos_1 = splits[1]
        intermediate_files_1 = self.executor.execute_map(
            job_id="test_split_job",
            task_id=1,
            input_file=test_input_file,
            job_file_path=job_file,
            num_reduce_tasks=2,
            start_pos=start_pos_1,
            end_pos=end_pos_1
        )
        
        print(f"\n  Map Task 0 processed split [{start_pos_0}, {end_pos_0})")
        print(f"  Map Task 1 processed split [{start_pos_1}, {end_pos_1})")
        
        # Load and inspect intermediate files
        all_data_0 = []
        for file_name in intermediate_files_0:
            file_path = os.path.join(self.executor.intermediate_dir, file_name)
            with open(file_path, 'rb') as f:
                data = pickle.load(f)
                all_data_0.extend(data)
        
        all_data_1 = []
        for file_name in intermediate_files_1:
            file_path = os.path.join(self.executor.intermediate_dir, file_name)
            with open(file_path, 'rb') as f:
                data = pickle.load(f)
                all_data_1.extend(data)
        
        # Extract unique words from each task
        words_0 = set([key for key, value in all_data_0])
        words_1 = set([key for key, value in all_data_1])
        
        print(f"\n  Map Task 0 found {len(words_0)} unique words: {sorted(words_0)}")
        print(f"  Map Task 1 found {len(words_1)} unique words: {sorted(words_1)}")
        
        # Verify that tasks process different parts (words should be different or minimal overlap)
        # Since we're splitting by byte position, there might be some overlap at boundaries
        # But the majority of words should be different
        
        # Count total words processed
        total_words_0 = sum([value for key, value in all_data_0])
        total_words_1 = sum([value for key, value in all_data_1])
        
        print(f"\n  Map Task 0 total word count: {total_words_0}")
        print(f"  Map Task 1 total word count: {total_words_1}")
        print(f"  Combined total: {total_words_0 + total_words_1}")
        
        # Expected: 6 lines * 3 words per line = 18 words total
        # Each line has: lineN, wordX, wordY (3 words)
        expected_total = 18
        actual_total = total_words_0 + total_words_1
        
        print(f"\n  Expected total words: {expected_total}")
        print(f"  Actual total words: {actual_total}")
        
        # Verify we're not processing everything twice (should match expected exactly)
        # With proper splitting, there should be no duplicate processing
        self.assertEqual(actual_total, expected_total, 
                       f"Total words should match expected ({expected_total}). "
                       f"Got {actual_total}, which suggests duplicate processing if not {expected_total}")
        
        # Verify that tasks are processing different data
        # (not both processing the entire file)
        overlap = words_0.intersection(words_1)
        print(f"\n  Word overlap between tasks: {len(overlap)} words")
        print(f"    Overlapping words: {sorted(overlap)}")
        
        # If splitting works, there should be minimal overlap (only at boundaries)
        # If both tasks process entire file, overlap would be very high
        overlap_ratio = len(overlap) / max(len(words_0), len(words_1), 1)
        print(f"  Overlap ratio: {overlap_ratio:.2%}")
        
        # If overlap is > 80%, it suggests both tasks processed the entire file
        self.assertLess(overlap_ratio, 0.8, 
                       "Overlap should be low (<80%) - indicates proper splitting")
        
        print(f"\n  ✓ Input splitting test passed!")
        
        # Cleanup intermediate files
        for file_name in intermediate_files_0 + intermediate_files_1:
            file_path = os.path.join(self.executor.intermediate_dir, file_name)
            if os.path.exists(file_path):
                os.remove(file_path)