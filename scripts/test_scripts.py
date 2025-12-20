import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# MOCK DEPENDENCIES BEFORE IMPORTING SCRIPTS
sys.modules['numpy'] = MagicMock()
sys.modules['pyarrow'] = MagicMock()
sys.modules['pyarrow.flight'] = MagicMock()
sys.modules['pandas'] = MagicMock()
sys.modules['polars'] = MagicMock()

# Define a mock Action class that behaves like the real one
class MockAction:
    def __init__(self, action_type, body):
        self.type = action_type
        self.body = body

sys.modules['pyarrow.flight'].Action = MockAction
sys.modules['pyarrow'].flight = sys.modules['pyarrow.flight'] # Ensure consistency

# Setup mocks for specific attributes used at import time
sys.modules['numpy'].random.rand.return_value.astype.return_value = MagicMock()

# Now import the scripts
# Use importlib to ensure we get fresh imports if needed, though mostly standard import works if modules mock is set
import importlib.util

def import_script(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module

# Path to scripts
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
perf_test = import_script('perf_test', os.path.join(SCRIPT_DIR, 'perf_test.py'))
ops_test = import_script('ops_test', os.path.join(SCRIPT_DIR, 'ops_test.py'))

import json

class TestPerfTest(unittest.TestCase):
    def test_benchmark_vector_search(self):
        # Setup mock client
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.body.to_pybytes.return_value = json.dumps({"ids": [1, 2, 3]}).encode('utf-8')
        mock_client.do_action.return_value = [mock_result]
        
        # Mock numpy array inputs
        # perf_test expects query_vectors to be a numpy array like object
        query_vectors = [MagicMock()] 
        query_vectors[0].tolist.return_value = [0.1, 0.2]
        
        k = 10
        name = "test_dataset"
        
        # Run function
        result = perf_test.benchmark_vector_search(mock_client, name, query_vectors, k)
        
        # Verify
        self.assertEqual(result.name, "VectorSearch")
        self.assertEqual(result.rows, 3) 
        
        # Verify call
        args, _ = mock_client.do_action.call_args
        action = args[0]
        self.assertEqual(action.type, "VectorSearch")
        payload = json.loads(action.body.to_pybytes())
        self.assertEqual(payload['dataset'], name)
        self.assertEqual(payload['k'], k)

class TestOpsTest(unittest.TestCase):
    def test_command_search(self):
        # Setup mock client
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.body.to_pybytes.return_value = json.dumps({"ids": [1]}).encode('utf-8')
        mock_client.do_action.return_value = iter([mock_result])
        
        # Setup Args
        args = MagicMock()
        args.dataset = "test_ops"
        args.k = 5
        args.dim = 128
        args.text_query = "hello"
        args.alpha = 0.5
        
        # Run - note: ops_test imports numpy globally so we need to ensure our mock handles the np.random.rand call inside command_search
        # We mocked sys.modules['numpy'] so op_test.np is that mock
        ops_test.np.random.rand.return_value.astype.return_value.tolist.return_value = [0.1] * 128
        
        ops_test.command_search(args, None, mock_client)
        
        # Verify
        args_call, _ = mock_client.do_action.call_args
        action = args_call[0]
        self.assertEqual(action.type, "VectorSearch")
        payload = json.loads(action.body.to_pybytes())
        self.assertEqual(payload['dataset'], "test_ops")
        self.assertEqual(payload['text_query'], "hello")

    def test_command_list(self):
        # Mock dependencies in ops_test
        mock_client = MagicMock()
        mock_info = MagicMock()
        mock_info.descriptor.path = [b"test_ds"]
        mock_info.total_records = 100
        mock_info.total_bytes = 1024
        mock_client.list_flights.return_value = [mock_info]
        
        ops_test.command_list(None, None, mock_client)
        
        mock_client.list_flights.assert_called_once()

if __name__ == '__main__':
    unittest.main()
