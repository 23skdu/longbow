import time
import pytest
import pandas as pd
import numpy as np
from longbow import LongbowClient

class TestIndexingConsistency:
    def test_indexing_latency(self, test_dataset_name):
        """
        Test that inserts eventually become searchable (checking consistency/latency).
        """
        client = LongbowClient(uri="grpc://localhost:3000")
        
        # Setup data
        docs = [{"id": 1, "text": "Test document", "category": "latency_check"}]
        # Random vector
        vector = np.random.randn(384).tolist()
        df = pd.DataFrame(docs)
        df['vector'] = [vector]
        
        try:
            client.delete_namespace(test_dataset_name)
            time.sleep(1) # Wait for deletion
        except:
            pass

        print(f"Inserting into {test_dataset_name}...")
        client.insert(test_dataset_name, df)
        
        # Poll for results
        max_retries = 20
        retry_delay = 1.0 # seconds
        
        found = False
        start_time = time.time()
        
        for i in range(max_retries):
            print(f"Search attempt {i+1}...")
            try:
                # Search without filters first
                results = client.search(
                    test_dataset_name, 
                    vector=vector, 
                    k=5
                ).compute()
                
                if len(results) > 0:
                    print(f"Found {len(results)} items after {time.time() - start_time:.2f}s")
                    found = True
                    break
                else:
                    print(f"No results yet. Waiting {retry_delay}s...")
            except Exception as e:
                print(f"Search failed with error: {e}")
            
            time.sleep(retry_delay)
            
        assert found, f"Data not found after {max_retries * retry_delay} seconds"
        
        # Verify filtered search also works once data is found
        print("Testing filtered search...")
        results_filtered = client.search(
            test_dataset_name,
            vector=vector,
            k=5,
            filters=[{"field": "category", "op": "Eq", "value": "latency_check"}]
        ).compute()
        
        assert len(results_filtered) > 0, "Filtered search failed even after data was found"
        print("Filtered search success!")
