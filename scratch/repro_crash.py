import os
import sys
import time
import numpy as np
from longbow import LongbowClient

# Constants
DIM = 128
COUNT = 25000
DTYPE = "float32"
BATCH_SIZE = 1000

def run_test():
    client = LongbowClient(uri="grpc://127.0.0.1:3000")
    dataset_name = f"crash_test_{int(time.time())}"
    
    print(f"Creating dataset {dataset_name} with {COUNT} vectors...")
    
    for i in range(0, COUNT, BATCH_SIZE):
        end = min(i + BATCH_SIZE, COUNT)
        batch_count = end - i
        vectors_batch = np.random.randn(batch_count, DIM).astype(np.float32)
        batch_ids = [str(j) for j in range(i, end)]
        
        print(f"  Inserting batch {i//BATCH_SIZE + 1}...")
        client.insert(
            dataset_name,
            [{"id": id, "vector": vec.tolist()} for id, vec in zip(batch_ids, vectors_batch)],
        )
    
    print("Insertion complete. Running search...")
    for alpha in [0.0, 0.5, 1.0]:
        print(f"  GraphRAG alpha={alpha}...")
        query_vec = np.random.randn(DIM).astype(np.float32).tolist()
        res = client.search(
            dataset_name,
            query_vec,
            k=5,
            # GraphRAG specific params if needed, but search usually works
        )
        print(f"    Results: {len(res)}")

if __name__ == "__main__":
    run_test()
