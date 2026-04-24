import time
import numpy as np
import pandas as pd
from longbow import LongbowClient
import sys

def run_sdk_benchmark(mode="dense", count=1000, dim=128, queries=1000):
    client = LongbowClient(uri="grpc://127.0.0.1:3000")
    dataset_name = f"sdk_bench_{mode}_{dim}d"
    
    print(f"--- SDK Benchmark: {mode.upper()} ---")
    print(f"Dataset: {dataset_name}, Dim: {dim}, Count: {count}")
    
    # Ingest
    vectors = np.random.rand(count, dim).astype(np.float32)
    ids = [str(i) for i in range(count)]
    
    print(f"Ingesting {count} vectors...")
    start_ingest = time.time()
    client.insert(dataset_name, [{"id": id, "vector": vec.tolist()} for id, vec in zip(ids, vectors)])
    ingest_duration = time.time() - start_ingest
    print(f"Ingest completed in {ingest_duration:.2f}s ({count/ingest_duration:.1f} vec/s)")
    
    time.sleep(2) # Wait for indexing
    
    # Search
    print(f"Running {queries} queries...")
    latencies = []
    
    for _ in range(queries):
        query_vec = np.random.rand(dim).astype(np.float32).tolist()
        start = time.time()
        try:
            if mode == "dense":
                client.search(dataset_name, vector=query_vec, k=10)
            elif mode == "geo":
                # Assuming existing geo_point in data (this is a simplified test)
                client.search(dataset_name, vector=query_vec, k=10, geo_point={"lat": 40.7, "lon": -74.0}, radius_km=10.0)
            elif mode == "recommend":
                client.recommend(dataset_name, seed_ids=["0", "1", "2"], k=10)
            elif mode == "graphrag":
                client.search(dataset_name, vector=query_vec, k=10, graph_alpha=0.5)
            
            latencies.append((time.time() - start) * 1000)
        except Exception as e:
            continue
            
    if latencies:
        latencies.sort()
        avg = sum(latencies) / len(latencies)
        qps = 1000.0 / avg
        print(f"QPS: {qps:.1f}")
        print(f"P50: {latencies[int(0.5 * len(latencies))]:.2f}ms")
        print(f"P95: {latencies[int(0.95 * len(latencies))]:.2f}ms")
    else:
        print("Search failed.")

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "dense"
    run_sdk_benchmark(mode=mode)
