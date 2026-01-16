#!/usr/bin/env python3
import time
import argparse
import sys
import numpy as np
import pyarrow as pa
import pyarrow.flight as flight
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_client(uri):
    return flight.FlightClient(uri)

def check_readiness(client, dataset):
    """Polls the check_readiness action. Returns True if READY."""
    action = flight.Action("check_readiness", json.dumps({"dataset": dataset}).encode('utf-8'))
    for result in client.do_action(action):
        res = json.loads(result.body.to_pybytes())
        if res["status"] == "READY":
             return True, res
        return False, res
    return False, {}

def run_benchmark(uri, sizes, dim):
    client = get_client(uri)
    
    print(f"{'Size':<10} | {'Ingest(s)':<10} | {'Index(s)':<10} | {'Total(s)':<10} | {'Rate(v/s)':<12}")
    print("-" * 65)

    for size in sizes:
        dataset = f"bench_idx_{size}"
        
        # Generata Data
        # logging.info(f"Generating {size} vectors...")
        data = np.random.rand(size, dim).astype(np.float32)
        
        # Create Batch
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([str(i) for i in range(size)]),
                pa.array([row for row in data], type=pa.list_(pa.float32(), dim)),
            ],
            names=["id", "vector"]
        )

        # DoPut
        start_ingest = time.time()
        writer, _ = client.do_put(flight.FlightDescriptor.for_path(dataset), batch.schema)
        writer.write_batch(batch)
        writer.close()
        end_ingest = time.time()
        
        ingest_time = end_ingest - start_ingest
        
        # Wait for Indexing
        # Poll every 100ms
        while True:
            ready, status = check_readiness(client, dataset)
            if ready:
                break
            time.sleep(0.1)
            
        end_index = time.time()
        index_time = end_index - end_ingest
        total_time = end_index - start_ingest
        rate = size / total_time
        
        print(f"{size:<10} | {ingest_time:<10.4f} | {index_time:<10.4f} | {total_time:<10.4f} | {rate:<12.2f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Measure Indexing Time")
    parser.add_argument("--uri", default="grpc://localhost:3000", help="Flight URI")
    args = parser.parse_args()
    
    SIZES = [3000, 5000, 10000, 20000, 50000]
    run_benchmark(args.uri, SIZES, 384)
