#!/usr/bin/env python3
"""
Detailed ingestion profiling script to identify slow ingestion bottlenecks
using the Longbow Python SDK.
"""
import time
import numpy as np
import pyarrow as pa
import json
import sys
from collections import defaultdict

try:
    from longbow import LongbowClient
except ImportError:
    print("Error: 'longbow' SDK not found. Install it via 'pip install ./pythonsdk'")
    sys.exit(1)

DATASET = "profile_test"
DIM = 384

def generate_batch(start_id, count, dim):
    ids = pa.array(np.arange(start_id, start_id + count), type=pa.int64())
    data = np.random.rand(count, dim).astype(np.float32)
    tensor_type = pa.list_(pa.float32(), dim)
    flat_data = data.flatten()
    vectors = pa.FixedSizeListArray.from_arrays(flat_data, type=tensor_type)
    ts = pa.array([int(time.time_ns())] * count, type=pa.timestamp("ns"))
    
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("vector", tensor_type),
        pa.field("timestamp", pa.timestamp("ns")),
    ])
    return pa.Table.from_arrays([ids, vectors, ts], schema=schema)

def profile_ingestion(client, batch_sizes, iterations=3):
    """Profile ingestion with different batch sizes using SDK insert"""
    results = defaultdict(list)
    
    for batch_size in batch_sizes:
        print(f"\n{'='*80}")
        print(f"Testing batch_size={batch_size}")
        print(f"{'='*80}")
        
        for iteration in range(iterations):
            print(f"\n  Iteration {iteration + 1}/{iterations}")
            
            # Generate batch
            gen_start = time.time()
            batch = generate_batch(0, batch_size, DIM)
            gen_time = time.time() - gen_start
            print(f"    Batch generation: {gen_time*1000:.2f}ms")
            
            dataset_name = f"{DATASET}_{batch_size}_{iteration}"
            
            # Measure SDK insert
            # SDK abstract stream/write/close, so we measure total SDK time.
            insert_start = time.time()
            client.insert(dataset_name, batch)
            insert_time = time.time() - insert_start
            print(f"    SDK Insert Time: {insert_time*1000:.2f}ms")
            
            total_time = time.time() - gen_start
            throughput = batch_size / total_time
            bandwidth_mbs = (throughput * DIM * 4) / (1024 * 1024)
            
            print(f"    Total time: {total_time*1000:.2f}ms")
            print(f"    Throughput: {throughput:.0f} vectors/s ({bandwidth_mbs:.1f} MB/s)")
            
            results[batch_size].append({
                'gen_time': gen_time,
                'insert_time': insert_time,
                'total_time': total_time,
                'throughput': throughput,
                'bandwidth_mbs': bandwidth_mbs
            })
            
            # Small delay between iterations
            time.sleep(0.5)
    
    return results

def print_summary(results):
    """Print summary statistics"""
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}\n")
    
    for batch_size in sorted(results.keys()):
        iterations = results[batch_size]
        avg_throughput = np.mean([r['throughput'] for r in iterations])
        avg_bandwidth = np.mean([r['bandwidth_mbs'] for r in iterations])
        avg_gen = np.mean([r['gen_time'] for r in iterations]) * 1000
        avg_insert = np.mean([r['insert_time'] for r in iterations]) * 1000
        avg_total = np.mean([r['total_time'] for r in iterations]) * 1000
        
        print(f"Batch Size: {batch_size}")
        print(f"  Avg Throughput: {avg_throughput:.0f} vectors/s ({avg_bandwidth:.1f} MB/s)")
        print(f"  Avg Timings:")
        print(f"    Generation:  {avg_gen:7.2f}ms ({avg_gen/avg_total*100:5.1f}%)")
        print(f"    SDK Insert:  {avg_insert:7.2f}ms ({avg_insert/avg_total*100:5.1f}%)")
        print(f"    Total:       {avg_total:7.2f}ms")
        print()

def check_index_status(client, dataset_name):
    """Check if dataset has been indexed"""
    try:
        # Use get_info from SDK
        info = client.get_info(dataset_name)
        # client.get_info return dict with total_records
        print(f"  Dataset: {dataset_name}")
        print(f"    Records: {info.get('total_records', 'N/A')}")
        print(f"    Total Bytes: {info.get('total_bytes', 'N/A')}")
    except Exception as e:
        print(f"  Status check failed: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Profile Ingestion (SDK)")
    parser.add_argument("--uri", default="grpc://localhost:3000", help="Longbow Data URI")
    parser.add_argument("--batch-sizes", type=str, default="100,500,1000,3000,5000", help="Comma-sep batch sizes")
    args = parser.parse_args()
    
    uri = args.uri
    print(f"Connecting to {uri}")
    # Infer meta uri
    meta_uri = uri.replace("3000", "3001")
    client = LongbowClient(uri=uri, meta_uri=meta_uri)
    
    batch_sizes = [int(x) for x in args.batch_sizes.split(",")]
    
    print("\nStarting ingestion profiling (SDK)...")
    results = profile_ingestion(client, batch_sizes, iterations=3)
    
    print_summary(results)
    
    # Check index status for a few datasets
    print(f"\n{'='*80}")
    print("INDEX STATUS")
    print(f"{'='*80}\n")
    for batch_size in [100, 1000, 5000]:
        check_index_status(client, f"{DATASET}_{batch_size}_0")
