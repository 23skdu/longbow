#!/usr/bin/env python3
"""
Detailed ingestion profiling script to identify slow ingestion bottlenecks.
Measures timing at each stage of the ingestion pipeline.
"""
import time
import numpy as np
import pyarrow as pa
import pyarrow.flight as flight
import json
import sys
from collections import defaultdict

DATASET = "profile_test"
DIM = 384

def get_client(uri):
    return flight.FlightClient(uri)

def generate_batch(start_id, count, dim):
    ids = pa.array(np.arange(start_id, start_id + count), type=pa.int64())
    data = np.random.rand(count, dim).astype(np.float32)
    tensor_type = pa.list_(pa.float32(), dim)
    flat_data = data.flatten()
    vectors = pa.FixedSizeListArray.from_arrays(flat_data, type=tensor_type)
    ts = pa.array([time.time_ns()] * count, type=pa.timestamp("ns"))
    
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("vector", tensor_type),
        pa.field("timestamp", pa.timestamp("ns")),
    ])
    return pa.Table.from_arrays([ids, vectors, ts], schema=schema)

def profile_ingestion(client, batch_sizes, iterations=3):
    """Profile ingestion with different batch sizes"""
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
            
            # Create descriptor and writer
            descriptor = flight.FlightDescriptor.for_path(f"{DATASET}_{batch_size}_{iteration}")
            
            # Measure DoPut stream creation
            stream_start = time.time()
            writer, _ = client.do_put(descriptor, batch.schema)
            stream_time = time.time() - stream_start
            print(f"    Stream creation: {stream_time*1000:.2f}ms")
            
            # Measure write_table
            write_start = time.time()
            writer.write_table(batch)
            write_time = time.time() - write_start
            print(f"    Write table: {write_time*1000:.2f}ms")
            
            # Measure close
            close_start = time.time()
            writer.close()
            close_time = time.time() - close_start
            print(f"    Close stream: {close_time*1000:.2f}ms")
            
            total_time = time.time() - gen_start
            throughput = batch_size / total_time
            bandwidth_mbs = (throughput * DIM * 4) / (1024 * 1024)
            
            print(f"    Total time: {total_time*1000:.2f}ms")
            print(f"    Throughput: {throughput:.0f} vectors/s ({bandwidth_mbs:.1f} MB/s)")
            
            results[batch_size].append({
                'gen_time': gen_time,
                'stream_time': stream_time,
                'write_time': write_time,
                'close_time': close_time,
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
        avg_stream = np.mean([r['stream_time'] for r in iterations]) * 1000
        avg_write = np.mean([r['write_time'] for r in iterations]) * 1000
        avg_close = np.mean([r['close_time'] for r in iterations]) * 1000
        avg_total = np.mean([r['total_time'] for r in iterations]) * 1000
        
        print(f"Batch Size: {batch_size}")
        print(f"  Avg Throughput: {avg_throughput:.0f} vectors/s ({avg_bandwidth:.1f} MB/s)")
        print(f"  Avg Timings:")
        print(f"    Generation:  {avg_gen:7.2f}ms ({avg_gen/avg_total*100:5.1f}%)")
        print(f"    Stream:      {avg_stream:7.2f}ms ({avg_stream/avg_total*100:5.1f}%)")
        print(f"    Write:       {avg_write:7.2f}ms ({avg_write/avg_total*100:5.1f}%)")
        print(f"    Close:       {avg_close:7.2f}ms ({avg_close/avg_total*100:5.1f}%)")
        print(f"    Total:       {avg_total:7.2f}ms")
        print()

def check_index_status(client, dataset_name):
    """Check if dataset has been indexed"""
    try:
        req = json.dumps({"dataset": dataset_name}).encode("utf-8")
        action = flight.Action("Status", req)
        results = list(client.do_action(action))
        if results:
            status = json.loads(results[0].body.to_pybytes())
            print(f"  Dataset: {dataset_name}")
            print(f"    Records: {status.get('record_count', 'N/A')}")
            print(f"    Index size: {status.get('index_size', 'N/A')}")
    except Exception as e:
        print(f"  Status check failed: {e}")

if __name__ == "__main__":
    uri = "grpc://localhost:3000"
    if len(sys.argv) > 1:
        uri = sys.argv[1]
    
    print(f"Connecting to {uri}")
    client = get_client(uri)
    
    # Test with progressively larger batches
    batch_sizes = [100, 500, 1000, 3000, 5000]
    
    print("\nStarting ingestion profiling...")
    results = profile_ingestion(client, batch_sizes, iterations=3)
    
    print_summary(results)
    
    # Check index status for a few datasets
    print(f"\n{'='*80}")
    print("INDEX STATUS")
    print(f"{'='*80}\n")
    for batch_size in [100, 1000, 5000]:
        check_index_status(client, f"{DATASET}_{batch_size}_0")
