
import time
import argparse
import sys
import numpy as np
import pyarrow as pa
import pyarrow.flight as flight
import json

DATASET = "bench_batch_384"
DIM = 384

def get_client(uri):
    return flight.FlightClient(uri)

def generate_batch(start_id, count, dim):
    ids = pa.array([str(i) for i in range(start_id, start_id + count)], type=pa.string())
    data = np.random.rand(count, dim).astype(np.float32)
    tensor_type = pa.list_(pa.float32(), dim)
    flat_data = data.flatten()
    vectors = pa.FixedSizeListArray.from_arrays(flat_data, type=tensor_type)
    ts = pa.array([time.time_ns()] * count, type=pa.timestamp("ns"))
    
    fields = [
        pa.field("id", pa.int64()),
        pa.field("vector", tensor_type),
        pa.field("timestamp", pa.timestamp("ns")),
    ]
    return pa.Table.from_arrays([ids, vectors, ts], schema=pa.schema(fields))

def generate_batch_with_metadata(start_id, count, dim):
    ids = pa.array([str(i) for i in range(start_id, start_id + count)], type=pa.string())
    data = np.random.rand(count, dim).astype(np.float32)
    tensor_type = pa.list_(pa.float32(), dim)
    flat_data = data.flatten()
    vectors = pa.FixedSizeListArray.from_arrays(flat_data, type=tensor_type)
    ts = pa.array([time.time_ns()] * count, type=pa.timestamp("ns"))
    
    categories = pa.array([f"cat_{i % 10}" for i in range(count)], type=pa.string())
    texts = pa.array([f"document {i} about topic {i % 5}" for i in range(count)], type=pa.string())
    
    fields = [
        pa.field("id", pa.int64()),
        pa.field("vector", tensor_type),
        pa.field("timestamp", pa.timestamp("ns")),
        pa.field("category", pa.string()),
        pa.field("text", pa.string()),
    ]
    return pa.Table.from_arrays([ids, vectors, ts, categories, texts], schema=pa.schema(fields))

def benchmark_put(client, batch_size, metadata=False, total_vectors=50000):
    print(f"Testing Batch Size: {batch_size}, Metadata: {metadata}")
    
    # New dataset per run
    ds_name = f"{DATASET}_{batch_size}_{metadata}"
    
    start = time.time()
    total = 0
    for i in range(0, total_vectors, batch_size):
        end = min(i + batch_size, total_vectors)
        if metadata:
            batch = generate_batch_with_metadata(i, end - i, DIM)
        else:
            batch = generate_batch(i, end - i, DIM)
            
        try:
            descriptor = flight.FlightDescriptor.for_path(ds_name)
            writer, _ = client.do_put(descriptor, batch.schema)
            writer.write_table(batch)
            writer.close()
            total += (end - i)
        except Exception as e:
            print(f"  Error: {e}")
            
    duration = time.time() - start
    mb_s = (total * DIM * 4) / (1024 * 1024) / duration
    print(f"  Result: {total} vectors in {duration:.4f}s = {mb_s:.2f} MB/s")
    return mb_s

def main():
    client = get_client("grpc://localhost:3000")
    
    # Warmup
    benchmark_put(client, 1000, False, 5000)
    
    print("\nStarting Metadata Impact Test (Dim 384, Float32)...")
    
    # Baseline (No Metadata)
    benchmark_put(client, 1000, False, 50000)
    
    # With Metadata
    benchmark_put(client, 1000, True, 50000)

if __name__ == "__main__":
    main()
