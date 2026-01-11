#!/usr/bin/env python3
"""Quick ingestion test to verify batch size fix"""
import time
import numpy as np
import pyarrow as pa
import pyarrow.flight as flight

DATASET = "perf_test"
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

def test_ingestion(client, count, batch_size):
    print(f"Testing ingestion: {count} vectors, batch_size={batch_size}")
    total = 0
    start = time.time()
    
    for i in range(0, count, batch_size):
        end = min(i + batch_size, count)
        batch = generate_batch(i, end - i, DIM)
        descriptor = flight.FlightDescriptor.for_path(DATASET)
        writer, _ = client.do_put(descriptor, batch.schema)
        writer.write_table(batch)
        writer.close()
        total += (end - i)
    
    duration = time.time() - start
    throughput_vps = total / duration
    throughput_mbs = (throughput_vps * DIM * 4) / (1024 * 1024)
    print(f"  {total} vectors in {duration:.2f}s")
    print(f"  Throughput: {throughput_vps:.0f} vectors/s ({throughput_mbs:.1f} MB/s)")
    return throughput_mbs

if __name__ == "__main__":
    client = get_client("grpc://localhost:3000")
    
    print("Testing batch_size=1000 (fixed)")
    test_ingestion(client, 5000, 1000)
