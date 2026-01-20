
import time
import argparse
import sys
import numpy as np
import pyarrow as pa
import pyarrow.flight as flight
import json
import requests
import os
import threading

# Config
DATASET = "soak_perf_384d_f32"
DIM = 384
PROFILES_DIR = "profiles_384d"

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

def collect_pprof(url, label, duration=30):
    print(f"  Starting pprof capture on {url} for {duration}s...")
    try:
        if not os.path.exists(PROFILES_DIR):
            os.makedirs(PROFILES_DIR)
            
        # CPU Profile
        res = requests.get(f"{url}/debug/pprof/profile?seconds={duration}", timeout=duration+10)
        with open(f"{PROFILES_DIR}/cpu_{label}_{duration}s.pprof", "wb") as f:
            f.write(res.content)
        print(f"  Captured CPU profile from {url}")
        
    except Exception as e:
        print(f"  Failed to capture profile from {url}: {e}")

def benchmark_put_sustained(client, batch_size, duration_sec):
    print(f"Starting Sustained Ingestion (Batch={batch_size}, Duration={duration_sec}s)...")
    
    # Pre-generate 100 batches (enough to cycle without cache effects strictly, but fast)
    print("  Pre-generating batches to avoid client bottleneck...")
    batches = []
    for i in range(20):
        # reuse IDs? Ideally different IDs to avoid dedup logic overhead if any
        # But for ingestion throughput (network/wal), overwriting is fine or use sequential IDs offset
        # To be safe, we'll increment IDs in the loop but reuse the vector data buffers?
        # Constructing Arrow Table is fast if data is ready.
        # Let's just generate full tables.
        batches.append(generate_batch(i * batch_size, batch_size, DIM))
    
    # Start pprof in background after 5 seconds
    pprof_thread = threading.Timer(5.0, collect_pprof, args=["http://localhost:9090", "node1", 30])
    pprof_thread.start()
    
    start_time = time.time()
    total_vectors = 0
    batch_idx = 0
    
    try:
        while time.time() - start_time < duration_sec:
            # We reuse batches but 'do_put' sends them as is. 
            # This means we Are overwriting IDs 0..N*Batch repeatedly.
            # Longbow treats upserts as writes, so workload is similar.
            batch = batches[batch_idx % len(batches)]
            batch_idx += 1
            
            try:
                descriptor = flight.FlightDescriptor.for_path(DATASET)
                writer, _ = client.do_put(descriptor, batch.schema)
                writer.write_table(batch)
                writer.close()
                total_vectors += batch_size
                
                if total_vectors % 100000 == 0:
                     print(f"  Ingested {total_vectors} vectors...")
                     
            except Exception as e:
               print(f"  Error: {e}")
               time.sleep(1)
               
    finally:
        pprof_thread.join()

    total_duration = time.time() - start_time
    throughput = total_vectors / total_duration
    mb_s = (throughput * DIM * 4) / (1024 * 1024)
    
    print(f"\nResult:")
    print(f"  Total Vectors: {total_vectors}")
    print(f"  Duration: {total_duration:.2f}s")
    print(f"  Throughput: {throughput:.0f} vectors/s")
    print(f"  Bandwidth: {mb_s:.2f} MB/s")

def main():
    client = get_client("grpc://localhost:3000")
    
    # Initial Warmup
    print("Warming up...")
    benchmark_put_sustained(client, 1000, 5)
    
    # Real Run
    print(f"\n{'='*60}")
    print(f"PROFILE RUN: 384d Float32 (1000 batch)")
    print(f"{'='*60}")
    
    # Run for 15s to measure peak throughput before memory limits/compaction pressure
    benchmark_put_sustained(client, 1000, 15)

if __name__ == "__main__":
    main()
