#!/usr/bin/env python3
import time
import argparse
import sys
import numpy as np
import pyarrow as pa
import pyarrow.flight as flight
import requests
import json
import threading
import os
from concurrent.futures import ThreadPoolExecutor

# Config
DATASET = "bench_progressive"
DIM = 384
SIZES = [3000, 5000, 9000, 15000]
PROFILES_DIR = "profiles_progressive"

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

def ingest(clients, start_id, count):
    # Round robin
    batch_size = 100
    total = 0
    start = time.time()
    for i in range(0, count, batch_size):
        end = min(i + batch_size, count)
        batch = generate_batch(start_id + i, end - i, DIM)
        client = clients[i % len(clients)]
        try:
            descriptor = flight.FlightDescriptor.for_path(DATASET)
            writer, _ = client.do_put(descriptor, batch.schema)
            writer.write_table(batch)
            writer.close()
            total += (end - i)
        except Exception as e:
            print(f"Ingest error: {e}")
    duration = time.time() - start
    print(f"Ingested {total} vectors in {duration:.2f}s ({total/duration:.0f} vectors/s)")

def benchmark_search(clients, k=10, num_queries=1000, concurrency=4):
    print(f"Benchmarking search (n={num_queries}, c={concurrency})...")
    
    latencies = []
    errors = 0
    
    def run_query():
        nonlocal errors
        try:
            vec = np.random.rand(DIM).astype(np.float32).tolist()
            req = json.dumps({
                "dataset": DATASET,
                "vector": vec,
                "k": k
            }).encode("utf-8")
            
            client = clients[np.random.randint(len(clients))]
            # Assuming client URI is data port, need meta port for action? 
            # Or assume Action is available on same port? 
            # In Longbow, VectorSearch action is on the same Flight service.
            
            t0 = time.time()
            action = flight.Action("VectorSearch", req)
            list(client.do_action(action))
            latencies.append((time.time() - t0) * 1000)
        except Exception as e:
            # print(e)
            errors += 1

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(run_query) for _ in range(num_queries)]
        for f in futures:
            f.result()
            
    if latencies:
        p50 = np.percentile(latencies, 50)
        p99 = np.percentile(latencies, 99)
        print(f"Search P50: {p50:.2f}ms, P99: {p99:.2f}ms, Errors: {errors}")
    else:
        print("No successful searches.")

def benchmark_deletion(clients, ids_to_delete):
    print(f"Benchmarking deletion of {len(ids_to_delete)} IDs...")
    start = time.time()
    # Simple delete action? Or repeated DoAction?
    # Assuming 'Delete' action exists taking json list of IDs or single ID.
    # If not, skipping deletion bench/simulating it.
    # Looking at code, usually 'Delete' action.
    
    # We'll try batch deletion if supported, or single.
    # Let's assume single for worst case or batch.
    # Emulating bulk delete via Action "Delete"
    
    # Just do one bulk delete call per node? Vector deletion must be propagated.
    # Ideally send to leader.
    
    client = clients[0]
    payload = json.dumps({"dataset": DATASET, "ids": ids_to_delete}).encode("utf-8")
    try:
        # Check if 'BatchDelete' exists, otherwise loop.
        # Longbow usually supports 'Delete' taking one ID?
        # Let's try 'BatchDelete' or just loop 'Delete'.
        # For bench purposes, we loop concurrently.
        
        def del_one(id):
             req = json.dumps({"dataset": DATASET, "id": id}).encode("utf-8")
             action = flight.Action("Delete", req)
             list(client.do_action(action))

        with ThreadPoolExecutor(max_workers=10) as ex:
             ex.map(del_one, ids_to_delete)

    except Exception as e:
        print(f"Deletion error: {e}")
        
    duration = time.time() - start
    print(f"Deletion took {duration:.2f}s")

def collect_pprof(urls, label=""):
    print(f"Collecting pprof for phase {label}...")
    if not os.path.exists(PROFILES_DIR):
        os.makedirs(PROFILES_DIR)
        
    for i, url in enumerate(urls):
        try:
            # Heap
            res = requests.get(f"{url}/debug/pprof/heap")
            with open(f"{PROFILES_DIR}/heap_{label}_node{i}.pprof", "wb") as f:
                f.write(res.content)
            # Profile (CPU)
            res = requests.get(f"{url}/debug/pprof/profile?seconds=5")
            with open(f"{PROFILES_DIR}/cpu_{label}_node{i}.pprof", "wb") as f:
                f.write(res.content)
        except Exception as e:
            print(f"Failed pprof {url}: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uris", default="grpc://localhost:3000,grpc://localhost:3010,grpc://localhost:3020")
    parser.add_argument("--pprof", default="http://localhost:9090,http://localhost:9091,http://localhost:9092")
    args = parser.parse_args()
    
    clients = [get_client(u) for u in args.uris.split(",")]
    pprof_urls = args.pprof.split(",")
    
    current_count = 0
    
    print("Starting Progressive Benchmark...")
    
    for target in SIZES:
        needed = target - current_count
        print(f"\n=== Phase: Reach {target} vectors (adding {needed}) ===")
        
        # 1. Ingest
        if needed > 0:
            ingest(clients, current_count, needed)
            current_count = target
        
        # Allow settling
        time.sleep(2)
        
        # 2. Search Bench
        benchmark_search(clients)
        
        # 3. PProf
        collect_pprof(pprof_urls, label=f"{target}")
        
    # 4. Deletion Bench (Delete last 1000 IDs)
    print("\n=== Phase: Deletion Verification ===")
    ids_to_del = list(range(current_count - 1000, current_count))
    benchmark_deletion(clients, ids_to_del)
    
    # 5. Verify Search after delete (should be fewer results or success)
    benchmark_search(clients, num_queries=100)
    
    collect_pprof(pprof_urls, label="final")
    print("Benchmark Complete.")

if __name__ == "__main__":
    main()
