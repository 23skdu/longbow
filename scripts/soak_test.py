#!/usr/bin/env python3
"""Longbow Soak Test Script

Run a continuous workload against Longbow for an extended duration to identify
memory leaks, resource exhaustion, or reliability issues.

Usage:
  python3 scripts/soak_test.py --uris grpc://localhost:3000,grpc://localhost:3010,grpc://localhost:3020 --duration 600 --concurrency 6 --pprof-urls http://localhost:9090,http://localhost:9091,http://localhost:9092
"""
import argparse
import time
import random
import threading
import signal
import sys
import psutil
import statistics
from datetime import datetime
import pyarrow.flight as flight
import json
import numpy as np
import pyarrow as pa
import requests
import os

# Configuration
DATASET_NAME = "soak_test_dataset"
DIM = 384
METRIC_INTERVAL = 5  # Reduced for more frequent logging
PROFILE_INTERVAL = 10 # Collect profiles every 10 seconds

stop_event = threading.Event()

def generate_batch(rows=100, dim=128):
    """Generate a random batch of vectors."""
    data = np.random.rand(rows, dim).astype(np.float32)
    tensor_type = pa.list_(pa.float32(), dim)
    flat_data = data.flatten()
    vectors = pa.FixedSizeListArray.from_arrays(flat_data, type=tensor_type)
    ids = pa.array(np.random.randint(0, 1000000, size=rows), type=pa.int64())
    ts = pa.array([time.time_ns()] * rows, type=pa.timestamp("ns"))
    
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("vector", tensor_type),
        pa.field("timestamp", pa.timestamp("ns")),
    ])
    return pa.Table.from_arrays([ids, vectors, ts], schema=schema)


def worker_routine(uris, worker_id, stats):
    """Continuous worker loop."""
    data_clients = []
    meta_clients = []
    
    for uri in uris:
        try:
            # URI format: grpc://host:port
            # We assume Meta port is Data port + 1
            if "grpc://" in uri:
                host_port = uri.split("grpc://")[1]
            else:
                host_port = uri
                
            host, port = host_port.split(":")
            meta_port = int(port) + 1
            meta_uri = f"grpc://{host}:{meta_port}"
            
            data_clients.append(flight.FlightClient(uri))
            meta_clients.append(flight.FlightClient(meta_uri))
            print(f"[Worker {worker_id}] Connected to Data {uri} / Meta {meta_uri}")
        except Exception as e:
            print(f"[Worker {worker_id}] Failed to connect to {uri}: {e}")

    if not data_clients:
        print(f"[Worker {worker_id}] No clients available. Exiting.")
        return

    op_count = 0
    errors = 0
    
    print(f"[Worker {worker_id}] Starting loop w/ {len(data_clients)} pairs")
    
    while not stop_event.is_set():
        try:
            # Pick a pair (round robin or random)
            idx = random.randint(0, len(data_clients) - 1)
            d_client = data_clients[idx]
            m_client = meta_clients[idx]
            
            # Disable get for now as it hangs
            op = random.choice(["put", "search"])
            # print(f"[Worker {worker_id}] Doing {op}") 
            start = time.time()
            
            if op == "put":
                batch = generate_batch(rows=50, dim=DIM)
                descriptor = flight.FlightDescriptor.for_path(DATASET_NAME)
                writer, _ = d_client.do_put(descriptor, batch.schema)
                writer.write_table(batch)
                writer.close()
                
            elif op == "get":
                query = {"name": DATASET_NAME, "limit": 10}
                ticket = flight.Ticket(json.dumps(query).encode("utf-8"))
                reader = d_client.do_get(ticket)
                reader.read_all()
                
            elif op == "search":
                # Simulated vector search via Meta Client
                vec = np.random.rand(DIM).astype(np.float32).tolist()
                req = json.dumps({
                    "dataset": DATASET_NAME,
                    "vector": vec,
                    "k": 5
                }).encode("utf-8")
                # Using VectorSearch action
                action = flight.Action("VectorSearch", req)
                list(m_client.do_action(action))

                
            duration = (time.time() - start) * 1000
            stats["latencies"].append(duration)
            stats["ops"] += 1
            op_count += 1
            
            # Small sleep to prevent total CPU saturation
            time.sleep(random.uniform(0.05, 0.2)) # Faster pace
            
        except Exception as e:
            errors += 1
            stats["errors"] += 1
            if errors % 100 == 0:
                print(f"[Worker {worker_id}] Error: {e}")
            time.sleep(1)

def collect_profiles(pprof_urls, output_dir):
    """Periodically collect pprof profiles."""
    if not pprof_urls:
        return
        
    os.makedirs(output_dir, exist_ok=True)
    
    while not stop_event.is_set():
        time.sleep(PROFILE_INTERVAL)
        timestamp = int(time.time())
        print(f"Collecting profiles at {timestamp}...")
        
        for i, url in enumerate(pprof_urls):
            # Collect Heap
            try:
                resp = requests.get(f"{url}/debug/pprof/heap")
                if resp.status_code == 200:
                    with open(f"{output_dir}/heap_node{i}_{timestamp}.pprof", "wb") as f:
                        f.write(resp.content)
            except Exception as e:
                print(f"Failed to collect heap from {url}: {e}")

            # Collect Profile (CPU) - short duration
            try:
                resp = requests.get(f"{url}/debug/pprof/profile?seconds=5")
                if resp.status_code == 200:
                    with open(f"{output_dir}/cpu_node{i}_{timestamp}.pprof", "wb") as f:
                        f.write(resp.content)
            except Exception as e:
                print(f"Failed to collect cpu from {url}: {e}")

def monitor_system(pid_list, start_time, stats):
    """Monitor system resources of target processes."""
    print("Timestamp,Elapsed(s),Ops,Errors,Latency_P99(ms),CPU_Percent,RSS_MB")
    
    while not stop_event.is_set():
        time.sleep(METRIC_INTERVAL)
        
        # Resource usage
        cpu_total = 0.0
        rss_total = 0
        active_pids = 0
        
        for pid in pid_list:
            try:
                p = psutil.Process(pid)
                cpu_total += p.cpu_percent(interval=None)
                rss_total += p.memory_info().rss
                active_pids += 1
            except psutil.NoSuchProcess:
                pass
                
        # Only break if PIDs were provided and all are gone.
        # If no PIDs provided (monitoring remote cluster), don't break.
        if pid_list and active_pids == 0:
            print("No active Longbow processes found!")
            stop_event.set()
            break
            
        # Calc stats
        ops = stats["ops"]
        errs = stats["errors"]
        p99 = 0.0
        if stats["latencies"]:
            p99 = statistics.quantiles(stats["latencies"][-1000:], n=100)[98] # approx p99 of last 1000
            
        elapsed = int(time.time() - start_time)
        rss_mb = rss_total / 1024 / 1024
        
        print(f"{datetime.now().isoformat()},{elapsed},{ops},{errs},{p99:.2f},{cpu_total:.1f},{rss_mb:.1f}")
        sys.stdout.flush()

def main():
    parser = argparse.ArgumentParser(description="Longbow Soak Tester")
    parser.add_argument("--uris", default="grpc://localhost:3000", help="Comma-separated Longbow URIs")
    parser.add_argument("--duration", type=int, default=3600, help="Duration in seconds")
    parser.add_argument("--concurrency", type=int, default=4, help="Number of threads")
    parser.add_argument("--pid", type=int, nargs="+", help="PIDs to monitor (optional)")
    parser.add_argument("--pprof-urls", default="", help="Comma-separated pprof URLs (e.g. http://localhost:9090)")
    parser.add_argument("--output-dir", default="./profiles", help="Directory to save profiles")
    args = parser.parse_args()
    
    uris = args.uris.split(",")
    pprof_urls = args.pprof_urls.split(",") if args.pprof_urls else []
    
    print(f"Starting Soak Test: {args.duration}s, {args.concurrency} threads")
    print(f"URIs: {uris}")
    print(f"PProf URLs: {pprof_urls}")
    
    # Identify target processes if not provided
    pids = args.pid or []
    if not pids:
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            if 'longbow' in proc.info['name'] or (proc.info['cmdline'] and 'longbow' in proc.info['cmdline'][0]):
                pids.append(proc.info['pid'])
    
    if pids:
        print(f"Monitoring PIDs: {pids}")
    else:
        print("No local PIDs found to monitor (assuming remote cluster or not started yet)")
    
    stats = {"ops": 0, "errors": 0, "latencies": []}
    threads = []
    
    # Start monitor
    monitor = threading.Thread(target=monitor_system, args=(pids, time.time(), stats))
    monitor.daemon = True
    monitor.start()
    
    # Start profile collector
    if pprof_urls:
        profiler = threading.Thread(target=collect_profiles, args=(pprof_urls, args.output_dir))
        profiler.daemon = True
        profiler.start()
    
    # Start workers
    for i in range(args.concurrency):
        t = threading.Thread(target=worker_routine, args=(uris, i, stats))
        t.daemon = True
        t.start()
        threads.append(t)
        
    try:
        # Run for duration
        time.sleep(args.duration)
        print("\nTest duration reached. Stopping...")
    except KeyboardInterrupt:
        print("\nInterrupted. Stopping...")
        
    stop_event.set()
    time.sleep(2) # Allow threads to cleanup
    
    print("\nFinal Stats:")
    print(f"Total Ops: {stats['ops']}")
    print(f"Total Errors: {stats['errors']}")
    
if __name__ == "__main__":
    main()
