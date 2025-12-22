#!/usr/bin/env python3
"""Longbow Soak Test Script

Run a continuous workload against Longbow for an extended duration to identify
memory leaks, resource exhaustion, or reliability issues.

Usage:
  python3 scripts/soak_test.py --duration 86400 --concurrency 4
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

# Configuration
DATASET_NAME = "soak_test_dataset"
DIM = 128
METRIC_INTERVAL = 60  # seconds

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

def worker_routine(uri, worker_id, stats):
    """Continuous worker loop."""
    client = flight.FlightClient(uri)
    op_count = 0
    errors = 0
    
    while not stop_event.is_set():
        try:
            op = random.choice(["put", "get", "search"])
            start = time.time()
            
            if op == "put":
                batch = generate_batch(rows=50, dim=DIM)
                descriptor = flight.FlightDescriptor.for_path(DATASET_NAME)
                writer, _ = client.do_put(descriptor, batch.schema)
                writer.write_table(batch)
                writer.close()
                
            elif op == "get":
                query = {"name": DATASET_NAME, "limit": 10}
                ticket = flight.Ticket(json.dumps(query).encode("utf-8"))
                reader = client.do_get(ticket)
                reader.read_all()
                
            elif op == "search":
                # Simulated vector search
                vec = np.random.rand(DIM).astype(np.float32).tolist()
                req = json.dumps({
                    "dataset": DATASET_NAME,
                    "vector": vec,
                    "k": 5
                }).encode("utf-8")
                action = flight.Action("VectorSearch", req)
                list(client.do_action(action))
                
            duration = (time.time() - start) * 1000
            stats["latencies"].append(duration)
            stats["ops"] += 1
            op_count += 1
            
            # Small sleep to prevent total CPU saturation
            time.sleep(random.uniform(0.1, 0.5))
            
        except Exception as e:
            errors += 1
            stats["errors"] += 1
            if errors % 100 == 0:
                print(f"[Worker {worker_id}] Error: {e}")
            time.sleep(1)

def monitor_system(pid_list, start_time):
    """Monitor system resources of target processes."""
    print("Timestamp,Elapsed(s),Ops,Errors,Latency_P99(ms),CPU_Percent,RSS_MB")
    
    stats = {"ops": 0, "errors": 0, "latencies": []}
    
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
                
        if active_pids == 0:
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
    parser.add_argument("--uri", default="grpc://localhost:3000", help="Longbow URI")
    parser.add_argument("--duration", type=int, default=3600, help="Duration in seconds")
    parser.add_argument("--concurrency", type=int, default=4, help="Number of threads")
    parser.add_argument("--pid", type=int, nargs="+", help="PIDs to monitor (optional)")
    args = parser.parse_args()
    
    print(f"Starting Soak Test: {args.duration}s, {args.concurrency} threads")
    
    # Identify target processes if not provided
    pids = args.pid or []
    if not pids:
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            if 'longbow' in proc.info['name'] or (proc.info['cmdline'] and 'longbow' in proc.info['cmdline'][0]):
                pids.append(proc.info['pid'])
    
    print(f"Monitoring PIDs: {pids}")
    
    stats = {"ops": 0, "errors": 0, "latencies": []}
    threads = []
    
    # Start monitor
    monitor = threading.Thread(target=monitor_system, args=(pids, time.time()))
    monitor.daemon = True
    monitor.start()
    
    # Start workers
    for i in range(args.concurrency):
        t = threading.Thread(target=worker_routine, args=(args.uri, i, stats))
        t.daemon = True
        t.start()
        threads.append(t)
        
    try:
        time.sleep(args.duration)
        print("\nTest duration reached. Stopping...")
    except KeyboardInterrupt:
        print("\nInterrupted. Stopping...")
        
    stop_event.set()
    time.sleep(2)
    
    print("\nFinal Stats:")
    print(f"Total Ops: {stats['ops']}")
    print(f"Total Errors: {stats['errors']}")
    
if __name__ == "__main__":
    main()
