#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime

def run_command(cmd, env=None, capture_output=True, timeout=None):
    try:
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=capture_output,
            text=True,
            timeout=timeout,
            shell=True
        )
        return result
    except subprocess.TimeoutExpired:
        print(f"Command timed out after {timeout}s: {cmd}")
        return None

class BenchmarkRunner:
    def __init__(self, args):
        self.args = args
        self.server_addr = os.environ.get("LONGBOW_ADDR", "127.0.0.1:3000")
        self.node_id = os.environ.get("LONGBOW_NODE_ID", "bench1")
        self.data_dir = os.environ.get("LONGBOW_DATA_PATH", os.path.join(os.getcwd(), "data/bench"))
        
        self.bin_dir = os.path.join(os.getcwd(), "bin")
        self.log_dir = os.path.join(os.getcwd(), "data/perf_logs")
        os.makedirs(self.log_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)
        
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_file = os.path.join(self.log_dir, f"perf_matrix_{args.mode}_{self.timestamp}.json")
        self.results = []
        self.server_pid = None

    def start_server(self, label):
        # Cleanup
        self.stop_server()
        
        server_bin = os.path.join(self.bin_dir, "longbow-metal" if self.args.mode == "metal" else "longbow")
        if not os.path.exists(server_bin):
            print(f"Error: Server binary not found at {server_bin}")
            sys.exit(1)
            
        # Unique data path for this specific run config
        data_root = os.path.join(self.data_dir, label)
        # Extreme cleanup for fresh state
        subprocess.run(f"rm -rf {data_root}", shell=True)
        os.makedirs(data_root, exist_ok=True)
        
        env = os.environ.copy()
        env["LONGBOW_MAX_MEMORY"] = str(self.args.memory)
        env["ARROW_DISABLE_LOCKING"] = "1"
        
        log_file = os.path.join(self.log_dir, f"longbow_{self.args.mode}_{label}.log")
        print(f"  Starting fresh server (log: {os.path.basename(log_file)})...")
        
        with open(log_file, "w") as f:
            process = subprocess.Popen(
                [server_bin, "--listen-addr", self.server_addr, "--data-path", data_root, "--node-id", self.node_id],
                env=env,
                stdout=f,
                stderr=subprocess.STDOUT
            )
            self.server_pid = process.pid
            
        # Wait for server
        startup_timeout = int(os.environ.get("LONGBOW_STARTUP_TIMEOUT", "60"))
        port = self.server_addr.split(":")[-1]
        
        for _ in range(startup_timeout):
            result = run_command(f"lsof -i :{port}")
            if result and "LISTEN" in result.stdout:
                wait_after = int(os.environ.get("LONGBOW_WAIT_AFTER_START", "5"))
                time.sleep(wait_after)
                return True
            time.sleep(1)
            
        print(f"  WARNING: Server may not be ready on port {port}")
        return False

    def stop_server(self):
        if self.server_pid:
            try:
                subprocess.run(f"kill -9 {self.server_pid}", shell=True, stderr=subprocess.DEVNULL)
            except:
                pass
            self.server_pid = None
        
        # Force cleanup port 3000 (or configured port)
        subprocess.run(f"pkill -9 -f 'longbow.*{self.node_id}'", shell=True, stderr=subprocess.DEVNULL)
        time.sleep(2)

    def run_benchmark(self, dim, dtype, count):
        bench_tool = os.path.join(self.bin_dir, "benchmark-tool")
        dataset = f"bench_{self.args.mode}_{dtype}_{dim}_{count}"
        result_json = os.path.join(self.log_dir, f"result_{self.args.mode}_{dtype}_{dim}_{count}.json")
        
        cmd = [
            bench_tool,
            f"--uri={self.server_addr}",
            f"--dim={dim}",
            f"--dtype={dtype}",
            f"--scale={count}",
            f"--queries={self.args.queries}",
            f"--dataset={dataset}",
            f"--json={result_json}"
        ]
        
        print(f"  Running benchmark: {dtype} dim={dim} count={count}...")
        result = run_command(" ".join(cmd), timeout=self.args.timeout)
        
        if result and result.returncode == 0:
            if os.path.exists(result_json):
                with open(result_json, "r") as f:
                    self.results.extend(json.load(f))
                return True
        else:
            print(f"  FAILED: {dtype} dim={dim} count={count}")
            return False

    def run_pprof(self, label):
        print(f"  Capturing pprof profiles for {label}...")
        profiles = ["profile", "heap", "mutex", "block"]
        for p in profiles:
            out = os.path.join(self.log_dir, f"{label}_{p}.prof")
            if p == "profile":
                run_command(f"curl -s \"http://127.0.0.1:9090/debug/pprof/{p}?seconds=10\" -o {out}")
            else:
                run_command(f"curl -s \"http://127.0.0.1:9090/debug/pprof/{p}\" -o {out}")

    def execute(self):
        dims = [int(d) for d in self.args.dims.split(",")]
        counts = [int(c) for c in self.args.counts.split(",")]
        dtypes = self.args.dtypes.split(",")
        
        total = len(dims) * len(counts) * len(dtypes)
        current = 0
        
        print("="*80)
        print(f"UNIFIED BENCHMARK MATRIX ({self.args.mode.upper()})")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Dims: {dims}")
        print(f"Counts: {counts}")
        print(f"Types: {dtypes}")
        print("="*80)
        
        try:
            for dim in dims:
                for dtype in dtypes:
                    for count in counts:
                        current += 1
                        label = f"{dtype}_{dim}_{count}"
                        print(f"\n[{current}/{total}] {label}")
                        
                        if self.start_server(label):
                            if self.run_benchmark(dim, dtype, count):
                                if self.args.pprof and current == total: # Capture pprof on last run or specific ones
                                    self.run_pprof(label)
                                print("  DONE")
                            else:
                                print("  Benchmark reported failure")
                        else:
                            print("  Server failed to start")
        finally:
            self.stop_server()
            
        with open(self.output_file, "w") as f:
            json.dump(self.results, f, indent=2)
            
        print("\n" + "="*80)
        print(f"Results saved to: {self.output_file}")
        print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified Longbow Benchmark Script")
    parser.add_argument("--mode", choices=["cpu", "metal"], default="cpu", help="Benchmark mode")
    parser.add_argument("--dims", default="128,384,768,1536,3072", help="Comma-separated dimensions")
    parser.add_argument("--counts", default="1000,3000,5000,7000,10000,15000,25000", help="Comma-separated vector counts")
    parser.add_argument("--dtypes", default="float32,float64,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant", help="Comma-separated datatypes")
    parser.add_argument("--memory", type=int, default=21474836480, help="LONGBOW_MAX_MEMORY (default 20GB)")
    parser.add_argument("--queries", type=int, default=200, help="Number of search queries per test")
    parser.add_argument("--timeout", type=int, default=600, help="Timeout in seconds per test")
    parser.add_argument("--pprof", action="store_true", help="Capture pprof profiles")
    
    args = parser.parse_args()
    runner = BenchmarkRunner(args)
    runner.execute()
