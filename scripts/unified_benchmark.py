#!/usr/bin/env python3
"""
Longbow Unified Benchmark Script

Runs ingest and search benchmarks across multiple dimensions, data types, and counts.
Supports CPU, Metal (macOS), and CUDA (Linux) modes.

Usage:
    python3 scripts/unified_benchmark.py --mode cpu
    python3 scripts/unified_benchmark.py --mode metal
    python3 scripts/unified_benchmark.py --mode cuda
    python3 scripts/unified_benchmark.py --mode cpu --dtypes float32,int8,int16
    python3 scripts/unified_benchmark.py --dims 128,384,768,1536,3072
"""

import argparse
import json
import os
import platform
import re
import subprocess
import sys
import time
import numpy as np
from datetime import datetime

try:
    from longbow import LongbowClient

    HAS_LONGBOW_SDK = True
except ImportError:
    HAS_LONGBOW_SDK = False

# All supported data types
ALL_DTYPES = "float32,float64,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant"

# Bytes per element for each dtype
DTYPE_BYTES = {
    "int8": 1,
    "uint8": 1,
    "int16": 2,
    "uint16": 2,
    "int32": 4,
    "uint32": 4,
    "float32": 4,
    "complex64": 4,
    "int64": 8,
    "uint64": 8,
    "float64": 8,
    "complex128": 8,
    "turboquant": 1,
}


def run_command(cmd, env=None, capture_output=True, timeout=None):
    try:
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=capture_output,
            text=True,
            timeout=timeout,
            shell=True,
        )
        return result
    except subprocess.TimeoutExpired:
        print(f"  Command timed out after {timeout}s")
        return None


def parse_bench_json(json_file):
    """Parse benchmark-tool JSON output to extract metrics."""
    try:
        with open(json_file) as f:
            results = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

    metrics = {}
    for r in results:
        name = r.get("name", "")
        if name == "DoPut":
            metrics["ingest_vec_per_sec"] = r.get("throughput", 0)
            metrics["ingest_mb_per_sec"] = r.get("throughput_mbs", 0)
        elif name == "DoGet":
            metrics["get_vec_per_sec"] = r.get("throughput", 0)
            metrics["get_mb_per_sec"] = r.get("throughput_mbs", 0)
        elif name.startswith("Search_"):
            # Generic search parser
            prefix = name.replace("Search_", "").lower()
            metrics[f"{prefix}_qps"] = r.get("throughput", 0)

            # Extract latency percentiles
            latencies = sorted(r.get("latencies_ms", []))
            if latencies:
                metrics[f"{prefix}_p50_ms"] = r.get("p50_latency_ms", 0)
                metrics[f"{prefix}_p90_ms"] = latencies[int(0.9 * (len(latencies) - 1))]
                metrics[f"{prefix}_p95_ms"] = r.get("p95_latency_ms", 0)
                metrics[f"{prefix}_p99_ms"] = r.get("p99_latency_ms", 0)

    return metrics


class BenchmarkRunner:
    def __init__(self, args):
        self.args = args
        self.server_addr = os.environ.get("LONGBOW_ADDR", args.addr)
        self.node_id = os.environ.get("LONGBOW_NODE_ID", "bench1")
        self.data_dir = os.environ.get(
            "LONGBOW_DATA_PATH", os.path.join(os.getcwd(), "data/bench")
        )

        self.bin_dir = os.path.join(os.getcwd(), "bin")
        self.log_dir = os.path.join(os.getcwd(), "data/perf_logs")
        os.makedirs(self.log_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)

        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_file = os.path.join(
            self.log_dir, f"perf_matrix_{args.mode}_{self.timestamp}.json"
        )
        self.results = []
        self.server_pid = None

    def get_server_binary(self):
        mode_binaries = {
            "cpu": "longbow",
            "metal": "longbow-metal",
            "cuda": "longbow-cuda",
        }
        bin_name = mode_binaries.get(self.args.mode, "longbow")
        path = os.path.join(self.bin_dir, bin_name)

        # Fall back to CPU if GPU binary not found
        if not os.path.exists(path) and self.args.mode in ["metal", "cuda"]:
            print(f"  {self.args.mode.upper()} binary not found, using CPU")
            path = os.path.join(self.bin_dir, "longbow")

        return path

    def get_bench_tool(self):
        for name in ["benchmark-tool", "bench-tool"]:
            path = os.path.join(self.bin_dir, name)
            if os.path.exists(path):
                return path
        return os.path.join(self.bin_dir, "benchmark-tool")

    def check_cuda(self):
        if self.args.mode == "cuda" and platform.system() != "Linux":
            print("  CUDA mode only supported on Linux, using CPU")
            return False
        if self.args.mode == "cuda":
            result = run_command(
                "nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null"
            )
            if result and result.returncode == 0 and result.stdout.strip():
                print(f"  CUDA GPU: {result.stdout.strip()}")
                return True
            print("  WARNING: No CUDA GPU detected")
        return True

    def start_server(self, label):
        self.stop_server()

        server_bin = self.get_server_binary()
        if not os.path.exists(server_bin):
            print(f"  Error: Server binary not found at {server_bin}")
            return False

        data_root = os.path.join(self.data_dir, label)
        subprocess.run(f"rm -rf {data_root}", shell=True)
        os.makedirs(data_root, exist_ok=True)

        env = os.environ.copy()
        env["LONGBOW_MAX_MEMORY"] = str(self.args.memory)
        env["ARROW_DISABLE_LOCKING"] = "1"

        # Use unique port per config to avoid conflicts
        base_port = int(self.server_addr.split(":")[-1])
        port = base_port  # Use fixed port since we kill server between runs

        log_file = os.path.join(self.log_dir, f"longbow_{self.args.mode}_{label}.log")

        # Server uses envconfig, not command-line flags
        env["LONGBOW_LISTEN_ADDR"] = f"127.0.0.1:{port}"
        env["LONGBOW_META_ADDR"] = f"127.0.0.1:{port + 1}"
        env["LONGBOW_REST_ADDR"] = f"127.0.0.1:{port + 80}"  # e.g. 3080
        env["LONGBOW_METRICS_ADDR"] = f"127.0.0.1:{port + 6000}"  # e.g. 9000
        env["LONGBOW_DATA_PATH"] = data_root
        env["LONGBOW_NODE_ID"] = self.node_id

        with open(log_file, "w") as f:
            process = subprocess.Popen(
                [server_bin],
                env=env,
                stdout=f,
                stderr=subprocess.STDOUT,
            )
            self.server_pid = process.pid

        # Wait for server to be ready with robust checking
        for i in range(self.args.startup_timeout):
            # Check if process is still running
            if process.poll() is not None:
                print(f"  Server exited with code {process.returncode}")
                self.server_pid = None
                return False

            # Check if port is listening
            result = run_command(f"lsof -i :{port} 2>/dev/null | grep LISTEN")
            if result and result.returncode == 0:
                # Additional wait for indexing workers to start
                time.sleep(3)
                return True
            time.sleep(1)

        print(f"  WARNING: Server startup timeout on port {port}")
        return False

    def stop_server(self):
        if self.server_pid:
            try:
                subprocess.run(
                    f"kill -9 {self.server_pid}", shell=True, stderr=subprocess.DEVNULL
                )
            except:
                pass
            self.server_pid = None

        # Kill any leftovers
        subprocess.run(
            "pkill -9 longbow || true",
            shell=True,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            "pkill -9 longbow-metal || true",
            shell=True,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(2)

    def run_benchmark(self, dim, dtype, count, label):
        """Run benchmark-tool with JSON output for a configuration."""
        bench_tool = self.get_bench_tool()
        batch_size = min(count, self.args.batch_size)
        duration = self.args.duration
        json_file = os.path.join(self.log_dir, f"result_{label}.json")

        # Run benchmark-tool (does ingest + search + all modes)
        cmd = f"{bench_tool} --uri={self.server_addr} --dim={dim} --dtype={dtype} --scale={batch_size} --queries={self.args.queries} --dataset={label} --json={json_file}"
        print(f"  Running {dtype} dim={dim}...", end="", flush=True)
        result = run_command(cmd, timeout=duration * 3 + 60)

        if not result or result.returncode != 0:
            print(" FAILED")
            return False

        metrics = parse_bench_json(json_file)
        if not metrics:
            print(" NO DATA")
            return False

        # Extract all search types
        search_metrics = {}
        for key, value in metrics.items():
            if "_qps" in key:
                prefix = key.replace("_qps", "")
                search_metrics[prefix] = {
                    "qps": value,
                    "p50": metrics.get(f"{prefix}_p50_ms", 0),
                    "p95": metrics.get(f"{prefix}_p95_ms", 0),
                    "p99": metrics.get(f"{prefix}_p99_ms", 0),
                }

        result_entry = {
            "dim": dim,
            "dtype": dtype,
            "count": batch_size,
            "mode": self.args.mode,
            "ingest": {
                "vec_per_sec": metrics.get("ingest_vec_per_sec", 0),
            },
            "search": search_metrics,
            "timestamp": datetime.now().isoformat(),
        }
        self.results.append(result_entry)

        print(f" {metrics.get('ingest_vec_per_sec', 0):.0f} vec/s")
        return True

    def execute_recommend(self):
        if not HAS_LONGBOW_SDK:
            print(
                "Error: longbow Python SDK not installed. Install with: pip install longbow"
            )
            return

        dims = [int(d) for d in self.args.dims.split(",")]
        counts = [int(c) for c in self.args.counts.split(",")]
        alpha_values = [float(a) for a in self.args.alpha_values.split(",")]
        k_values = [int(k) for k in self.args.k_values.split(",")]

        count = counts[0] if counts else 10000
        dim = dims[0] if dims else 128
        dtype = "float32"

        print("=" * 80)
        print(f"RECOMMEND BENCHMARK (Hybrid vs ANN)")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Dim: {dim}, Count: {count}")
        print(f"Alpha values: {alpha_values} (0.0=graph, 1.0=ANN, 0.5=hybrid)")
        print(f"K values: {k_values}")
        print(f"Max hops: {self.args.max_hops}, Decay: {self.args.decay}")
        print("=" * 80)

        label = f"rec_{dtype}_{dim}_{count}"
        if not self.start_server(label):
            print("  Failed to start server!")
            return

        try:
            client = LongbowClient(
                uri=f"grpc://{self.server_addr}",
                meta_uri=f"grpc://{self.server_addr.replace('3000', '3001')}",
            )

            dataset_name = f"rec_bench_{dim}d"
            print(f"\nCreating dataset {dataset_name}...")

            vectors = np.random.rand(count, dim).astype(np.float32).tolist()
            ids = [str(i) for i in range(count)]

            client.insert(
                dataset_name,
                [{"id": id, "vector": vec} for id, vec in zip(ids, vectors)],
            )
            time.sleep(2)

            seed_ids = [str(i) for i in range(self.args.num_seeds)]
            print(f"Using seed IDs: {seed_ids}")

            total_tests = len(alpha_values) * len(k_values)
            current = 0

            for alpha in alpha_values:
                for k in k_values:
                    current += 1
                    print(f"\n[{current}/{total_tests}] Alpha={alpha}, K={k}")

                    latencies = []
                    for _ in range(self.args.queries):
                        start = time.time()
                        try:
                            results = client.recommend(
                                dataset=dataset_name,
                                seed_ids=seed_ids,
                                k=k,
                                alpha=alpha,
                                max_hops=self.args.max_hops,
                                decay=self.args.decay,
                            )
                            latency = (time.time() - start) * 1000
                            latencies.append(latency)
                        except Exception as e:
                            print(f"  Error: {e}")
                            continue

                    if latencies:
                        latencies.sort()
                        qps = 1000.0 / (sum(latencies) / len(latencies))
                        self.results.append(
                            {
                                "dim": dim,
                                "dtype": dtype,
                                "count": count,
                                "alpha": alpha,
                                "k": k,
                                "qps": qps,
                                "p50": latencies[int(0.5 * len(latencies))],
                                "p95": latencies[int(0.95 * len(latencies))],
                                "p99": latencies[int(0.99 * len(latencies))],
                                "timestamp": datetime.now().isoformat(),
                            }
                        )
                        print(
                            f"  QPS: {qps:.1f}, P50: {latencies[int(0.5 * len(latencies))]:.2f}ms"
                        )

        except Exception as e:
            print(f"Error: {e}")
        finally:
            self.stop_server()
            data_root = os.path.join(self.data_dir, label)
            subprocess.run(f"rm -rf {data_root}", shell=True)

        with open(self.output_file, "w") as f:
            json.dump(
                {
                    "mode": "recommend",
                    "timestamp": self.timestamp,
                    "config": {
                        "dim": dim,
                        "count": count,
                        "alpha_values": alpha_values,
                        "k_values": k_values,
                        "max_hops": self.args.max_hops,
                        "decay": self.args.decay,
                        "num_seeds": self.args.num_seeds,
                        "queries": self.args.queries,
                    },
                    "results": self.results,
                },
                f,
                indent=2,
            )

        self.print_summary()
        print(f"\nResults saved to: {self.output_file}")

    def execute(self):
        if self.args.mode == "recommend":
            self.execute_recommend()
            return

        dims = [int(d) for d in self.args.dims.split(",")]
        counts = [int(c) for c in self.args.counts.split(",")]
        dtypes = self.args.dtypes.split(",")

        count = counts[0] if counts else 1000

        total = len(dims) * len(dtypes)
        current = 0

        self.check_cuda()

        print("=" * 80)
        print(f"UNIFIED BENCHMARK MATRIX ({self.args.mode.upper()})")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Platform: {platform.system()} {platform.machine()}")
        print(f"Dims: {dims}")
        print(f"Count: {count}")
        print(f"Types: {dtypes}")
        print(f"Duration per test: {self.args.duration}s")
        print("=" * 80)

        for count in counts:
            print(f"\n{'=' * 70}")
            print(f"Vector Count: {count}")
            print(f"{'=' * 70}")

            for dtype in dtypes:
                print(f"\n{'━' * 70}")
                print(f"Data Type: {dtype} (Count: {count})")
                print(f"{'━' * 70}")

                for dim in dims:
                    current += 1
                    label = f"{dtype}_{dim}_{count}"
                    print(
                        f"\n[{current}/{total * len(counts)}] {dtype} dim={dim} count={count}"
                    )

                    # Start fresh server for this config
                    if not self.start_server(label):
                        print("  Failed to start server!")
                        continue

                    try:
                        self.run_benchmark(dim, dtype, count, label)
                    finally:
                        self.stop_server()
                        # Clean up data directory
                        data_root = os.path.join(self.data_dir, label)
                        subprocess.run(f"rm -rf {data_root}", shell=True)

        # Save results
        with open(self.output_file, "w") as f:
            json.dump(
                {
                    "mode": self.args.mode,
                    "timestamp": self.timestamp,
                    "platform": f"{platform.system()} {platform.machine()}",
                    "config": {
                        "dims": dims,
                        "counts": counts,
                        "dtypes": dtypes,
                        "duration": self.args.duration,
                    },
                    "results": self.results,
                },
                f,
                indent=2,
            )

        # Print summary
        self.print_summary()
        self.generate_markdown_report()

        print("\n" + "=" * 80)
        print(f"Results saved to: {self.output_file}")
        print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)

    def print_summary(self):
        if self.args.mode == "recommend":
            print("\n" + "─" * 100)
            print("RECOMMEND BENCHMARK SUMMARY (Hybrid vs ANN)")
            print("─" * 100)
            print(
                f"{'Alpha':<8} {'K':<6} {'QPS':<12} {'P50 ms':<10} {'P95 ms':<10} {'P99 ms':<10}"
            )
            print("─" * 100)

            for r in self.results:
                print(
                    f"{r['alpha']:<8} "
                    f"{r['k']:<6} "
                    f"{r['qps']:<12.1f} "
                    f"{r['p50']:<10.2f} "
                    f"{r['p95']:<10.2f} "
                    f"{r['p99']:<10.2f}"
                )
            print("─" * 100)
            return

        print("\n" + "─" * 100)
        print("BENCHMARK SUMMARY")
        print("─" * 100)
        print(
            f"{'Dim':<8} {'Dtype':<12} {'Count':<8} {'Search Type':<15} {'QPS':<10} {'P50 ms':<8} {'P95 ms':<8} {'P99 ms':<8}"
        )
        print("─" * 100)

        for r in self.results:
            for s_type, s_data in r["search"].items():
                print(
                    f"{r['dim']:<8} "
                    f"{r['dtype']:<12} "
                    f"{r['count']:<8} "
                    f"{s_type:<15} "
                    f"{s_data['qps']:<10.1f} "
                    f"{s_data['p50']:<8.3f} "
                    f"{s_data['p95']:<8.3f} "
                    f"{s_data['p99']:<8.3f}"
                )
        print("─" * 100)

    def generate_markdown_report(self):
        if self.args.mode == "recommend":
            md_file = self.output_file.replace(".json", ".md")
            with open(md_file, "w") as f:
                f.write("# Recommend Benchmark Results (Hybrid vs ANN)\n\n")
                f.write(f"**Generated**: {datetime.now().strftime('%Y-%m-%d')}\n")
                f.write(f"**Test Tool**: Longbow Unified Benchmark Script\n\n")

                f.write("## Alpha Comparison\n\n")
                f.write("| Alpha | Mode | K | QPS | P50 (ms) | P95 (ms) | P99 (ms) |\n")
                f.write("|-------|------|---|-----|----------|----------|----------|\n")

                for r in self.results:
                    mode = (
                        "Pure ANN"
                        if r["alpha"] == 1.0
                        else ("Graph" if r["alpha"] == 0.0 else "Hybrid")
                    )
                    f.write(
                        f"| {r['alpha']} | {mode} | {r['k']} | {r['qps']:.1f} | {r['p50']:.2f} | {r['p95']:.2f} | {r['p99']:.2f} |\n"
                    )

                f.write("\n## Key Insights\n\n")
                f.write(
                    "- **Alpha = 0.0**: Pure graph-based connectivity (BFS traversal)\n"
                )
                f.write("- **Alpha = 1.0**: Pure vector similarity (ANN search)\n")
                f.write("- **Alpha = 0.5**: Hybrid blend of both approaches\n")
            return

        md_file = self.output_file.replace(".json", ".md")
        mode_title = self.args.mode.upper()
        if self.args.mode == "metal":
            mode_title = "Metal GPU"

        with open(md_file, "w") as f:
            f.write(f"# Performance Validation Matrix — Apple M3 Pro {mode_title}\n\n")
            f.write(f"**Generated**: {datetime.now().strftime('%Y-%m-%d')}\n")
            f.write(f"**Platform**: {platform.system()} ({platform.machine()})\n")
            f.write(f"**Memory**: {self.args.memory // (1024**3)}GB allocated\n")
            f.write(f"**Test Tool**: Longbow Unified Benchmark Script\n")
            f.write(f"**Queries**: {self.args.queries} per test\n\n")

            f.write("## Results Table\n\n")
            f.write(
                "| DType | Dim | Count | Ingest (vec/s) | Dense QPS | Dense P50 | Hybrid QPS | Hybrid P50 | Filtered QPS | Filtered P50 | ByID QPS | ByID P50 |\n"
            )
            f.write(
                "|-------|-----|-------|----------------|-----------|-----------|------------|------------|--------------|--------------|----------|----------|\n"
            )

            for r in self.results:
                search = r["search"]
                dense = search.get("dense", {"qps": 0, "p50": 0})
                hybrid = search.get("hybrid", {"qps": 0, "p50": 0})
                filtered = search.get("filtered", {"qps": 0, "p50": 0})
                byid = search.get("byid", {"qps": 0, "p50": 0})

                f.write(
                    f"| {r['dtype']} | {r['dim']} | {r['count']:,} | {r['ingest']['vec_per_sec']:,.0f} | "
                    f"{dense['qps']:,.0f} | {dense['p50']:.3f}ms | "
                    f"{hybrid['qps']:,.0f} | {hybrid['p50']:.3f}ms | "
                    f"{filtered['qps']:,.0f} | {filtered['p50']:.3f}ms | "
                    f"{byid['qps']:,.0f} | {byid['p50']:.3f}ms |\n"
                )

            f.write("\n---\n\n")
            f.write(
                f"## {datetime.now().strftime('%Y-%m-%d')} Full Performance Benchmark Summary ({mode_title})\n\n"
            )
            f.write(
                "| Dim | Dtype | Vec/s Ingest | Search QPS | P50 ms | P90 ms | P95 ms | P99 ms |\n"
            )
            f.write(
                "|-----|-------|--------------|------------|--------|--------|--------|--------|\n"
            )

            # Use largest count for the summary table
            max_count = max(r["count"] for r in self.results) if self.results else 0
            for r in self.results:
                if r["count"] == max_count:
                    dense = r["search"].get(
                        "dense", {"qps": 0, "p50": 0, "p90": 0, "p95": 0, "p99": 0}
                    )
                    f.write(
                        f"| {r['dim']} | {r['dtype']} | {r['ingest']['vec_per_sec']:,.0f} | "
                        f"{dense['qps']:,.1f} | {dense['p50']:.3f} | {dense.get('p90', 0.0):.3f} | "
                        f"{dense['p95']:.3f} | {dense['p99']:.3f} |\n"
                    )

            f.write("\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified Longbow Benchmark Script")
    parser.add_argument(
        "--mode",
        choices=["cpu", "metal", "cuda", "recommend"],
        default="cpu",
        help="Benchmark mode: cpu, metal (macOS), cuda (Linux), recommend (hybrid vs ANN)",
    )
    parser.add_argument(
        "--dims", default="128,384,768,1536,3072", help="Comma-separated dimensions"
    )
    parser.add_argument(
        "--counts",
        default="1000,5000,10000",
        help="Comma-separated vector counts (uses first)",
    )
    parser.add_argument(
        "--dtypes", default=ALL_DTYPES, help="Comma-separated datatypes"
    )
    parser.add_argument(
        "--memory",
        type=int,
        default=10 * 1024 * 1024 * 1024,
        help="LONGBOW_MAX_MEMORY (default 10GB)",
    )
    parser.add_argument(
        "--duration", type=int, default=15, help="Duration in seconds per test"
    )
    parser.add_argument(
        "--queries", type=int, default=1000, help="Number of search queries"
    )
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Batch size for ingest"
    )
    parser.add_argument(
        "--startup-timeout", type=int, default=60, help="Server startup timeout"
    )
    parser.add_argument("--addr", default="127.0.0.1:3000", help="Server address")
    # Recommend mode specific parameters
    parser.add_argument(
        "--alpha-values",
        default="0.0,0.5,1.0",
        help="Comma-separated alpha values to test (0.0=graph, 1.0=ANN, 0.5=hybrid)",
    )
    parser.add_argument(
        "--k-values",
        default="5,10,20",
        help="Comma-separated k values (number of recommendations)",
    )
    parser.add_argument(
        "--num-seeds",
        type=int,
        default=5,
        help="Number of seed IDs to use for recommendations",
    )
    parser.add_argument(
        "--max-hops",
        type=int,
        default=2,
        help="Maximum BFS hops for graph connectivity",
    )
    parser.add_argument(
        "--decay",
        type=float,
        default=0.5,
        help="Multi-hop connectivity decay factor",
    )

    args = parser.parse_args()
    runner = BenchmarkRunner(args)
    runner.execute()
