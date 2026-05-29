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
import atexit
import json
import os
import platform
import re
import signal
import subprocess
import sys
import time
try:
    import numpy as np
    import pandas as pd
    HAS_ANALYSIS_LIBS = True
except ImportError:
    HAS_ANALYSIS_LIBS = False
from datetime import datetime

try:
    from longbow import LongbowClient

    HAS_LONGBOW_SDK = True
except ImportError:
    HAS_LONGBOW_SDK = False

# All supported data types
ALL_DTYPES = "float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant,turboquant2,turboquant4,turboquant8"

# Bytes per element for each dtype
DTYPE_BYTES = {
    "int8": 1,
    "uint8": 1,
    "int16": 2,
    "uint16": 2,
    "int32": 4,
    "uint32": 4,
    "float32": 4,
    "float16": 2,
    "complex64": 4,
    "int64": 8,
    "uint64": 8,
    "float64": 8,
    "complex128": 8,
    "turboquant": 1,
    "turboquant2": 1,
    "turboquant4": 1,
    "turboquant8": 1,
}


class ResourceExhaustedException(Exception):
    pass



def _kill_port(port):
    """Kill any process listening on the given port.

    Uses lsof on macOS, ss+fuser on Linux, and handles missing tools gracefully."""
    system = platform.system()
    if system == "Linux":
        # ss is universally available on modern Linux
        ss_res = subprocess.run(
            f"ss -tlnp 'sport = :{port}' 2>/dev/null",
            shell=True, capture_output=True, text=True, timeout=5
        )
        # Extract PIDs from ss output (format: users:(("foo",pid,fd),...))
        if ss_res.stdout:
            for match in re.finditer(r'pid=(\d+)', ss_res.stdout):
                pid = match.group(1)
                subprocess.run(f"kill -9 {pid} 2>/dev/null", shell=True, timeout=5)
        # fuser -k as backup
        subprocess.run(f"fuser -k {port}/tcp 2>/dev/null", shell=True, timeout=5)
    else:
        subprocess.run(
            f"lsof -ti:{port} 2>/dev/null | xargs -r kill -9 2>/dev/null || true",
            shell=True, timeout=5
        )


def run_command(cmd, env=None, capture_output=True, timeout=None, shell=False):
    import shlex
    import time
    try:
        if shell:
            args = cmd
        else:
            args = shlex.split(cmd)
            
        kwargs = {
            "env": env,
            "text": True,
            "shell": shell,
            "preexec_fn": os.setsid,  # Start in new session so child processes are tracked
        }
        if capture_output:
            kwargs["stdout"] = subprocess.PIPE
            kwargs["stderr"] = subprocess.PIPE
            
        process = subprocess.Popen(args, **kwargs)
        try:
            stdout, stderr = process.communicate(timeout=timeout)
            return subprocess.CompletedProcess(process.args, process.returncode, stdout, stderr)
        except subprocess.TimeoutExpired:
            print(f"  Command timed out after {timeout}s. Terminating gracefully...")
            # Send SIGTERM to the entire process group
            try:
                pgid = os.getpgid(process.pid)
                os.killpg(pgid, signal.SIGTERM)
            except (ProcessLookupError, OSError):
                process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                print("  Graceful termination failed. Killing process group...")
                try:
                    pgid = os.getpgid(process.pid)
                    os.killpg(pgid, signal.SIGKILL)
                except (ProcessLookupError, OSError):
                    process.kill()
                process.wait()
            return None
    except Exception as e:
        print(f"  Error running command: {e}")
        return None


def parse_bench_json(json_file):
    """Parse benchmark-tool JSON output to extract metrics."""
    try:
        with open(json_file) as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

    metrics = {}
    if isinstance(data, list):
        for entry in data:
            name = entry.get("name", "")
            if name == "DoPut":
                metrics["ingest_vec_per_sec"] = entry.get("throughput", 0)
            elif name == "DoGet":
                metrics["get_vec_per_sec"] = entry.get("throughput", 0)
            elif name.startswith("Search_"):
                prefix = name.replace("Search_", "").lower()
                metrics[f"{prefix}_qps"] = entry.get("throughput", 0)
                metrics[f"{prefix}_p50_ms"] = entry.get("p50_latency_ms", 0)
                metrics[f"{prefix}_p95_ms"] = entry.get("p95_latency_ms", 0)
                metrics[f"{prefix}_p99_ms"] = entry.get("p99_latency_ms", 0)
    elif isinstance(data, dict):
        metrics = data

    return metrics


class BenchmarkRunner:
    def __init__(self, args):
        self.args = args
        self.server_addr = os.environ.get("LONGBOW_ADDR", args.addr)
        self.node_id = os.environ.get("LONGBOW_NODE_ID", "bench1")
        self.data_dir = os.environ.get(
            "LONGBOW_DATA_PATH", os.path.join(os.getcwd(), "data/bench")
        )
        self.log_dir = os.environ.get("LONGBOW_PERF_LOGS", os.path.join(os.getcwd(), "data/perf_logs"))
        print(f"DEBUG: data_dir={self.data_dir}")
        print(f"DEBUG: log_dir={self.log_dir}")
        os.makedirs(self.log_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)

        self.bin_dir = os.environ.get("LONGBOW_BIN_PATH", os.path.join(os.getcwd(), "bin"))
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        label_suffix = f"_{args.label}" if args.label else ""
        self.output_file = os.path.join(
            self.log_dir, f"perf_matrix_{args.mode}{label_suffix}_{self.timestamp}.json"
        )
        self.results = []
        self.exhausted_configs = set()
        self.server_pid = None
        self.test_counter = 0

        # Register cleanup to prevent zombie longbow processes on any exit path.
        # _force_cleanup uses port-scoped lsof so it is safe for parallel benchmark runs.
        atexit.register(self._force_cleanup)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _save_checkpoint(self):
        """Save partial results checkpoint to disk."""
        if hasattr(self, 'results') and self.results and hasattr(self, 'output_file'):
            try:
                dims = [int(d) for d in self.args.dims.split(",")]
                counts = [int(c) for c in self.args.counts.split(",")]
                dtypes = self.args.dtypes.split(",")
                with open(self.output_file, "w") as f:
                    json.dump({
                        "mode": getattr(self, 'current_mode', self.args.mode),
                        "timestamp": self.timestamp,
                        "platform": f"{platform.system()} {platform.machine()}",
                        "config": {
                            "dims": dims,
                            "counts": counts,
                            "dtypes": dtypes,
                            "duration": self.args.duration,
                        },
                        "results": self.results,
                    }, f, indent=2)
                print(f"\n  [checkpoint] Partial results saved to {self.output_file}")
            except Exception as e:
                print(f"\n  [checkpoint] Failed to save partial results: {e}")

    def _signal_handler(self, signum, frame):
        """Handle SIGINT/SIGTERM by saving partial results and cleaning up."""
        print(f"\n  [cleanup] Signal {signum} received, saving checkpoint and cleaning up...")
        self._save_checkpoint()
        self._force_cleanup()
        print("  [cleanup] Done. Partial results may be available.")
        sys.exit(1)

    def _force_cleanup(self):
        """Kill the tracked server PID and any stray processes on our ports.

        This method is intentionally port-scoped (not a global pkill) so it does
        not interfere with other benchmark runs on different ports."""
        # First, send SIGTERM then SIGKILL to the tracked PID if still alive
        if self.server_pid:
            try:
                os.kill(self.server_pid, signal.SIGTERM)
                # Give it a moment for graceful shutdown
                _, status = os.waitpid(self.server_pid, os.WNOHANG)
                if status is None:
                    time.sleep(2)
                    os.kill(self.server_pid, signal.SIGKILL)
                    os.waitpid(self.server_pid, 0)
            except (ProcessLookupError, ChildProcessError, OSError):
                pass
            # Also reap any zombie children
            try:
                while True:
                    wpid, _ = os.waitpid(-1, os.WNOHANG)
                    if wpid <= 0:
                        break
            except (ChildProcessError, OSError):
                pass
            self.server_pid = None

        # Then sweep the specific ports this runner owns
        if ":" in self.server_addr:
            try:
                port = int(self.server_addr.split(":")[-1])
                # Sweep: data port, meta port (+1), metrics port (+6000),
                # admin port (+7000), and HTTP port (+80)
                ports_to_kill = [port, port + 1, port + 6000, port + 7000, port + 80]
                for p in ports_to_kill:
                    _kill_port(p)
            except Exception:
                pass

    def get_server_binary(self):
        mode_binaries = {
            "cpu": "longbow",
            "metal": "longbow-metal",
            "cuda": "longbow-cuda",
        }
        current_mode = getattr(self, "current_mode", self.args.mode)
        bin_name = mode_binaries.get(current_mode, "longbow")
        path = os.path.join(self.bin_dir, bin_name)

        # Fall back to CPU if GPU binary not found
        if not os.path.exists(path) and current_mode in ["metal", "cuda"]:
            print(f"  {current_mode.upper()} binary not found, using CPU")
            path = os.path.join(self.bin_dir, "longbow")

        return path

    def get_bench_tool(self):
        """Get benchmark tool. Prefers bench-tool which supports vec benchmark mode."""
        # First try bench-tool (has full vec benchmark mode)
        for name in ["bench-tool", "benchmark-tool"]:
            path = os.path.join(self.bin_dir, name)
            if os.path.exists(path):
                return path
        # Return bench-tool path even if doesn't exist yet (will be built)
        return os.path.join(self.bin_dir, "bench-tool")

    def get_cli_tool(self):
        """Get the longbow-cli binary path"""
        path = os.path.join(self.bin_dir, "longbow-cli")
        if os.path.exists(path):
            return path
        # Try parent bin dir
        path = os.path.join(self.bin_dir, "..", "bin", "longbow-cli")
        if os.path.exists(path):
            return path
        return os.path.join(self.bin_dir, "longbow-cli")
    
    def get_sdk_client(self):
        """Get the Python SDK LongbowClient for benchmarking"""
        if not HAS_LONGBOW_SDK:
            print("  Warning: longbow Python SDK not installed")
            return None
        try:
            from longbow import LongbowClient
            return LongbowClient(self.server_addr)
        except Exception as e:
            print(f"  Warning: Failed to create SDK client: {e}")
            return None

    def check_cuda(self):
        current_mode = getattr(self, "current_mode", self.args.mode)
        if current_mode == "cuda" and platform.system() != "Linux":
            print("  CUDA mode only supported on Linux, using CPU")
            return False
        if current_mode == "cuda":
            result = run_command(
                "nvidia-smi --query-gpu=name --format=csv,noheader",
                shell=True
            )
            if result and result.returncode == 0 and result.stdout.strip():
                print(f"  CUDA GPU: {result.stdout.strip()}")
                return True
            print("  WARNING: No CUDA GPU detected")
        return True

    def start_server(self, label, env_overrides=None):
        """Start a fresh Longbow server for a specific configuration."""
        self.stop_server()
        
        # Calculate dynamic port to avoid TIME_WAIT issues; never reuses a port
        base_port = self.args.port + self.test_counter * 10
        self.server_addr = f"127.0.0.1:{base_port}"
        port = base_port
        self.test_counter += 1

        print(f"  Cleaning up ports starting from {port}...")
        for p in [port, port + 1, port + 80, port + 6000]:
            _kill_port(p)
        
        # Wait for ports to be actually free
        import socket
        for p in [port, port + 1, port + 80, port + 6000]:
            for _ in range(30):
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    try:
                        s.bind(('127.0.0.1', p))
                        break # Success, we can bind!
                    except socket.error:
                        pass # Still in use
                time.sleep(1.0)
        
        # Also kill any lingering longbow processes by name to be sure
        for name in ["longbow", "longbow-metal", "longbow-cuda", "bench-tool", "benchmark-tool", "longbow-cli"]:
            subprocess.run(f"pkill -9 -x {name} 2>/dev/null || true", shell=True)
        
        time.sleep(1) 
        
        server_bin = self.get_server_binary()
        if not os.path.exists(server_bin):
            print(f"  Error: {server_bin} not found!")
            return False

        data_root = os.path.join(self.data_dir, label)
        subprocess.run(f"rm -rf {data_root}", shell=True)
        os.makedirs(data_root, exist_ok=True)

        env = os.environ.copy()
        if env_overrides:
            env.update(env_overrides)

        # ── Core resource limits ──────────────────────────────────────────
        limit_gb = os.environ.get("LONGBOW_MAX_MEMORY", 18 * 1024 * 1024 * 1024)
        env["LONGBOW_MAX_MEMORY"] = str(limit_gb)
        env["ARROW_DISABLE_LOCKING"] = "1"
        env["LONGBOW_GOGC"] = "200"
        env["LONGBOW_INGESTION_WORKER_COUNT"] = "6"
        env["LONGBOW_SNAPSHOT_INTERVAL"] = "24h"
        env["LONGBOW_AUTOSCALE_ENABLED"] = "false"
        env["LONGBOW_ADAPTIVE_M_MAX_FACTOR"] = "1.5"
        env["LONGBOW_MAX_M0"] = "32"

        # ── Network addresses ─────────────────────────────────────────────
        env["LONGBOW_LISTEN_ADDR"] = f"0.0.0.0:{port}"
        env["LONGBOW_META_ADDR"] = f"0.0.0.0:{port + 1}"
        env["LONGBOW_REST_ADDR"] = f"0.0.0.0:{port + 80}"
        env["LONGBOW_METRICS_ADDR"] = f"0.0.0.0:{port + 6000}"
        env["LONGBOW_DATA_PATH"] = data_root
        env["LONGBOW_NODE_ID"] = self.node_id

        # ── GPU mode ──────────────────────────────────────────────────────
        current_mode = getattr(self, "current_mode", self.args.mode)
        if current_mode in ("metal", "cuda"):
            env["LONGBOW_GPU_ENABLED"] = "true"
        else:
            env["LONGBOW_GPU_ENABLED"] = "false"

        # ── Feature flags (always enabled for comprehensive benchmarking) ─
        env["LONGBOW_TEMPORAL_ENABLED"] = "true"
        env["LONGBOW_TEMPORAL_AGGREGATION_ENABLED"] = "true"
        try:
            parts = label.split("_")
            if len(parts) >= 4:
                dim = parts[-2]
                env["LONGBOW_TEMPORAL_DIM"] = str(dim)
        except Exception:
            pass
        env["LONGBOW_SPARSE_ENABLED"] = "true"
        env["LONGBOW_GEOSPATIAL_ENABLED"] = "true"
        env["LONGBOW_GEO_SEARCH_ENABLED"] = "true"
        env["LONGBOW_GRAPHRAG_ENABLED"] = "true"
        env["LONGBOW_LEARNED_INDEX_ENABLED"] = "true"
        env["LONGBOW_HYBRID_SEARCH_ENABLED"] = "true"
        env["LONGBOW_HNSW_TURBOQUANT_ENABLED"] = "true"
        env["LONGBOW_RERANKER_ENABLED"] = "true"
        env["LONGBOW_INDEXING_ADAPTIVE_ENABLED"] = "true"

        # ── Optional feature flags (CLI-driven) ───────────────────────────
        if self.args.rdma:
            env["LONGBOW_RDMA_ENABLED"] = "true"
        if self.args.iouring:
            env["LONGBOW_STORAGE_USE_IOURING"] = "true"
        if self.args.low_mem:
            env["LONGBOW_LOW_MEM"] = "1"
        if self.args.use_disk:
            env["LONGBOW_USE_DISK"] = "1"
        if self.args.pq_ingest:
            env["LONGBOW_PQ_INGEST"] = "1"
        if self.args.debug:
            env["LONGBOW_DEBUG"] = "true"
        if getattr(self.args, "learned_samples", 0) > 0:
            env["LONGBOW_LEARNED_MIN_SAMPLES"] = str(self.args.learned_samples)
        if getattr(self.args, "learned_confidence", 0.0) > 0:
            env["LONGBOW_LEARNED_CONFIDENCE_THRESHOLD"] = str(self.args.learned_confidence)
        if getattr(self.args, "learned_interval", 0) > 0:
            env["LONGBOW_LEARNED_UPDATE_INTERVAL"] = str(self.args.learned_interval)

        # ── Scale gRPC message size for large workloads ───────────────────
        max_count = max(int(c) for c in self.args.counts.split(","))
        if max_count >= 100000:
            env["LONGBOW_GRPC_MAX_RECV_MSG_SIZE"] = "2147483647"
            env["LONGBOW_GRPC_MAX_SEND_MSG_SIZE"] = "2147483647"
            print(f"  Scaling gRPC message size for {max_count} vectors")

        # ── Autoshard threshold: test sharded migrations ───────────────────
        shard_threshold = 10000
        env["AUTO_SHARDING_THRESHOLD"] = str(shard_threshold)
        env["AUTO_SHARDING_ENABLED"] = "true"
        env["RING_SHARDING_ENABLED"] = "true"

        log_file = os.path.join(self.log_dir, f"longbow_{current_mode}_{label}.log")
        cmd = [server_bin]
        if getattr(self.args, "numa_bind", False) and platform.system() == "Linux":
            cmd = ["numactl", "--cpunodebind=0", "--membind=0", server_bin]
            env["LONGBOW_NUMA_NODE"] = "0"

        with open(log_file, "w") as f:
            process = subprocess.Popen(
                cmd,
                env=env,
                stdout=f,
                stderr=subprocess.STDOUT,
            )
            self.server_pid = process.pid

        # Wait for server to be ready with robust gRPC /ready polling.
        # Records the handshake duration and surfaces transient port-collision retries.
        startup_start = time.time()
        connection_refused_retries = 0
        for i in range(self.args.startup_timeout):
            # Check if process is still running
            if process.poll() is not None:
                print(f"  Server exited with code {process.returncode}")
                self.server_pid = None
                return False

            # Check if port is listening and server is READY via gRPC/HTTP health check
            # Metrics port is configured as port + 6000 in start_server
            metrics_port = port + 6000
            ready_url = f"http://127.0.0.1:{metrics_port}/ready"

            # 1. First check if port is at least listening (cross-platform)
            port_listening = False
            if platform.system() == "Linux":
                ss_res = subprocess.run(
                    f"ss -tlnp 'sport = :{port}' 2>/dev/null",
                    shell=True, capture_output=True, text=True, timeout=5
                )
                port_listening = bool(ss_res.stdout.strip())
            else:
                lsof_res = subprocess.run(
                    f"lsof -i :{port} 2>/dev/null | grep LISTEN",
                    shell=True, capture_output=True, text=True, timeout=5
                )
                port_listening = lsof_res.returncode == 0
            if port_listening:
                # 2. Then check the /ready endpoint
                try:
                    # Use curl for cross-platform compatibility without extra python deps
                    ready_res = subprocess.run(
                        ["curl", "-s", "-f", ready_url],
                        capture_output=True,
                        text=True,
                        timeout=1
                    )
                    if ready_res.returncode == 0 and "OK" in ready_res.stdout:
                        handshake_duration = time.time() - startup_start
                        # Additional wait for indexing workers to settle
                        time.sleep(2)
                        # Log readiness handshake duration in benchmark summaries
                        if connection_refused_retries > 0:
                            print(f"  [readiness] server ready after {handshake_duration:.2f}s "
                                  f"({connection_refused_retries} transient port-collision retries)")
                        else:
                            print(f"  [readiness] server ready in {handshake_duration:.2f}s")
                        return True
                    elif ready_res.returncode != 0:
                        # Transient connection-refused race – count for summary
                        connection_refused_retries += 1
                except Exception:
                    # curl timeout or other transient error – count and retry
                    connection_refused_retries += 1

            time.sleep(1)

        elapsed = time.time() - startup_start
        print(f"  WARNING: Server startup timeout after {elapsed:.1f}s on port {port} "
              f"({connection_refused_retries} transient retries recorded)")
        
        # Prevent leaking the process if it times out
        if self.server_pid:
            try:
                os.kill(self.server_pid, signal.SIGKILL)
                time.sleep(2)
            except Exception:
                pass
            self.server_pid = None
            
        return False

    def stop_server(self):
        if hasattr(self, "args") and self.args and getattr(self.args, "pprof", False):
            print("  Waiting 2 seconds for active pprof collections to complete...")
            time.sleep(2)
        if self.server_pid:
            try:
                os.kill(self.server_pid, signal.SIGTERM)
                # Wait up to 15 seconds for graceful stop (server's own timeout)
                for _ in range(30):
                    time.sleep(0.5)
                    try:
                        pid_reaped, status = os.waitpid(self.server_pid, os.WNOHANG)
                        if pid_reaped == self.server_pid:
                            self.server_pid = None
                            print("  Waiting 5 seconds for port cooling...")
                            time.sleep(0.1)
                            return
                        os.kill(self.server_pid, 0)
                    except (ProcessLookupError, ChildProcessError):
                        self.server_pid = None
                        print("  Waiting 5 seconds for port cooling...")
                        time.sleep(0.1)
                        return
                
                # Fallback to kill -9
                print(f"  Server PID {self.server_pid} didn't stop gracefully, killing -9")
                os.kill(self.server_pid, signal.SIGKILL)
                time.sleep(2)
            except (ProcessLookupError, ChildProcessError):
                pass
            self.server_pid = None
            print("  Waiting 5 seconds for port cooling...")
            time.sleep(0.1)

    def collect_pprof(self, label):
        """Collect pprof profiles from the running server concurrently."""
        import threading
        
        def _do_collect():
            profiles = ["profile", "heap", "allocs", "goroutine", "threadcreate", "block", "mutex"]
            os.makedirs("profiles", exist_ok=True)
            
            host, port = self.server_addr.split(":")
            # Metrics server is typically on base_port + 6000.
            metrics_port = int(self.server_addr.split(":")[-1]) + 6000
            
            # Wait briefly for metrics server to be ready and load to start
            for retry in range(3):
                try:
                    import socket
                    with socket.create_connection((host, metrics_port), timeout=2):
                        time.sleep(2) # Give the benchmark some time to ramp up load
                        break
                except (socket.timeout, ConnectionRefusedError, OSError):
                    if retry < 2:
                        print(f"  Waiting for metrics server on {host}:{metrics_port} (attempt {retry+1}/3)...")
                        time.sleep(2)
                    else:
                        print(f"  Metrics server not reachable on {host}:{metrics_port}, skipping pprof")
                        return
            
            for profile in profiles:
                url = f"http://{host}:{metrics_port}/debug/pprof/{profile}"
                if profile == "profile":
                    url += "?seconds=5"
                
                output_file = os.path.join("profiles", f"{label}_{profile}_{self.timestamp}.pprof")
                try:
                    res = subprocess.run(f"curl -s -o {output_file} \"{url}\"", shell=True, capture_output=True, text=True, timeout=15)
                    if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                        print(f"  Collected {profile} profile")
                    else:
                        print(f"  Failed to collect {profile} profile: {res.stderr}")
                except Exception as e:
                    print(f"  Error collecting {profile} profile: {e}")

        t = threading.Thread(target=_do_collect)
        t.start()
        return t

    def save_pprof_snapshot(self, label):
        """Fetch and snapshot all pprof profiles synchronously upon test completion before shutdown."""
        if not hasattr(self, "args") or not self.args or not getattr(self.args, "pprof", False):
            return
            
        print(f"  Fetching final pprof snapshot for {label} before shutdown...")
        profiles = ["profile", "heap", "allocs", "goroutine", "threadcreate", "block", "mutex"]
        os.makedirs("profiles", exist_ok=True)
        
        host, port = self.server_addr.split(":")
        metrics_port = int(self.server_addr.split(":")[-1]) + 6000
        
        for profile in profiles:
            url = f"http://{host}:{metrics_port}/debug/pprof/{profile}"
            if profile == "profile":
                url += "?seconds=2"
            
            output_file = os.path.join("profiles", f"{label}_{profile}_{self.timestamp}_final.pprof")
            try:
                res = subprocess.run(f"curl -s -o {output_file} \"{url}\"", shell=True, capture_output=True, text=True, timeout=10)
                if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                    print(f"    Saved final {profile} profile to {output_file}")
                else:
                    print(f"    Failed to fetch final {profile} profile: {res.stderr}")
            except Exception as e:
                print(f"    Error fetching final {profile} profile: {e}")

    def run_benchmark_cli(self, dim, dtype, count, label):
        """Run benchmark using longbow-cli for comparison"""
        cli_tool = self.get_cli_tool()
        batch_size = min(count, self.args.batch_size)
        json_file = os.path.join(self.log_dir, f"result_cli_{label}.json")
        
        print(f"  CLI testing {dtype} dim={dim}...", end="", flush=True)
        
        # Create namespace
        ns_cmd = f"{cli_tool} create-namespace -name {label} -dims {dim} -data_type {dtype}"
        result = run_command(ns_cmd, timeout=30)
        
        # Import test data using CLI
        # Note: CLI doesn't support direct data generation, this is demonstration
        # Full implementation would generate test vectors
        
        print(f" CLI done")
        return True
    
    def run_benchmark_sdk(self, dim, dtype, count, label):
        """Run benchmark using Python SDK for accuracy comparison"""
        if not HAS_ANALYSIS_LIBS:
            print("  Skipping SDK test: numpy/pandas not installed")
            return False
        client = self.get_sdk_client()
        if not client:
            return False
            
        print(f"  SDK testing {dtype} dim={dim}...", end="", flush=True)
        
        try:
            # Create namespace via SDK
            ns = client.create_namespace(label, dims=dim, data_type=dtype)
            
            # Generate test vectors
            import numpy as np
            vectors = np.random.rand(count, dim).astype(np.float32)
            
            # Normalize
            norms = np.linalg.norm(vectors, axis=1, keepdims=True)
            vectors = vectors / norms
            
            # Insert via SDK
            client.insert(label, vectors)
            
            # Search via SDK
            for _ in range(min(100, self.args.queries)):
                query = vectors[0]
                results = client.search(label, query, k=10)
            
            print(f" SDK done")
            return True
        except Exception as e:
            print(f" SDK error: {e}")
            return False

        # Surgical cleanup using port discovery for the specific port
        if ":" in self.server_addr:
            try:
                port = int(self.server_addr.split(":")[-1])
                for p in [port, port + 1, port + 6000, port + 7000, port + 80]:
                    _kill_port(p)
            except:
                pass
        
        # Avoid global pkill if possible, it breaks parallel runs
        # subprocess.run("pkill -9 longbow || true", shell=True, stderr=subprocess.DEVNULL)
        time.sleep(1)

    def run_benchmark(self, dim, dtype, count, label):
        """Run benchmark-tool with JSON output for a configuration."""
        bench_tool = self.get_bench_tool()
        batch_size = count
        duration = self.args.duration
        json_file = os.path.join(self.log_dir, f"result_{label}.json")

        # Handle TurboQuant bit-packs
        is_turboquant = False
        tq_bits = 4
        if dtype == "turboquant2":
            dtype = "turboquant"
            tq_bits = 2
            is_turboquant = True
        elif dtype == "turboquant4":
            dtype = "turboquant"
            tq_bits = 4
            is_turboquant = True
        elif dtype == "turboquant8":
            dtype = "turboquant"
            tq_bits = 8
            is_turboquant = True

        # Run benchmark-tool (does ingest + search + all modes)
        uri = self.server_addr
        if not uri.startswith("grpc://"):
            uri = f"grpc://{self.server_addr}"

        # Build search-modes string based on mode
        search_modes = self.args.search_modes
        current_mode = getattr(self, "current_mode", self.args.mode)
        if search_modes == "all":
            if current_mode == "temporal":
                search_modes = "temporal_as_of,temporal_range,temporal_window"
            else:
                search_modes = "dense,hybrid,sparse,filtered,byid"

        extra_args = ""
        if self.args.fbin:
            extra_args += f" -fbin {self.args.fbin}"
        if self.args.arrow:
            extra_args += f" -fbin {self.args.arrow}"

        if self.args.generate_only:
            os.makedirs(self.args.output_dir, exist_ok=True)
            output_path = os.path.join(self.args.output_dir, f"{label}.fbin")
            extra_args += f" -output-fbin {output_path}"
            print(f"  Generating {dtype} dim={dim} count={batch_size} -> {output_path}")

        tq_arg = f" -tq-bits {tq_bits}" if is_turboquant else ""
        cmd = f"{bench_tool} -mode vec -uri {uri} -dim {dim} -dtype {dtype}{tq_arg} -scale {batch_size} -queries {self.args.queries} -workers {self.args.workers} -dataset {label} -json {json_file} -search-modes {search_modes}{extra_args}"
        print(f"DEBUG: cmd={cmd}", flush=True)
        print(f"  Running {dtype} dim={dim}...", end="", flush=True)
        base_timeout = getattr(self.args, "timeout", 1800)
        dim_factor = max(1.0, dim / 128.0)

        # Use actual measured speed if available, otherwise conservative estimate
        measured_speed = getattr(self, f'_speed_{dtype}', None)
        if measured_speed and measured_speed > 0:
            base_speed = measured_speed
        else:
            if dtype in ("float64", "complex64", "complex128"):
                base_speed = 30.0
            elif dtype.startswith("turboquant"):
                base_speed = 50.0
            elif dtype in ("int8", "uint8", "int16", "uint16"):
                base_speed = 80.0
            elif dtype in ("float32",):
                base_speed = 60.0
            else:
                base_speed = 40.0

        scaled_timeout = int((batch_size / base_speed) * dim_factor)
        search_overhead = int(self.args.queries / 100.0 * dim_factor)
        scaled_timeout += max(search_overhead, 60)

        # Scaled timeout is primary; clamp between base_timeout and 4x base_timeout
        timeout = max(base_timeout, scaled_timeout)
        timeout = min(timeout, base_timeout * 4)
        
        bench_log = os.path.join(self.log_dir, f"bench_{label}.log")
        with open(bench_log, "w") as f:
            result = run_command(cmd, timeout=timeout)
            if result:
                f.write(result.stdout)
                f.write(result.stderr)
                
                # Check for ResourceExhausted error
                output_to_check = (result.stdout or "") + (result.stderr or "")
                if "ResourceExhausted" in output_to_check or "code = 8" in output_to_check or "code 8" in output_to_check or "migration throttled" in output_to_check:
                    print(" EXHAUSTED")
                    raise ResourceExhaustedException(f"ResourceExhausted detected in {label}")

        # Parse JSON first — bench-tool logs to stderr (Go log package) which
        # can cause non-zero exit codes even on successful runs.
        print(f"DEBUG: json_file={json_file}")
        metrics = parse_bench_json(json_file)
        if not metrics:
            print(" FAILED")
            if result and result.stderr:
                print(f"    Error: {result.stderr.strip()}")
            return False

        # Extract all search types
        search_metrics = {}
        # Only validate modes relevant to the current benchmark mode
        current_mode = getattr(self, "current_mode", self.args.mode)
        if current_mode == "temporal":
            expected_modes = ["temporal_as_of", "temporal_range", "temporal_window"]
        else:
            expected_modes = ["dense", "hybrid", "sparse", "filtered", "byid"]
        for key, value in metrics.items():
            if "_qps" in key:
                prefix = key.replace("_qps", "")
                search_metrics[prefix] = {
                    "qps": value,
                    "p50": metrics.get(f"{prefix}_p50_ms", 0),
                    "p95": metrics.get(f"{prefix}_p95_ms", 0),
                    "p99": metrics.get(f"{prefix}_p99_ms", 0),
                }

        # Validate mode field - warn if any mode returns 0 QPS
        mode_failures = []
        for mode in expected_modes:
            if mode in search_metrics and search_metrics[mode]["qps"] == 0:
                mode_failures.append(mode)
        if mode_failures:
            print(f"    WARNING: Mode field validation failed for: {mode_failures}")

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

        # Adaptive timeout re-calibration: track actual throughput and adjust
        # base speed estimate for the next run of the same dtype
        vec_per_sec = metrics.get("ingest_vec_per_sec", 0)
        if vec_per_sec > 0 and not hasattr(self, 'actual_speed'):
            self.actual_speed = {}
        if vec_per_sec > 0:
            self.actual_speed[dtype] = vec_per_sec
            # Persist for dynamic re-calibration
            setattr(self, f'_speed_{dtype}', vec_per_sec)

        print(f" {vec_per_sec:.0f} vec/s")
        return True

    def execute_recommend(self):
        if not HAS_LONGBOW_SDK or not HAS_ANALYSIS_LIBS:
            print(
                "Error: longbow SDK or numpy/pandas not installed."
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
                meta_uri=f"grpc://127.0.0.1:{int(self.server_addr.split(':')[-1]) + 1}",
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
            self._force_cleanup()  # Kill stray processes on our ports before graceful stop
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

    def execute_deletion(self):
        """Test deletion and tombstone operations."""
        if not HAS_LONGBOW_SDK:
            print(
                "Error: longbow Python SDK not installed. Install with: pip install longbow"
            )
            return

        dims = [int(d) for d in self.args.dims.split(",")]
        counts = [int(c) for c in self.args.counts.split(",")]
        delete_counts = [int(d) for d in self.args.delete_counts.split(",")]

        count = counts[0] if counts else 10000
        dim = dims[0] if dims else 128

        print("=" * 80)
        print(f"DELETION BENCHMARK (Tombstone Operations)")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Dim: {dim}, Total Count: {count}")
        print(f"Delete counts: {delete_counts}")
        print("=" * 80)

        label = f"del_{dim}_{count}"
        if not self.start_server(label):
            print("  Failed to start server!")
            return

        try:
            client = LongbowClient(
                uri=f"grpc://{self.server_addr}",
                meta_uri=f"grpc://127.0.0.1:{int(self.server_addr.split(':')[-1]) + 1}",
            )

            dataset_name = f"del_bench_{dim}d"
            print(f"\nCreating dataset {dataset_name} with {count} vectors...")

            vectors = np.random.rand(count, dim).astype(np.float32).tolist()
            ids = [str(i) for i in range(count)]

            client.insert(
                dataset_name,
                [{"id": id, "vector": vec} for id, vec in zip(ids, vectors)],
            )
            time.sleep(3)  # Wait for indexing

            # Test different delete counts
            for del_count in delete_counts:
                del_ids = [str(i) for i in range(del_count)]
                print(f"\nDeleting {del_count} vectors...")

                start = time.time()
                try:
                    client.delete(dataset_name, del_ids)
                    del_time = (time.time() - start) * 1000
                except Exception as e:
                    print(f"  Delete error: {e}")
                    continue

                # Verify search still works after deletion
                query_vec = np.random.rand(dim).astype(np.float32).tolist()
                start = time.time()
                try:
                    results = client.search(dataset_name, vector=query_vec, k=10)
                    search_time = (time.time() - start) * 1000
                except Exception as e:
                    print(f"  Search after delete error: {e}")
                    continue

                self.results.append(
                    {
                        "dim": dim,
                        "count": count,
                        "deleted": del_count,
                        "delete_time_ms": del_time,
                        "search_time_ms": search_time,
                        "timestamp": datetime.now().isoformat(),
                    }
                )
                print(f"  Delete: {del_time:.2f}ms, Search: {search_time:.2f}ms")

        except Exception as e:
            print(f"Error: {e}")
        finally:
            self._force_cleanup()  # Kill stray processes on our ports before graceful stop
            self.stop_server()
            data_root = os.path.join(self.data_dir, label)
            subprocess.run(f"rm -rf {data_root}", shell=True)

        with open(self.output_file, "w") as f:
            json.dump(
                {
                    "mode": "deletion",
                    "timestamp": self.timestamp,
                    "config": {
                        "dim": dim,
                        "count": count,
                        "delete_counts": delete_counts,
                    },
                    "results": self.results,
                },
                f,
                indent=2,
            )

        self.print_summary()
        print(f"\nResults saved to: {self.output_file}")

    def execute_graphrag(self):
        """Test GraphRAG graph spreading activation operations."""
        if not HAS_LONGBOW_SDK or not HAS_ANALYSIS_LIBS:
            print(
                "Error: longbow SDK or numpy/pandas not installed."
            )
            return

        dims = [int(d) for d in self.args.dims.split(",")]
        counts = [int(c) for c in self.args.counts.split(",")]
        dtypes = self.args.dtypes.split(",")
        
        dtype_map = {
            "float32": np.float32, "float64": np.float64, "float16": np.float16,
            "int8": np.int8, "int16": np.int16, "int32": np.int32, "int64": np.int64,
            "uint8": np.uint8, "uint16": np.uint16, "uint32": np.uint32, "uint64": np.uint64,
            "complex64": np.complex64, "complex128": np.complex128, "turboquant": np.float32,
        }

        all_results = []
        alpha_values = [float(a) for a in self.args.graph_alpha_values.split(",")]
        k_val = int(self.args.k_values.split(",")[0])

        for count in counts:
            for dim in dims:
                for dtype in dtypes:
                    print(f"\n{'=' * 80}")
                    print(f"GRAPHRAG Test: {dtype} dim={dim} count={count}")
                    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    print("=" * 80)
                    
                    label = f"gr_{dtype}_{dim}_{count}"
                    if not self.start_server(label):
                        print(f"  Failed to start server for {label}!")
                        continue

                    try:
                        client = LongbowClient(
                            uri=f"grpc://{self.server_addr}",
                            meta_uri=f"grpc://127.0.0.1:{int(self.server_addr.split(':')[-1]) + 1}",
                        )
                        client.connect()

                        pprof_proc = None
                        dataset_name = f"grag_bench_{dtype}_{dim}_{count}"
                        print(f"  Creating dataset {dataset_name}...")

                        # Start background pprof collection
                        label_full = f"{label}_{self.args.label}" if self.args.label else label
                        pprof_file = os.path.join(self.log_dir, f"profile_{label_full}.pprof")
                        metrics_port = int(self.server_addr.split(":")[-1]) + 6000
                        pprof_url = f"http://127.0.0.1:{metrics_port}/debug/pprof/profile?seconds=1"
                        pprof_proc = subprocess.Popen(
                            f"curl -s -o {pprof_file} \"{pprof_url}\"",
                            shell=True,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL
                        )

                        np_dtype = dtype_map.get(dtype, np.float32)
                        
                        # Fix: Batch insertion to avoid massive memory usage in Python
                        batch_size = 5000
                        for i in range(0, count, batch_size):
                            end = min(i + batch_size, count)
                            batch_count = end - i
                            
                            if "complex" in dtype:
                                vectors_batch = (np.random.randn(batch_count, dim) + 1j * np.random.randn(batch_count, dim)).astype(np_dtype)
                            elif "int" in dtype or "uint" in dtype:
                                vectors_batch = np.random.randint(0, 100, size=(batch_count, dim)).astype(np_dtype)
                            else:
                                vectors_batch = np.random.randn(batch_count, dim).astype(np_dtype)
                                
                            batch_ids = [str(j) for j in range(i, end)]
                            
                            # Note: vec.tolist() still converts to float64, but we only do it for one batch at a time
                            client.insert(
                                dataset_name,
                                [{"id": id, "vector": vec.tolist()} for id, vec in zip(batch_ids, vectors_batch)],
                            )
                            
                        time.sleep(3)  # Wait for indexing + graph build

                        for alpha in alpha_values:
                            print(f"    GraphRAG alpha={alpha}, k={k_val}...", end="", flush=True)
                            
                            if "complex" in dtype:
                                query_vec = (np.random.randn(dim) + 1j * np.random.randn(dim)).astype(np_dtype).tolist()
                            elif "int" in dtype or "uint" in dtype:
                                query_vec = np.random.randint(0, 100, size=dim).astype(np_dtype).tolist()
                            else:
                                query_vec = np.random.randn(dim).astype(np_dtype).tolist()

                            latencies = []
                            for _ in range(self.args.queries):
                                start = time.time()
                                try:
                                    _ = client.search(
                                        dataset_name,
                                        vector=query_vec,
                                        k=k_val,
                                        graph_alpha=alpha,
                                    )
                                    latency = (time.time() - start) * 1000
                                    latencies.append(latency)
                                except Exception as e:
                                    continue

                            if latencies:
                                latencies.sort()
                                qps = 1000.0 / (sum(latencies) / len(latencies))
                                result_entry = {
                                    "dim": dim,
                                    "dtype": dtype,
                                    "count": count,
                                    "alpha": alpha,
                                    "k": k_val,
                                    "qps": qps,
                                    "p50": latencies[int(0.5 * len(latencies))],
                                    "p95": latencies[int(0.95 * len(latencies))],
                                    "p99": latencies[int(0.99 * len(latencies))],
                                    "timestamp": datetime.now().isoformat(),
                                }
                                all_results.append(result_entry)
                                print(f" QPS: {qps:.1f}, P50: {latencies[int(0.5 * len(latencies))]:.2f}ms")
                            else:
                                print(" FAILED")

                    except Exception as e:
                        print(f"  Error: {e}")
                    finally:
                        if pprof_proc:
                            pprof_proc.wait()
                        self.stop_server()
                        data_root = os.path.join(self.data_dir, label)
                        subprocess.run(f"rm -rf {data_root}", shell=True)

        with open(self.output_file, "w") as f:
            json.dump(
                {
                    "mode": "graphrag",
                    "timestamp": self.timestamp,
                    "config": {
                        "dims": dims,
                        "counts": counts,
                        "dtypes": dtypes,
                        "alpha_values": alpha_values,
                        "k": k_val,
                    },
                    "results": all_results,
                },
                f,
                indent=2,
            )

        self.print_summary()
        print(f"\nResults saved to: {self.output_file}")

    def execute_exchange(self):
        """Test DoExchange mesh replication operations."""
        if not HAS_LONGBOW_SDK:
            print(
                "Error: longbow Python SDK not installed. Install with: pip install longbow"
            )
            return

        dims = [int(d) for d in self.args.dims.split(",")]
        counts = [int(c) for c in self.args.counts.split(",")]

        count = counts[0] if counts else 10000
        dim = dims[0] if dims else 128

        print("=" * 80)
        print(f"DOEXCHANGE BENCHMARK (Mesh Replication)")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Dim: {dim}, Count: {count}")
        print("=" * 80)

        label = f"ex_{dim}_{count}"
        if not self.start_server(label):
            print("  Failed to start server!")
            return

        try:
            client = LongbowClient(
                uri=f"grpc://{self.server_addr}",
                meta_uri=f"grpc://127.0.0.1:{int(self.server_addr.split(':')[-1]) + 1}",
            )

            # Create source dataset
            source_ds = f"source_{dim}d"
            print(f"\nCreating source dataset {source_ds}...")

            vectors = np.random.rand(count, dim).astype(np.float32).tolist()
            ids = [str(i) for i in range(count)]

            client.insert(
                source_ds, [{"id": id, "vector": vec} for id, vec in zip(ids, vectors)]
            )
            time.sleep(2)

            # Test DoExchange operations
            # Note: Full mesh replication requires multi-node setup
            # Here we test the exchange protocol with self-exchange

            print(f"\nTesting DoExchange protocol...")

            # Test vector search via exchange protocol
            query_vec = np.random.rand(dim).astype(np.float32).tolist()

            latencies = []
            for _ in range(self.args.queries):
                start = time.time()
                try:
                    # Search triggers DoExchange under the hood for distributed queries
                    results = client.search(source_ds, vector=query_vec, k=10)
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
                        "count": count,
                        "operation": "exchange_search",
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
            self._force_cleanup()  # Kill stray processes on our ports before graceful stop
            self.stop_server()
            data_root = os.path.join(self.data_dir, label)
            subprocess.run(f"rm -rf {data_root}", shell=True)

        with open(self.output_file, "w") as f:
            json.dump(
                {
                    "mode": "exchange",
                    "timestamp": self.timestamp,
                    "config": {"dim": dim, "count": count},
                    "results": self.results,
                },
                f,
                indent=2,
            )

        self.print_summary()
        print(f"\nResults saved to: {self.output_file}")

    def execute_onnx(self):
        """Test ONNX reranker benchmarks via Go test binary."""
        print("=" * 80)
        print("ONNX RERANKER BENCHMARK")
        print("Started:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print("=" * 80)

        bench_bin = os.path.join(self.bin_dir, "longbow")
        if not os.path.exists(bench_bin):
            bench_bin = os.path.join(self.bin_dir, "longbow-metal")
        if not os.path.exists(bench_bin):
            print("  Error: No longbow binary found")
            return

        run_cmd = f"{bench_bin} test -bench=BenchmarkMetalReranker -benchtime={self.args.duration}x -run=^$"
        print(f"  Running: {run_cmd}")

        result = run_command(run_cmd, timeout=self.args.timeout)

        if result and result.returncode == 0:
            self.results.append(
                {
                    "mode": "onnx",
                    "output": result.stdout,
                    "timestamp": datetime.now().isoformat(),
                }
            )
            print(f"  COMPLETED")
            print(result.stdout)
        else:
            print(f"  FAILED: {result.stderr if result else 'no output'}")

        with open(self.output_file, "w") as f:
            json.dump(
                {
                    "mode": "onnx",
                    "timestamp": self.timestamp,
                    "results": self.results,
                },
                f,
                indent=2,
            )
        print(f"\nResults saved to {self.output_file}")

    def execute_temporal(self):
        """Test temporal query capabilities."""
        if not HAS_LONGBOW_SDK:
            print("ERROR: longbow SDK not installed. Install with: pip install longbow")
            return

        print("=" * 80)
        print("TEMPORAL QUERY BENCHMARK")
        print("Started:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print("=" * 80)

        dims = [int(d) for d in self.args.dims.split(",")]
        counts = [int(c) for c in self.args.counts.split(",")]
        dtypes = self.args.dtypes.split(",")
        
        dtype_map = {
            "float32": np.float32, "float64": np.float64, "float16": np.float16,
            "int8": np.int8, "int16": np.int16, "int32": np.int32, "int64": np.int64,
            "uint8": np.uint8, "uint16": np.uint16, "uint32": np.uint32, "uint64": np.uint64,
            "complex64": np.complex64, "complex128": np.complex128, "turboquant": np.float32,
        }

        pprof_proc = None
        for count in counts:
            for dim in dims:
                for dtype in dtypes:
                    print(f"\n{'=' * 80}")
                    print(f"Temporal Test: {dtype} dim={dim} count={count}")
                    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    print("=" * 80)
                    
                    label = f"temporal_{dtype}_{dim}_{count}"
                    if not self.start_server(label, env_overrides={"LONGBOW_TEMPORAL_ENABLED": "true", "LONGBOW_TEMPORAL_AGGREGATION_ENABLED": "true", "LONGBOW_TEMPORAL_DIM": str(dim)}):
                        print(f"  Failed to start server for {label}!")
                        continue

                    try:
                        print(f"  Generating {count} vectors with timestamps...")
                        now = time.time()
                        base_timestamp = int(now * 1e9)
                        np_dtype = dtype_map.get(dtype, np.float32)
                        print(f"  Inserting {count} vectors in batches...")
                        client = LongbowClient(
                            uri=f"grpc://{self.server_addr}",
                            meta_uri=f"grpc://127.0.0.1:{int(self.server_addr.split(':')[-1]) + 1}",
                        )

                        batch_size = 5000
                        for i in range(0, count, batch_size):
                            end = min(i + batch_size, count)
                            batch_count = end - i
                            
                            vectors_batch = []
                            for j in range(i, end):
                                if "complex" in dtype:
                                    vec = (np.random.randn(dim) + 1j * np.random.randn(dim)).astype(np_dtype)
                                elif "int" in dtype or "uint" in dtype:
                                    vec = np.random.randint(0, 100, size=dim).astype(np_dtype)
                                else:
                                    vec = np.random.randn(dim).astype(np_dtype)
                                    
                                vectors_batch.append(
                                    {
                                        "id": str(j),
                                        "vector": vec.tolist(),
                                        "timestamp": base_timestamp + j * 1000000000,
                                        "metadata": {"index": j},
                                    }
                                )
                            
                            df_batch = pd.DataFrame(vectors_batch)
                            client.insert(f"temporal_{dtype}_{dim}", df_batch)
                            
                        print("  Insert complete!")

                        results = []
                        search_types = ["as_of", "range", "sliding_window", "sliding_window_time"]

                        # Start background pprof collection
                        label_full = f"{label}_{self.args.label}" if self.args.label else label
                        pprof_file = os.path.join(self.log_dir, f"profile_{label_full}.pprof")
                        metrics_port = int(self.server_addr.split(":")[-1]) + 6000
                        pprof_url = f"http://127.0.0.1:{metrics_port}/debug/pprof/profile?seconds=1"
                        pprof_proc = subprocess.Popen(
                            f"curl -s -o {pprof_file} \"{pprof_url}\"",
                            shell=True,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL
                        )

                        print(f"  Testing temporal search types...")
                        for stype in search_types:
                            try:
                                if stype == "as_of":
                                    res = client.temporal_search(
                                        search_type=stype,
                                        timestamp=base_timestamp + count * 500000000,
                                        k=10,
                                    )
                                elif stype == "range":
                                    res = client.temporal_search(
                                        search_type=stype,
                                        start_time=base_timestamp,
                                        end_time=base_timestamp + count * 1000000000,
                                        k=10,
                                    )
                                elif stype == "sliding_window":
                                    res = client.temporal_search(
                                        search_type=stype, window_size=100, k=10
                                    )
                                elif stype == "sliding_window_time":
                                    res = client.temporal_search(
                                        search_type=stype, duration=3600 * 1000000000, k=10
                                    )

                                results.append(
                                    {"search_type": stype, "count": len(res) if res else 0}
                                )
                                print(f"    {stype}: {len(res) if res else 0} results")
                            except Exception as e:
                                print(f"    {stype}: ERROR - {e}")
                                results.append({"search_type": stype, "error": str(e)})

                        print(f"  Testing version history and aggregation...")
                        try:
                            history = client.temporal_version_history(vector_id=0)
                            print(f"    Version history: {len(history) if history else 0} versions")
                            results.append(
                                {"version_history_count": len(history) if history else 0}
                            )
                        except Exception as e:
                            print(f"    Version history: ERROR - {e}")

                        try:
                            agg = client.temporal_aggregation(
                                aggregation_type="count",
                                start_time=base_timestamp,
                                end_time=base_timestamp + count * 1000000000,
                                interval=360000000000,
                            )
                            print(f"    Aggregation: {agg.get('total_count', 0)} total")
                            results.append({"aggregation": agg})
                        except Exception as e:
                            print(f"    Aggregation: ERROR - {e}")


                        self.results.append({
                            "dim": dim,
                            "dtype": dtype,
                            "count": count,
                            "mode": "temporal",
                            "results": results,
                            "timestamp": datetime.now().isoformat()
                        })

                    finally:
                        if pprof_proc:
                            pprof_proc.wait()
                        self.stop_server()
                        data_root = os.path.join(self.data_dir, label)
                        subprocess.run(f"rm -rf {data_root}", shell=True)

        with open(self.output_file, "w") as f:
            json.dump(
                {"mode": "temporal", "timestamp": self.timestamp, "results": self.results},
                f,
                indent=2,
            )
        print(f"\nResults saved to: {self.output_file}")

    def execute_geo(self):
        """Test geo-spatial search capabilities (radius, box, hybrid, Quadtree)."""
        if not HAS_LONGBOW_SDK:
            print("ERROR: longbow SDK not installed. Install with: pip install longbow")
            return

        print("=" * 80)
        print("GEO-SPATIAL SEARCH BENCHMARK")
        print("Started:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print("=" * 80)

        dims = [int(d) for d in self.args.dims.split(",")]
        counts = [int(c) for c in self.args.counts.split(",")]
        dtypes = [d for d in self.args.dtypes.split(",") if "float" in d]
        if not dtypes:
            dtypes = ["float32"]

        dtype_map = {
            "float32": np.float32, "float64": np.float64, "float16": np.float16,
        }

        geo_centers = [
            {"lat": 40.7128, "lon": -74.0060},   # NYC
            {"lat": 34.0522, "lon": -118.2437}, # LA
            {"lat": 51.5074, "lon": -0.1278},    # London
            {"lat": 48.8566, "lon": 2.3522},     # Paris
            {"lat": 35.6762, "lon": 139.6503},  # Tokyo
        ]
        radius_values = [5.0, 25.0, 100.0, 500.0]

        all_results = []
        for count in counts:
            for dim in dims:
                for dtype in dtypes:
                    print(f"\n{'=' * 80}")
                    print(f"Geo Search: {dtype} dim={dim} count={count}")
                    print("=" * 80)

                    label = f"geo_{dtype}_{dim}_{count}"
                    env_overrides = {
                        "GEO_ENABLED": "true",
                        "LONGBOW_MAX_MEMORY": str(self.args.memory),
                    }
                    if not self.start_server(label, env_overrides=env_overrides):
                        print(f"  Failed to start server for {label}!")
                        continue

                    try:
                        client = LongbowClient(
                            uri=f"grpc://{self.server_addr}",
                            meta_uri=f"grpc://127.0.0.1:{int(self.server_addr.split(':')[-1]) + 1}",
                        )
                        client.connect()

                        np_dtype = dtype_map.get(dtype, np.float32)
                        print(f"  Inserting {count} geo-tagged vectors...")

                        batch_size = 5000
                        for i in range(0, count, batch_size):
                            end = min(i + batch_size, count)
                            batch_count = end - i

                            vectors_batch = []
                            for j in range(i, end):
                                vec = np.random.randn(dim).astype(np_dtype)
                                center_idx = j % len(geo_centers)
                                center = geo_centers[center_idx]
                                lat = center["lat"] + (np.random.rand() - 0.5) * 2.0
                                lon = center["lon"] + (np.random.rand() - 0.5) * 2.0
                                vectors_batch.append({
                                    "id": str(j),
                                    "vector": vec.tolist(),
                                    "geo_point": {"lat": float(lat), "lon": float(lon)},
                                })

                            client.insert(
                                f"geo_{dtype}_{dim}",
                                [{"id": r["id"], "vector": r["vector"],
                                  "geo_point": r["geo_point"]} for r in vectors_batch],
                            )

                        time.sleep(3)
                        print(f"  Indexing complete.")

                        search_types = [
                            ("radius_5km", {"radius_km": 5.0, "k": 10}),
                            ("radius_25km", {"radius_km": 25.0, "k": 10}),
                            ("radius_100km", {"radius_km": 100.0, "k": 10}),
                            ("radius_500km", {"radius_km": 500.0, "k": 10}),
                            ("box_1deg", {"geo_box": {"min_lat": 39.5, "max_lat": 41.5,
                                                      "min_lon": -75.5, "max_lon": -73.5}, "k": 10}),
                        ]

                        for geo_type, params in search_types:
                            latencies = []
                            center = geo_centers[0]
                            query_vec = np.random.randn(dim).astype(np_dtype).tolist()
                            for _ in range(self.args.queries):
                                start = time.time()
                                try:
                                    if geo_type.startswith("radius"):
                                        res = client.search(
                                            f"geo_{dtype}_{dim}",
                                            vector=query_vec,
                                            geo_center=center,
                                            geo_radius_km=params["radius_km"],
                                            k=params["k"],
                                        )
                                    else:
                                        res = client.search(
                                            f"geo_{dtype}_{dim}",
                                            vector=query_vec,
                                            geo_box=params["geo_box"],
                                            k=params["k"],
                                        )
                                    latency = (time.time() - start) * 1000
                                    latencies.append(latency)
                                except Exception:
                                    pass

                            if latencies:
                                latencies.sort()
                                avg_ms = sum(latencies) / len(latencies)
                                qps = 1000.0 / avg_ms if avg_ms > 0 else 0
                                all_results.append({
                                    "dim": dim, "dtype": dtype, "count": count,
                                    "search_type": geo_type,
                                    "qps": qps,
                                    "p50_ms": latencies[int(0.5 * len(latencies))],
                                    "p95_ms": latencies[int(0.95 * len(latencies))],
                                    "p99_ms": latencies[int(0.99 * len(latencies))],
                                    "avg_ms": avg_ms,
                                })
                                self.results = all_results
                                self.save_results()
                                print(f"    {geo_type}: QPS={qps:.1f} P50={latencies[int(0.5*len(latencies))]:.2f}ms")
                            else:
                                print(f"    {geo_type}: FAILED")

                        print("  Testing hybrid (vector + geo) search...")
                        hyb_latencies = []
                        for _ in range(min(200, self.args.queries)):
                            start = time.time()
                            try:
                                res = client.search(
                                    f"geo_{dtype}_{dim}",
                                    vector=query_vec,
                                    geo_center=center,
                                    geo_radius_km=50.0,
                                    k=10,
                                )
                                hyb_latencies.append((time.time() - start) * 1000)
                            except Exception:
                                pass
                        if hyb_latencies:
                            hyb_latencies.sort()
                            all_results.append({
                                "dim": dim, "dtype": dtype, "count": count,
                                "search_type": "hybrid_vector_geo",
                                "qps": 1000.0 / (sum(hyb_latencies) / len(hyb_latencies)),
                                "p50_ms": hyb_latencies[int(0.5 * len(hyb_latencies))],
                                "p95_ms": hyb_latencies[int(0.95 * len(hyb_latencies))],
                                "p99_ms": hyb_latencies[int(0.99 * len(hyb_latencies))],
                                "avg_ms": sum(hyb_latencies) / len(hyb_latencies),
                            })
                            print(f"    hybrid_vector_geo: QPS={1000.0/(sum(hyb_latencies)/len(hyb_latencies)):.1f}")

                    except Exception as e:
                        print(f"  Error: {e}")
                    finally:
                        self.stop_server()
                        subprocess.run(f"rm -rf {os.path.join(self.data_dir, label)}",
                                       shell=True, capture_output=True)

        with open(self.output_file, "w") as f:
            json.dump({"mode": "geo", "timestamp": self.timestamp, "results": all_results}, f, indent=2)
        print(f"\nResults saved to: {self.output_file}")

    def execute_churn(self):
        """Churn soak test: repeated add/delete cycles with varying payload sizes.

        Simulates real-world churn by cycling through adds and deletes while
        tracking memory pressure, fragmentation, and search quality.
        """
        if not HAS_LONGBOW_SDK:
            print("ERROR: longbow SDK not installed. Install with: pip install longbow")
            return

        print("=" * 80)
        print("CHURN SOAK TEST (Add/Delete Cycling)")
        print("Started:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print("=" * 80)

        dims = [int(d) for d in self.args.dims.split(",")]
        dtypes = self.args.dtypes.split(",")

        payload_sizes_kb = [int(s) for s in self.args.churn_payload_sizes.split(",")]
        if not payload_sizes_kb:
            payload_sizes_kb = [0, 1, 4, 64, 256, 1024]

        cycles = int(self.args.churn_cycles)
        chunk_size = int(self.args.churn_chunk_size)

        dtype_map = {
            "float32": np.float32, "float64": np.float64, "float16": np.float16,
            "int8": np.int8, "int16": np.int16, "int32": np.int32, "int64": np.int64,
            "uint8": np.uint8, "uint16": np.uint16, "uint32": np.uint32, "uint64": np.uint64,
            "complex64": np.complex64, "complex128": np.complex128, "turboquant": np.float32,
        }

        def make_lorem_payload(size_kb: int) -> dict:
            """Generate lorem-ipsum metadata payload of approx size_kb."""
            if size_kb <= 0:
                return {}
            words = [
                "lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing",
                "elit", "sed", "do", "eiusmod", "tempor", "incididunt", "ut", "labore",
                "et", "dolore", "magna", "aliqua", "enim", "ad", "minim", "veniam",
                "quis", "nostrud", "exercitation", "ullamco", "laboris", "nisi",
                "ut", "aliquip", "ex", "ea", "commodo", "consequat",
            ]
            target_chars = size_kb * 1024
            text = " ".join(np.random.choice(words, size=max(1, target_chars // 6)))
            while len(text) < target_chars:
                text += " " + " ".join(np.random.choice(words, size=50))
            return {"description": text[:target_chars]}

        all_results = []
        for dtype in dtypes:
            for dim in dims:
                print(f"\n{'=' * 80}")
                print(f"Churn Test: {dtype} dim={dim}")
                print(f"Payload sizes: {payload_sizes_kb} KB, {cycles} cycles x {chunk_size} vectors")
                print("=" * 80)

                label = f"churn_{dtype}_{dim}"
                env_overrides = {"LONGBOW_MAX_MEMORY": str(self.args.memory)}
                if not self.start_server(label, env_overrides=env_overrides):
                    print(f"  Failed to start server!")
                    continue

                try:
                    client = LongbowClient(
                        uri=f"grpc://{self.server_addr}",
                        meta_uri=f"grpc://127.0.0.1:{int(self.server_addr.split(':')[-1]) + 1}",
                    )
                    client.connect()

                    np_dtype = dtype_map.get(dtype, np.float32)

                    for payload_kb in payload_sizes_kb:
                        dataset = f"churn_{dtype}_{dim}_p{payload_kb}"
                        print(f"\n  Payload={payload_kb}KB")

                        base_ids = list(range(chunk_size))
                        id_counter = chunk_size

                        cycle_results = []
                        query_vec = None

                        for cycle in range(cycles):
                            added = 0
                            deleted = 0
                            cycle_start = time.time()

                            batch_add_ids = list(range(id_counter, id_counter + chunk_size))
                            id_counter += chunk_size

                            add_batch = []
                            for vec_id in batch_add_ids:
                                if "complex" in dtype:
                                    vec = (np.random.randn(dim) + 1j * np.random.randn(dim)).astype(np_dtype)
                                elif "int" in dtype or "uint" in dtype:
                                    vec = np.random.randint(0, 100, size=dim).astype(np_dtype)
                                else:
                                    vec = np.random.randn(dim).astype(np_dtype)

                                record = {
                                    "id": str(vec_id),
                                    "vector": vec.tolist(),
                                    **make_lorem_payload(payload_kb),
                                }
                                add_batch.append(record)

                            t0 = time.time()
                            client.insert(dataset, add_batch)
                            add_ms = (time.time() - t0) * 1000
                            added = chunk_size

                            delete_ids = base_ids if cycle == 0 else list(range(
                                id_counter - chunk_size * 2 if id_counter > chunk_size * 2 else 0,
                                id_counter - chunk_size,
                            ))
                            if delete_ids:
                                t0 = time.time()
                                for did in delete_ids:
                                    try:
                                        client.delete(dataset, str(did))
                                        deleted += 1
                                    except Exception:
                                        pass
                                delete_ms = (time.time() - t0) * 1000
                            else:
                                delete_ms = 0

                            if query_vec is None:
                                if "complex" in dtype:
                                    query_vec = (np.random.randn(dim) + 1j * np.random.randn(dim)).astype(np_dtype).tolist()
                                elif "int" in dtype or "uint" in dtype:
                                    query_vec = np.random.randint(0, 100, size=dim).astype(np_dtype).tolist()
                                else:
                                    query_vec = np.random.randn(dim).astype(np_dtype).tolist()

                            search_latencies = []
                            for _ in range(min(50, self.args.queries)):
                                t0 = time.time()
                                try:
                                    res = client.search(dataset, vector=query_vec, k=10)
                                    search_latencies.append((time.time() - t0) * 1000)
                                except Exception:
                                    pass

                            cycle_elapsed = (time.time() - cycle_start) * 1000
                            base_ids = batch_add_ids

                            entry = {
                                "dtype": dtype, "dim": dim, "payload_kb": payload_kb,
                                "cycle": cycle + 1, "added": added, "deleted": deleted,
                                "add_ms": add_ms, "delete_ms": delete_ms,
                                "cycle_ms": cycle_elapsed,
                            }
                            if search_latencies:
                                search_latencies.sort()
                                entry.update({
                                    "search_qps": 1000.0 / (sum(search_latencies) / len(search_latencies)),
                                    "search_p50_ms": search_latencies[int(0.5 * len(search_latencies))],
                                    "search_p99_ms": search_latencies[int(0.99 * len(search_latencies))],
                                })

                            cycle_results.append(entry)
                            print(f"    Cycle {cycle+1}: add={add_ms:.0f}ms del={delete_ms:.0f}ms "
                                  f"search={entry.get('search_p50_ms', 'N/A')}ms")

                        all_results.append({
                            "dtype": dtype, "dim": dim, "payload_kb": payload_kb,
                            "cycles": cycle_results,
                        })

                        try:
                            client.delete_dataset(dataset)
                        except Exception:
                            pass

                except Exception as e:
                    print(f"  Error: {e}")
                finally:
                    self.stop_server()
                    subprocess.run(f"rm -rf {os.path.join(self.data_dir, label)}",
                                   shell=True, capture_output=True)

        with open(self.output_file, "w") as f:
            json.dump({"mode": "churn", "timestamp": self.timestamp, "results": all_results}, f, indent=2)

        print("\n" + "=" * 80)
        print("CHURN SUMMARY")
        print("=" * 80)
        for r in all_results:
            cycles = r.get("cycles", [])
            if cycles:
                avg_cycle_ms = sum(c["cycle_ms"] for c in cycles) / len(cycles)
                print(f"  {r['dtype']} dim={r['dim']} payload={r['payload_kb']}KB: "
                      f"avg_cycle={avg_cycle_ms:.0f}ms over {len(cycles)} cycles")

        print(f"\nResults saved to: {self.output_file}")

    def execute_cluster(self):
        """Test gossip-based cluster search operations."""
        if not HAS_LONGBOW_SDK:
            print(
                "Error: longbow Python SDK not installed. Install with: pip install longbow"
            )
            return

        dims = [int(d) for d in self.args.dims.split(",")]
        counts = [int(c) for c in self.args.counts.split(",")]
        dtypes = self.args.dtypes.split(",")

        print("=" * 80)
        print(f"CLUSTER SEARCH BENCHMARK (Gossip Protocol)")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Nodes in cluster: {self.args.cluster_nodes}")
        print("=" * 80)

        env = os.environ.copy()
        env["LONGBOW_GOSSIP_ENABLED"] = "true"
        env["LONGBOW_GPU_ENABLED"] = "true"
        env["LONGBOW_MAX_MEMORY"] = "8589934592"

        for count in counts:
            for dtype in dtypes:
                for dim in dims:
                    label = f"cluster_{dim}_{dtype}_{count}"
                    nodes = []
                    base_port = 3000

                    try:
                        # Start cluster nodes
                        for i in range(self.args.cluster_nodes):
                            node_label = f"{label}_node{i}"
                            port = base_port + i * 100

                            data_root = os.path.join(self.data_dir, node_label)
                            subprocess.run(f"rm -rf {data_root}", shell=True)
                            os.makedirs(data_root, exist_ok=True)

                            env["LONGBOW_LISTEN_ADDR"] = f"127.0.0.1:{port}"
                            env["LONGBOW_META_ADDR"] = f"127.0.0.1:{port + 1}"
                            env["LONGBOW_DATA_PATH"] = data_root
                            env["LONGBOW_NODE_ID"] = f"node{i}"
                            env["LONGBOW_GOSSIP_PORT"] = str(7946 + i)
                            env["LONGBOW_GOSSIP_ADVERTISE_ADDR"] = "127.0.0.1"
                            if i > 0:
                                env["LONGBOW_GOSSIP_STATIC_PEERS"] = "127.0.0.1:7946"
                            else:
                                env["LONGBOW_GOSSIP_STATIC_PEERS"] = ""

                            server_bin = self.get_server_binary()
                            log_file = os.path.join(self.log_dir, f"longbow_{node_label}.log")

                            with open(log_file, "w") as f:
                                proc = subprocess.Popen(
                                    [server_bin], env=env, stdout=f, stderr=subprocess.STDOUT
                                )
                                nodes.append({"port": port, "pid": proc.pid, "label": node_label})

                            time.sleep(2)

                        # Wait for cluster formation (increased for Metal init)
                        time.sleep(10)

                        print(f"\n[{dtype} {dim}d {count}] Testing cluster search...")
                        client = LongbowClient(uri=f"grpc://127.0.0.1:{base_port}")
                        dataset_name = f"cluster_bench_{dim}_{dtype}_{count}"

                        # Create dataset with correct type
                        vtype = dtype
                        tq_bits = 0
                        if dtype == "turboquant":
                            vtype = "turboquant"
                            tq_bits = 8

                        client.create_dataset(
                            dataset_name,
                            dimensions=dim,
                            vector_type=vtype,
                            turboquant_bits=tq_bits,
                            metric="cosine"
                        )

                        # Insert data
                        if dtype == "complex128":
                            vectors = (np.random.rand(count, dim) + 1j * np.random.rand(count, dim)).astype(np.complex128)
                        elif dtype == "int8":
                            vectors = np.random.randint(-128, 127, (count, dim)).astype(np.int8)
                        elif dtype == "uint8":
                            vectors = np.random.randint(0, 255, (count, dim)).astype(np.uint8)
                        else:
                            vectors = np.random.rand(count, dim).astype(np.float32)

                        ids = [str(i) for i in range(count)]
                        df = pd.DataFrame({
                            "id": ids,
                            "vector": [v for v in vectors],
                            "timestamp": [datetime.now()] * count
                        })

                        start_ingest = time.time()
                        client.insert(dataset_name, df)
                        ingest_duration = time.time() - start_ingest
                        ingest_vec_per_sec = count / ingest_duration if ingest_duration > 0 else 0
                        print(f"  Ingest: {ingest_vec_per_sec:.0f} vec/s")

                        time.sleep(3)

                        # Test global search
                        query_vec = np.random.rand(dim).astype(np.float32).tolist()
                        latencies = []
                        for _ in range(self.args.queries):
                            start = time.time()
                            try:
                                client.search(dataset_name, vector=query_vec, k=10)
                                latency = (time.time() - start) * 1000
                                latencies.append(latency)
                            except Exception as e:
                                pass

                        if latencies:
                            latencies.sort()
                            avg_lat = sum(latencies) / len(latencies)
                            qps = 1000.0 / avg_lat if avg_lat > 0 else 0
                            self.results.append({
                                "dim": dim,
                                "dtype": dtype,
                                "count": count,
                                "nodes": len(nodes),
                                "operation": "global_search",
                                "qps": qps,
                                "ingest_vec_per_sec": ingest_vec_per_sec,
                                "p50": latencies[int(0.5 * len(latencies))],
                                "p99": latencies[int(0.99 * len(latencies))],
                                "timestamp": datetime.now().isoformat(),
                            })
                            print(f"  Global Search QPS: {qps:.1f}, P50: {latencies[int(0.5 * len(latencies))]:.2f}ms")

                    except Exception as e:
                        print(f"Error: {e}")
                    finally:
                        for node in nodes:
                            try:
                                subprocess.run(f"kill -9 {node['pid']}", shell=True, stderr=subprocess.DEVNULL)
                            except: pass
                        subprocess.run("pkill -9 longbow || true", shell=True, stderr=subprocess.DEVNULL)
                        time.sleep(2)

        with open(self.output_file, "w") as f:
            json.dump({
                "mode": "cluster",
                "timestamp": self.timestamp,
                "results": self.results,
            }, f, indent=2)

        self.print_summary()
        print(f"\nResults saved to: {self.output_file}")

    # -------------------------------------------------------------------------
    # Learned Index Benchmark
    # -------------------------------------------------------------------------

    def _fetch_metric(self, metrics_addr: str, metric_name: str, labels: dict | None = None) -> float:
        """Fetch a single metric value from the Prometheus /metrics endpoint."""
        try:
            import urllib.request
            url = f"http://{metrics_addr}/metrics"
            with urllib.request.urlopen(url, timeout=5) as resp:
                body = resp.read().decode()
        except Exception:
            return 0.0

        for line in body.splitlines():
            if line.startswith("#") or not line.strip():
                continue
            if metric_name not in line:
                continue
            if labels:
                if not all(f'{k}="{v}"' in line for k, v in labels.items()):
                    continue
            parts = line.rsplit(" ", 1)
            if len(parts) == 2:
                try:
                    return float(parts[1])
                except ValueError:
                    pass
        return 0.0

    def execute_learned_index(self):
        """
        Learned Index Benchmark
        =======================
        Validates and characterises the k-NN adaptive index scorer introduced in
        learned_index.go. The benchmark has four stages:

        1. Warm-up: inserts three dataset sizes (small/medium/large) and records
           which index Longbow selected before training data accumulates (heuristic
           path, method="default").
        2. Training accumulation: inserts a further batch to cross
           MinTrainingSamples and waits for the first weight update.
        3. Prediction-method verification: reads Prometheus metric
           longbow_learned_index_predictions_total to confirm knn-method
           predictions are now being issued.
        4. Latency measurement: measures p50/p99 search latency over the
           trained index and reports if it improves vs the heuristic phase.
        """
        if not HAS_LONGBOW_SDK:
            print("Error: longbow Python SDK not installed. Install with: pip install longbow")
            return

        metrics_addr = getattr(self.args, "metrics_addr", "127.0.0.1:9090")
        dim = 128
        label = "learned_index_bench"

        print("=" * 80)
        print("LEARNED INDEX BENCHMARK (k-NN Classifier Validation)")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Dim: {dim}")
        print("=" * 80)

        if not self.start_server(label):
            print("  Failed to start server!")
            return

        section_results = {
            "warmup": [],
            "knn_phase": [],
            "prometheus": {},
            "latency_comparison": {},
        }

        try:
            client = LongbowClient(
                uri=f"grpc://{self.server_addr}",
                meta_uri=f"grpc://127.0.0.1:{int(self.server_addr.split(':')[-1]) + 1}",
            )

            # ------------------------------------------------------------------
            # Stage 1 — Warm-up: three dataset sizes, heuristic path.
            # ------------------------------------------------------------------
            print("\n[Stage 1] Warm-up — inserting small/medium/large datasets")
            workloads = [
                ("small",  10_000),
                ("medium", 50_000),
            ]

            for size_label, n_vectors in workloads:
                ds_name = f"li_{size_label}_{dim}d"
                print(f"  Inserting {n_vectors:,} vectors into {ds_name}...", end="", flush=True)
                batch = min(n_vectors, 1000)
                for offset in range(0, n_vectors, batch):
                    vecs = np.random.rand(min(batch, n_vectors - offset), dim).astype(np.float32).tolist()
                    ids = [str(offset + i) for i in range(len(vecs))]
                    client.insert(ds_name, [{"id": id_, "vector": v} for id_, v in zip(ids, vecs)])
                print(" done")

                # Measure heuristic-phase latency (pre-training).
                query = np.random.rand(dim).astype(np.float32).tolist()
                latencies = []
                for _ in range(50):
                    t0 = time.time()
                    try:
                        client.search(ds_name, vector=query, k=10)
                        latencies.append((time.time() - t0) * 1000)
                    except Exception:
                        pass

                if latencies:
                    latencies.sort()
                    p50 = latencies[int(0.50 * len(latencies))]
                    p99 = latencies[int(0.99 * len(latencies))]
                else:
                    p50 = p99 = 0.0

                section_results["warmup"].append({
                    "dataset_size": n_vectors,
                    "size_label": size_label,
                    "heuristic_p50_ms": round(p50, 3),
                    "heuristic_p99_ms": round(p99, 3),
                })
                print(f"  [{size_label}] heuristic p50={p50:.2f}ms p99={p99:.2f}ms")

            # ------------------------------------------------------------------
            # Stage 2 — Accumulate training samples past MinTrainingSamples.
            # ------------------------------------------------------------------
            print("\n[Stage 2] Accumulating training samples (>= MinTrainingSamples)")
            ds_name = f"li_train_{dim}d"
            n_train = 500  # well above default MinTrainingSamples=100
            vecs = np.random.rand(n_train, dim).astype(np.float32).tolist()
            ids = [str(i) for i in range(n_train)]
            client.insert(ds_name, [{"id": id_, "vector": v} for id_, v in zip(ids, vecs)])
            # Issue queries to drive AddTrainingSample calls inside the server.
            query = np.random.rand(dim).astype(np.float32).tolist()
            for _ in range(200):
                try:
                    client.search(ds_name, vector=query, k=10)
                except Exception:
                    pass
            print(f"  Inserted {n_train} training vectors, issued 200 warm-up queries")

            # Wait for async weight update goroutine.
            print("  Waiting 5s for weight update goroutine...", end="", flush=True)
            time.sleep(0.1)
            print(" done")

            # ------------------------------------------------------------------
            # Stage 3 — Read Prometheus metrics.
            # ------------------------------------------------------------------
            print("\n[Stage 3] Reading Prometheus metrics")
            knn_total = self._fetch_metric(
                metrics_addr,
                "longbow_learned_index_predictions_total",
                {"method": "knn"},
            )
            default_total = self._fetch_metric(
                metrics_addr,
                "longbow_learned_index_predictions_total",
                {"method": "default"},
            )
            samples_total = self._fetch_metric(
                metrics_addr,
                "longbow_learned_index_training_samples_total",
            )
            correct_total = self._fetch_metric(
                metrics_addr,
                "longbow_learned_index_prediction_correct_total",
            )
            section_results["prometheus"] = {
                "knn_predictions": knn_total,
                "default_predictions": default_total,
                "training_samples": samples_total,
                "correct_predictions": correct_total,
            }
            print(f"  knn predictions  : {knn_total:.0f}")
            print(f"  default (heuristic): {default_total:.0f}")
            print(f"  training samples : {samples_total:.0f}")
            print(f"  correct  predictions : {correct_total:.0f}")

            if knn_total > 0:
                print("  ✓ k-NN scorer is ACTIVE (knn predictions > 0)")
            else:
                print("  ✗ WARNING: k-NN scorer may not be active — check MinTrainingSamples")

            # ------------------------------------------------------------------
            # Stage 4 — Measure post-training (k-NN) latency and compare.
            # ------------------------------------------------------------------
            print("\n[Stage 4] Latency comparison — post-training vs warm-up")
            query = np.random.rand(dim).astype(np.float32).tolist()
            knn_latencies = []
            for _ in range(200):
                t0 = time.time()
                try:
                    client.search(ds_name, vector=query, k=10)
                    knn_latencies.append((time.time() - t0) * 1000)
                except Exception:
                    pass

            if knn_latencies and section_results["warmup"]:
                knn_latencies.sort()
                knn_p50 = knn_latencies[int(0.50 * len(knn_latencies))]
                knn_p99 = knn_latencies[int(0.99 * len(knn_latencies))]
                heuristic_p50 = section_results["warmup"][0]["heuristic_p50_ms"]
                gain_p50 = heuristic_p50 - knn_p50
                section_results["latency_comparison"] = {
                    "knn_p50_ms": round(knn_p50, 3),
                    "knn_p99_ms": round(knn_p99, 3),
                    "heuristic_p50_ms": heuristic_p50,
                    "latency_gain_p50_ms": round(gain_p50, 3),
                }
                sign = "+" if gain_p50 >= 0 else ""
                print(f"  k-NN phase  p50={knn_p50:.2f}ms  p99={knn_p99:.2f}ms")
                print(f"  Gain vs heuristic p50: {sign}{gain_p50:.2f}ms")
            else:
                print("  Insufficient data for latency comparison")

        except Exception as e:
            print(f"Error during learned index benchmark: {e}")
        finally:
            self._force_cleanup()  # Kill stray processes on our ports before graceful stop
            self.stop_server()
            data_root = os.path.join(self.data_dir, label)
            subprocess.run(f"rm -rf {data_root}", shell=True)

        # Save results
        output = {
            "mode": "learned_index",
            "timestamp": self.timestamp,
            "config": {"dim": dim, "metrics_addr": metrics_addr},
            "results": section_results,
        }
        with open(self.output_file, "w") as f:
            json.dump(output, f, indent=2)

        print("\n" + "=" * 80)
        print("LEARNED INDEX BENCHMARK SUMMARY")
        print("=" * 80)
        prom = section_results["prometheus"]
        print(f"  Training samples buffered : {prom.get('training_samples', 0):.0f}")
        print(f"  k-NN predictions issued   : {prom.get('knn_predictions', 0):.0f}")
        print(f"  Default predictions issued: {prom.get('default_predictions', 0):.0f}")
        print(f"  Correct predictions       : {prom.get('correct_predictions', 0):.0f}")
        lat = section_results.get("latency_comparison", {})
        if lat:
            print(f"  k-NN p50 latency          : {lat.get('knn_p50_ms', 0):.2f}ms")
            print(f"  Latency gain vs heuristic : {lat.get('latency_gain_p50_ms', 0):+.2f}ms")
        print(f"\nResults saved to: {self.output_file}")
        print("=" * 80)


    def get_numa_topology(self):
        try:
            if platform.system() == "Linux":
                res = subprocess.run("numactl --hardware", shell=True, capture_output=True, text=True)
                if res.returncode == 0:
                    return res.stdout.strip()
        except Exception:
            pass
        return "Single NUMA node detected (no NUMA)"

    def check_cache(self, dim, dtype, count, label):
        if not self.args.cache:
            return False
        json_file = os.path.join(self.log_dir, f"result_{label}.json")
        return os.path.exists(json_file)

    def save_results(self):
        """Save current results to JSON matrix file."""
        with open(self.output_file, "w") as f:
            json.dump(
                {
                    "mode": self.args.mode,
                    "timestamp": self.timestamp,
                    "platform": f"{platform.system()} {platform.machine()}",
                    "config": {
                        "dims": [int(d) for d in self.args.dims.split(",")],
                        "counts": [int(c) for c in self.args.counts.split(",")],
                        "dtypes": self.args.dtypes.split(","),
                        "duration": self.args.duration,
                    },
                    "results": self.results,
                },
                f,
                indent=2,
            )

    def execute(self):
        modes = self.args.mode.split(",")
        print(f"Executing benchmarks for modes: {modes}")
        
        runs = [(False, "")]
        if getattr(self.args, "numa_compare", False) and platform.system() == "Linux":
            runs.append((True, "_numa"))
            
        for numa_bind, numa_suffix in runs:
            self.args.numa_bind = numa_bind
            if numa_bind:
                print("\n" + "*" * 80)
                print("RUNNING WITH NUMA BINDING (--cpunodebind=0 --membind=0)")
                print("*" * 80 + "\n")
            
            dims = [int(d) for d in self.args.dims.split(",")]
            counts = [int(c) for c in self.args.counts.split(",")]
            dtypes = self.args.dtypes.split(",")
            count = counts[0] if counts else 1000
            self.check_cuda()
            self.results = [] # Clear results once for all modes
            total = len(dims) * len(dtypes)
            current = 0
            print(f"Duration per test: {self.args.duration}s")
            print("=" * 80)
            for mode in modes:
                mode = mode.strip()
                self.current_mode = mode
                print(f"\n{'#' * 80}")
                print(f"SWITCHING TO MODE: {mode}{numa_suffix}")
                print(f"{'#' * 80}")
                
                if mode == "learned_index":
                    self.execute_learned_index()
                    continue
                if mode == "recommend":
                    self.execute_recommend()
                    continue
                if mode == "deletion":
                    self.execute_deletion()
                    continue
                if mode == "graphrag":
                    self.execute_graphrag()
                    continue
                if mode == "geo":
                    self.execute_geo()
                    continue
                if mode == "exchange":
                    self.execute_exchange()
                    continue
                if mode == "cluster":
                    self.execute_cluster()
                    continue
                if mode == "onnx":
                    self.execute_onnx()
                    continue
                if mode == "churn":
                    self.execute_churn()
                    continue
                if mode == "temporal":
                    self.execute_temporal()
                    continue

                # Default logic for cpu/metal/cuda



                print("=" * 80)
                print(f"UNIFIED BENCHMARK MATRIX ({mode.upper()})")
                print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"Platform: {platform.system()} {platform.machine()}")
                print(f"NUMA Topology:\n{self.get_numa_topology()}")
                print(f"Dims: {dims}")
                print(f"Count: {count}")
                print(f"Types: {dtypes}")
                print("=" * 80)

                print("=" * 80)


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
                            current_port = self.args.port
                            self.server_addr = f"127.0.0.1:{current_port}"

                            label = f"{mode}_{dtype}_{dim}_{count}{numa_suffix}"
                            print(
                                f"\n[{current}/{total * len(counts)}] {dtype} dim={dim} count={count} port={current_port}"
                            )

                            config_key = (mode, dtype, dim)
                            if config_key in self.exhausted_configs:
                                print(f"  [SKIPPED] skipping {label} due to prior ResourceExhausted error")
                                continue

                            # Skip server startup if only generating data
                            if self.args.generate_only:
                                try:
                                    self.run_benchmark(dim, dtype, count, label)
                                except Exception as e:
                                    print(f"  Generation failed: {e}")
                                continue

                            # Start fresh server for this config
                            if not self.start_server(label):
                                print("  Failed to start server!")
                                continue

                            if self.args.cache:
                                json_file = os.path.join(self.log_dir, f"result_{label}.json")
                                if os.path.exists(json_file):
                                    print(f"  [CACHE HIT] Skipping execution for {label}")
                                    continue

                            pprof_thread = None
                            try:
                                max_retries = getattr(self.args, "max_retries", 1)
                                success = False
                                for attempt in range(max_retries):
                                    try:
                                        if self.args.pprof:
                                            pprof_thread = self.collect_pprof(label)

                                        success = self.run_benchmark(dim, dtype, count, label)
                                        if success:
                                            break
                                        print(f"  Benchmark run failed (attempt {attempt+1}/{max_retries})")
                                    except ResourceExhaustedException as re_err:
                                        print(f"  [EARLY ABORT] {re_err}")
                                        self.exhausted_configs.add(config_key)
                                        break
                                    finally:
                                        if pprof_thread:
                                            pprof_thread.join()
                                            pprof_thread = None

                                # Partial save for real-time monitoring
                                with open(self.output_file, "w") as f:
                                    json.dump(
                                        {
                                            "mode": mode,
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
                            finally:
                                if getattr(self.args, "pprof", False):
                                    self.save_pprof_snapshot(label)
                                if pprof_thread:
                                    pprof_thread.join()
                                self.stop_server()
                                # Clean up data directory
                                data_root = os.path.join(self.data_dir, label)
                                subprocess.run(f"rm -rf {data_root}", shell=True)

        self.print_summary()

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
        current_mode = getattr(self, "current_mode", self.args.mode)
        if current_mode == "recommend":
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

        if current_mode == "deletion":
            print("\n" + "─" * 100)
            print("DELETION BENCHMARK SUMMARY (Tombstone Operations)")
            print("─" * 100)
            print(
                f"{'Total':<10} {'Deleted':<10} {'Delete (ms)':<15} {'Search (ms)':<15}"
            )
            print("─" * 100)
            for r in self.results:
                print(
                    f"{r['count']:<10} {r['deleted']:<10} {r['delete_time_ms']:<15.2f} {r['search_time_ms']:<15.2f}"
                )
            print("─" * 100)
            return

        if current_mode == "graphrag":
            print("\n" + "─" * 100)
            print("GRAPHRAG BENCHMARK SUMMARY (Graph Spreading)")
            print("─" * 100)
            print(
                f"{'Alpha':<8} {'K':<6} {'QPS':<12} {'P50 ms':<10} {'P95 ms':<10} {'P99 ms':<10}"
            )
            print("─" * 100)
            for r in self.results:
                if 'alpha' in r:
                    print(
                        f"{r['alpha']:<8} {r['k']:<6} {r['qps']:<12.1f} {r['p50']:<10.2f} {r['p95']:<10.2f} {r['p99']:<10.2f}"
                    )
                elif 'search' in r and 'graphrag' in r['search']:
                    s = r['search']['graphrag']
                    print(
                        f"{'0.5':<8} {'10':<6} {s['qps']:<12.1f} {s['p50']:<10.2f} {s['p95']:<10.2f} {s['p99']:<10.2f}"
                    )
            print("─" * 100)
            return

        if current_mode == "exchange":
            print("\n" + "─" * 100)
            print("DOEXCHANGE BENCHMARK SUMMARY (Mesh Replication)")
            print("─" * 100)
            print(
                f"{'Dim':<8} {'Count':<10} {'Operation':<20} {'QPS':<12} {'P50 ms':<10} {'P99 ms':<10}"
            )
            print("─" * 100)
            for r in self.results:
                print(
                    f"{r['dim']:<8} {r['count']:<10} {r['operation']:<20} {r['qps']:<12.1f} {r['p50']:<10.2f} {r['p99']:<10.2f}"
                )
            print("─" * 100)
            return

        if current_mode == "cluster":
            print("\n" + "─" * 100)
            print("CLUSTER SEARCH BENCHMARK SUMMARY (Gossip Protocol)")
            print("─" * 100)
            print(
                f"{'Dim':<8} {'Count':<10} {'Nodes':<8} {'Operation':<15} {'QPS':<12} {'P50 ms':<10} {'P99 ms':<10}"
            )
            print("─" * 100)
            for r in self.results:
                print(
                    f"{r['dim']:<8} {r['count']:<10} {r['nodes']:<8} {r['operation']:<15} {r['qps']:<12.1f} {r['p50']:<10.2f} {r['p99']:<10.2f}"
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
            search_results = r.get("search", {})
            for s_type, s_data in search_results.items():
                print(
                    f"{r.get('dim', 'N/A'):<8} "
                    f"{r.get('dtype', 'N/A'):<12} "
                    f"{r.get('count', 'N/A'):<8} "
                    f"{s_type:<15} "
                    f"{s_data.get('qps', 0):<10.1f} "
                    f"{s_data.get('p50', 0):<8.3f} "
                    f"{s_data.get('p95', 0):<8.3f} "
                    f"{s_data.get('p99', 0):<8.3f}"
                )
        print("─" * 100)

    def generate_markdown_report(self):
        current_mode = getattr(self, "current_mode", self.args.mode)
        if current_mode == "recommend":
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

        if current_mode == "deletion":
            md_file = self.output_file.replace(".json", ".md")
            with open(md_file, "w") as f:
                f.write("# Deletion Benchmark Results (Tombstone Operations)\n\n")
                f.write(f"**Generated**: {datetime.now().strftime('%Y-%m-%d')}\n")
                f.write(f"**Test Tool**: Longbow Unified Benchmark Script\n\n")
                f.write("## Results\n\n")
                f.write(
                    "| Total Vectors | Deleted | Delete Time (ms) | Search Time (ms) |\n"
                )
                f.write(
                    "|---------------|---------|------------------|------------------|\n"
                )
                for r in self.results:
                    f.write(
                        f"| {r['count']} | {r['deleted']} | {r['delete_time_ms']:.2f} | {r['search_time_ms']:.2f} |\n"
                    )
            return

        if current_mode == "graphrag":
            md_file = self.output_file.replace(".json", ".md")
            with open(md_file, "w") as f:
                f.write("# GraphRAG Benchmark Results (Graph Spreading)\n\n")
                f.write(f"**Generated**: {datetime.now().strftime('%Y-%m-%d')}\n")
                f.write(f"**Test Tool**: Longbow Unified Benchmark Script\n\n")
                f.write("## Alpha Comparison\n\n")
                f.write("| Alpha | K | QPS | P50 (ms) | P95 (ms) | P99 (ms) |\n")
                f.write("|-------|---|-----|----------|----------|----------|\n")
                for r in self.results:
                    if 'alpha' in r:
                        f.write(
                            f"| {r['alpha']} | {r['k']} | {r['qps']:.1f} | {r['p50']:.2f} | {r['p95']:.2f} | {r['p99']:.2f} |\n"
                        )
                    elif 'search' in r and 'graphrag' in r['search']:
                        s = r['search']['graphrag']
                        f.write(
                            f"| 0.5 | 10 | {s['qps']:.1f} | {s['p50']:.2f} | {s['p95']:.2f} | {s['p99']:.2f} |\n"
                        )
            return

        if current_mode == "exchange":
            md_file = self.output_file.replace(".json", ".md")
            with open(md_file, "w") as f:
                f.write("# DoExchange Benchmark Results (Mesh Replication)\n\n")
                f.write(f"**Generated**: {datetime.now().strftime('%Y-%m-%d')}\n")
                f.write(f"**Test Tool**: Longbow Unified Benchmark Script\n\n")
                f.write("## Results\n\n")
                f.write("| Dim | Count | Operation | QPS | P50 (ms) | P99 (ms) |\n")
                f.write("|-----|-------|-----------|-----|----------|----------|\n")
                for r in self.results:
                    f.write(
                        f"| {r['dim']} | {r['count']} | {r['operation']} | {r['qps']:.1f} | {r['p50']:.2f} | {r['p99']:.2f} |\n"
                    )
            return

        if current_mode == "cluster":
            md_file = self.output_file.replace(".json", ".md")
            with open(md_file, "w") as f:
                f.write("# Cluster Search Benchmark Results (Gossip Protocol)\n\n")
                f.write(f"**Generated**: {datetime.now().strftime('%Y-%m-%d')}\n")
                f.write(f"**Test Tool**: Longbow Unified Benchmark Script\n\n")
                f.write("## Results\n\n")
                f.write(
                    "| Dim | Count | Nodes | Operation | QPS | P50 (ms) | P99 (ms) |\n"
                )
                f.write(
                    "|-----|-------|-------|-----------|-----|----------|----------|\n"
                )
                for r in self.results:
                    f.write(
                        f"| {r['dim']} | {r['count']} | {r['nodes']} | {r['operation']} | {r['qps']:.1f} | {r['p50']:.2f} | {r['p99']:.2f} |\n"
                    )
            return

        if current_mode == "geo":
            md_file = self.output_file.replace(".json", ".md")
            with open(md_file, "w") as f:
                f.write("# Geo-Spatial Search Benchmark Results\n\n")
                f.write(f"**Generated**: {datetime.now().strftime('%Y-%m-%d')}\n")
                f.write(f"**Test Tool**: Longbow Unified Benchmark Script\n\n")
                f.write("## Radius Search\n\n")
                f.write("| Search Type | QPS | P50 (ms) | P95 (ms) | P99 (ms) |\n")
                f.write("|-------------|-----|----------|----------|----------|\n")
                for r in self.results:
                    if 'search_type' in r:
                        f.write(f"| {r['search_type']} | {r['qps']:.1f} | {r['p50_ms']:.2f} | "
                                f"{r['p95_ms']:.2f} | {r['p99_ms']:.2f} |\n")
                    elif 'search' in r and 'geo' in r['search']:
                        s = r['search']['geo']
                        f.write(f"| geo | {s['qps']:.1f} | {s['p50']:.2f} | "
                                f"{s['p95']:.2f} | {s['p99']:.2f} |\n")
            return

        if current_mode == "churn":
            md_file = self.output_file.replace(".json", ".md")
            with open(md_file, "w") as f:
                f.write("# Churn Soak Test Results (Add/Delete Cycling)\n\n")
                f.write(f"**Generated**: {datetime.now().strftime('%Y-%m-%d')}\n")
                f.write(f"**Test Tool**: Longbow Unified Benchmark Script\n\n")
                f.write("## Per-Cycle Summary\n\n")
                f.write("| Dtype | Dim | Payload KB | Cycle | Add (ms) | Delete (ms) | Search P50 (ms) |\n")
                f.write("|-------|-----|------------|-------|----------|-------------|----------------|\n")
                for r in self.results:
                    for c in r.get("cycles", []):
                        f.write(f"| {r['dtype']} | {r['dim']} | {r['payload_kb']} | "
                                f"{c['cycle']} | {c['add_ms']:.0f} | {c.get('delete_ms', 0):.0f} | "
                                f"{c.get('search_p50_ms', 'N/A')} |\n")
            return

        md_file = self.output_file.replace(".json", ".md")
        mode_title = current_mode.upper()
        if current_mode == "metal":
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
                if not isinstance(r, dict) or "search" not in r:
                    continue
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
            valid_results = [r for r in self.results if isinstance(r, dict) and "count" in r and "search" in r]
            max_count = max(r["count"] for r in valid_results) if valid_results else 0
            for r in valid_results:
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
    parser = argparse.ArgumentParser(description="Longbow Unified Benchmark Orchestrator")
    parser.add_argument(
        "--mode",
        default="cpu",
        help="Benchmark mode(s), comma-separated: cpu, metal, cuda, onnx, recommend, deletion, graphrag, exchange, cluster, temporal, geo, churn, learned_index",
    )
    parser.add_argument(
        "--dims", default="128,768", help="Comma-separated dimensions"
    )
    parser.add_argument(
        "--counts",
        default="1000,5000",
        help="Comma-separated vector counts (uses first)",
    )
    parser.add_argument(
        "--dtypes", default="float32,float16,int8", help="Comma-separated datatypes"
    )
    parser.add_argument(
        "--memory",
        type=int,
        default=10 * 1024 * 1024 * 1024,
        help="LONGBOW_MAX_MEMORY (default 10GB)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=1800,
        help="Command timeout in seconds (default 1800)",
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
        "--startup-timeout", type=int, default=120, help="Server startup timeout"
    )
    parser.add_argument("--addr", default="127.0.0.1:3000", help="Server address")
    parser.add_argument(
        "--metrics-addr",
        default="127.0.0.1:9090",
        help="Prometheus metrics endpoint for learned_index mode (default 127.0.0.1:9090)",
    )
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
    # Deletion mode parameters
    parser.add_argument(
        "--delete-counts",
        default="100,500,1000",
        help="Comma-separated delete counts to test (for deletion mode)",
    )
    # GraphRAG mode parameters
    parser.add_argument(
        "--graph-alpha-values",
        default="0.0,0.3,0.5,0.7,1.0",
        help="Comma-separated graph_alpha values to test (0.0=disabled, 1.0=full graph)",
    )
    # Cluster mode parameters
    parser.add_argument(
        "--cluster-nodes",
        type=int,
        default=3,
        help="Number of nodes in cluster for cluster mode",
    )
    # Churn (soak test) mode parameters
    parser.add_argument(
        "--churn-payload-sizes",
        default="0,1,4,64,256,1024",
        help="Comma-separated lorem-ipsum payload sizes in KB (0=minimal metadata)",
    )
    parser.add_argument(
        "--churn-cycles",
        default="10",
        help="Number of add/delete cycles per churn test (default 10)",
    )
    parser.add_argument(
        "--churn-chunk-size",
        default="1000",
        help="Number of vectors added/deleted per churn cycle (default 1000)",
    )
    parser.add_argument(
        "--label",
        default="",
        help="Custom label for result files and pprof profiles",
    )
    # Benchmark feature flags
    parser.add_argument(
        "--low-mem",
        action="store_true",
        help="Enable LONGBOW_LOW_MEM=1 for low memory mode",
    )
    parser.add_argument(
        "--use-disk",
        action="store_true",
        help="Enable LONGBOW_USE_DISK=1 for disk-based storage",
    )
    parser.add_argument(
        "--pq-ingest",
        action="store_true",
        help="Enable LONGBOW_PQ_INGEST=1 for PQ encoding during ingest",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable LONGBOW_DEBUG=true for verbose logging",
    )
    parser.add_argument(
        "--learned-samples",
        type=int,
        default=0,
        help="Set LONGBOW_LEARNED_MIN_SAMPLES for learned index",
    )
    parser.add_argument(
        "--learned-confidence",
        type=float,
        default=0.0,
        help="Set LONGBOW_LEARNED_CONFIDENCE_THRESHOLD for learned index",
    )
    parser.add_argument(
        "--learned-interval",
        type=int,
        default=0,
        help="Set LONGBOW_LEARNED_UPDATE_INTERVAL for learned index",
    )
    # Hardware acceleration flags
    parser.add_argument(
        "--rdma",
        action="store_true",
        help="Enable RDMA/RoCEv2 zero-copy ingest",
    )
    parser.add_argument(
        "--iouring",
        action="store_true",
        help="Enable io_uring optimized Parquet snapshots",
    )
    # Benchmarking Infrastructure (Large Scale)
    parser.add_argument(
        "--fbin",
        help="Path to an .fbin file for ingestion benchmarks",
    )
    parser.add_argument(
        "--arrow",
        help="Path to an Arrow IPC file for ingestion benchmarks",
    )
    parser.add_argument(
        "--generate-only",
        action="store_true",
        help="Only generate the test data files and exit",
    )
    parser.add_argument(
        "--output-dir",
        default="data/generated",
        help="Directory to save generated data files",
    )
    parser.add_argument(
        "--pprof",
        action="store_true",
        help="Enable pprof collection during benchmarks",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=1,
        help="Max retries for benchmark execution (default 1)",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=3000,
        help="Base port for server instances (default 3000)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of concurrent search workers (default 8)",
    )
    parser.add_argument(
        "--search-modes",
        type=str,
        default="all",
        help="Comma-separated search modes to run (default: all)",
    )
    parser.add_argument(
        "--full", action="store_true", help="Run the full release candidate matrix (overrides dims/counts/dtypes if not explicitly set)"
    )
    parser.add_argument(
        "--cache", action="store_true", help="Skip unchanged code paths if result json already exists"
    )
    numa_default = False
    try:
        if platform.system() == "Linux":
            numa_default = True
    except Exception:
        pass

    parser.add_argument(
        "--numa-bind",
        dest="numa_bind",
        action="store_true",
        default=numa_default,
        help=f"Enable NUMA binding in benchmarks (default: {numa_default})"
    )
    parser.add_argument(
        "--no-numa-bind",
        dest="numa_bind",
        action="store_false",
        help="Disable NUMA binding in benchmarks"
    )
    parser.add_argument(
        "--numa-compare", action="store_true", help="Run benchmarks with and without NUMA binding to compare"
    )
    parser.add_argument(
        "--ci", action="store_true", help="Run a reduced 'fast' matrix for CI environments"
    )
    args = parser.parse_args()
    
    if args.ci:
        args.dims = "128"
        args.counts = "10000,50000"
        args.dtypes = "float32,int8"
        if args.search_modes == "all":
            args.search_modes = "dense"
            
    runner = BenchmarkRunner(args)
    runner.execute()
