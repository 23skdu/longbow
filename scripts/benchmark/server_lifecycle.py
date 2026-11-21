"""Longbow server process lifecycle, readiness polling, and port-scoped cleanup."""

import os
import platform
import re
import signal
import subprocess
import sys
import time

from .memory_guard import format_size_gb, resolve_memory_limit_bytes
from .process import _kill_port, run_command

try:
    from longbow import LongbowClient

    HAS_LONGBOW_SDK = True
except ImportError:
    HAS_LONGBOW_SDK = False


class ServerLifecycleMixin:
    """Server binary resolution, startup, health polling, teardown, and port reaping."""

    def _signal_handler(self, signum, frame):
        """Handle SIGINT/SIGTERM by saving partial results and cleaning up."""
        print(f"\n  [cleanup] Signal {signum} received, saving checkpoint and cleaning up...")
        self._save_checkpoint()
        self._force_cleanup()
        print("  [cleanup] Done. Partial results may be available.")
        sys.exit(1)
    def _own_ports(self):
        """Ports this run may legitimately reclaim."""
        ports = set()
        try:
            base = int(self.server_addr.split(":")[-1])
        except (ValueError, IndexError, AttributeError):
            base = getattr(self.args, "port", 3000)
        for offset in (0, 1, 80, 6000, 7000):
            ports.add(base + offset)
        return ports
    def _reap_only_own_ports(self):
        """Free this run's ports, and nothing else.

        Deliberately narrow (R13, H1). The previous implementation ran
        `pkill -9 -x bench-tool` and friends on every start_server, which matches
        by process name across the whole host and so killed the clients of any
        concurrent benchmark run. The victim was recorded as FAILED with an empty
        error, which is how 10/15 configs at 250k CPU, 4/15 at 50k GPU and 12/15
        at 500k GPU disappeared without a diagnostic.

        Scoping to this run's own ports cannot do that: a concurrent run is using
        different ports by construction, and if two runs do collide on a port then
        one of them genuinely is holding a port the other needs.
        """
        import socket

        killed = []
        for port in sorted(self._own_ports()):
            # lsof/ss are not guaranteed present; fall back to a connect probe,
            # which detects a listener without needing to resolve a pid.
            for pid in self._pids_listening_on(port):
                if pid and pid != os.getpid():
                    try:
                        os.kill(pid, signal.SIGTERM)
                        killed.append((pid, port))
                    except (ProcessLookupError, PermissionError, OSError):
                        continue
        if killed:
            time.sleep(0.5)
            for pid, _port in killed:
                try:
                    os.kill(pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError, OSError):
                    continue
            print(f"  [cleanup] Reaped {len(killed)} process(es) on this run's ports: "
                  + ", ".join(f"pid {p}:{pt}" for p, pt in killed))
        return killed
    def _pids_listening_on(self, port):
        """PIDs listening on `port`, via /proc, without external tools."""
        inodes = set()
        try:
            with open("/proc/net/tcp") as f:
                next(f, None)  # header
                for line in f:
                    parts = line.split()
                    if len(parts) < 10:
                        continue
                    local = parts[1]
                    if int(local.rsplit(":", 1)[1], 16) != port:
                        continue
                    if parts[3] != "0A":  # TCP_LISTEN
                        continue
                    inodes.add(parts[9])
        except OSError:
            return []

        if not inodes:
            return []

        pids = []
        proc = "/proc"
        try:
            entries = os.listdir(proc)
        except OSError:
            return []

        for entry in entries:
            if not entry.isdigit():
                continue
            fd_dir = os.path.join(proc, entry, "fd")
            try:
                fds = os.listdir(fd_dir)
            except OSError:
                continue
            for fd in fds:
                try:
                    link = os.readlink(os.path.join(fd_dir, fd))
                except OSError:
                    continue
                if link.startswith("socket:[") and link[8:-1] in inodes:
                    pids.append(int(entry))
                    break
        return pids
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
        if getattr(self, "args", None) and getattr(self.args, "server_binary", None):
            return os.path.abspath(self.args.server_binary)
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
    def start_server(self, label, env_overrides=None, count=0):
        """Start a fresh Longbow server for a specific configuration.
        
        Args:
            label: Dataset label for directory naming.
            env_overrides: Optional dict of env vars to override defaults.
            count: Vector count for scale-adaptive HNSW parameters.
        """
        self.stop_server()
        
        # Calculate dynamic port to avoid TIME_WAIT issues; never reuses a port
        base_port = self.args.port + self.test_counter * 10
        self.server_addr = f"127.0.0.1:{base_port}"
        port = base_port
        self.test_counter += 1

        print(f"  Cleaning up ports starting from {port}...")
        for p in [port, port + 1, port + 80, port + 6000]:
            _kill_port(p)
        
        # Wait for ports to be actually free with exponential backoff
        import socket
        max_port_attempts = 30
        for p in [port, port + 1, port + 80, port + 6000]:
            for attempt in range(max_port_attempts):
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    try:
                        s.bind(('127.0.0.1', p))
                        break # Success, we can bind!
                    except socket.error:
                        pass # Still in use
                # Exponential backoff: 0.05s, 0.1s, 0.2s, 0.4s, 0.8s, capped at 1.0s
                sleep_time = min(0.05 * (2 ** attempt), 1.0)
                time.sleep(sleep_time)
            else:
                # Port still stuck after all attempts — try random fallback if enabled
                if getattr(self.args, 'random_port_fallback', False):
                    import random
                    fallback_offset = random.randint(1000, 9000)
                    fallback_port = self.args.port + fallback_offset
                    print(f"  Port {p} stuck after {max_port_attempts} attempts, "
                          f"falling back to port offset {fallback_offset} (port {fallback_port})")
                    # Recursively try with fallback port
                    self.args.port = self.args.port + fallback_offset
                    return self.start_server(label, env_overrides=env_overrides, count=count)
                print(f"  Port {p} still in use after {max_port_attempts} attempts!")
                return False
        
        # R13 (H1): never reap by bare process name. `pkill -x bench-tool` matches
        # every bench-tool on the host, including one belonging to a concurrent
        # benchmark run; the in-flight client is then SIGKILLed mid-search and
        # recorded as FAILED with an empty error. That is what silently destroyed
        # 10/15 configs at 250k CPU, 4/15 at 50k GPU and 12/15 at 500k GPU.
        #
        # This run only terminates processes it started itself, tracked by PID,
        # plus anything still holding this run's own ports. A leaked process from
        # an earlier run leaked it under a different port, and killing that would
        # be the same defect pointed the other way.
        self._reap_only_own_ports()

        time.sleep(1) 
        
        server_bin = self.get_server_binary()
        if not os.path.exists(server_bin):
            print(f"  Error: {server_bin} not found!")
            return False

        data_root = os.path.join(self.data_dir, label)
        subprocess.run(f"rm -rf {data_root}", shell=True)
        os.makedirs(data_root, exist_ok=True)

        env = os.environ.copy()

        # ── Core resource limits ──────────────────────────────────────────
        # R14: --memory is authoritative, then LONGBOW_MAX_MEMORY, then a
        # fraction of host RAM. Previously this read an 18 GiB literal and ignored
        # both, so the documented knob did nothing.
        limit_bytes = resolve_memory_limit_bytes(self.args)
        env["LONGBOW_MAX_MEMORY"] = str(limit_bytes)
        available = host_available_memory_bytes()
        if available is not None and limit_bytes > available:
            print(
                f"  [MEMORY] Ceiling {format_size_gb(limit_bytes)} exceeds currently "
                f"available {format_size_gb(available)}. The server will be admitted "
                f"more memory than the host can give it and the client is likely to "
                f"be OOM-killed mid-indexing with no diagnostic."
            )
        else:
            print(f"  [MEMORY] Ceiling {format_size_gb(limit_bytes)}"
                  + (f" (available {format_size_gb(available)})" if available else ""))
        env["LONGBOW_SHUTDOWN_SKIP_FINAL_SNAPSHOT"] = "true"
        # Item 31: the seed has to reach the server too. It fixes per-node
        # level assignment (index.LevelSeed) and, being set at all, pins the
        # ingestion and index worker pools at one and stops EfConstruction
        # being resized off queue depth - without those the same corpus builds
        # a different graph on every run no matter how the client is seeded.
        if getattr(self.args, "seed", None) is not None:
            env["LONGBOW_BENCH_SEED"] = str(self.args.seed)
        env["ARROW_DISABLE_LOCKING"] = "1"
        env["LONGBOW_GOGC"] = "200"
        env["LONGBOW_INGESTION_WORKER_COUNT"] = "6"
        env["LONGBOW_SNAPSHOT_INTERVAL"] = "24h"
        env["LONGBOW_AUTOSCALE_ENABLED"] = "false"
        env["LONGBOW_ADAPTIVE_M_MAX_FACTOR"] = "1.5"
        env["LONGBOW_AUTO_SHARDING_ENABLED"] = "false"

        # ── Scale-adaptive HNSW parameters (override defaults for large scales) ─
        # These trade recall for practical build times on large datasets.
        if count >= 500000:
            env["LONGBOW_MAX_M0"] = "16"
            env["LONGBOW_HNSW_MMAX0"] = "16"
            env["LONGBOW_HNSW_EF_CONSTRUCTION"] = "100"
        elif count >= 50000:
            env["LONGBOW_MAX_M0"] = "16"
            env["LONGBOW_HNSW_MMAX0"] = "16"
            env["LONGBOW_HNSW_EF_CONSTRUCTION"] = "200"
        else:
            env["LONGBOW_MAX_M0"] = "32"
            env["LONGBOW_HNSW_MMAX0"] = "32"
            env["LONGBOW_HNSW_EF_CONSTRUCTION"] = "400"

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
            # Item 34: ask for per-kernel device time on a GPU run. The
            # profile is written beside the pprof snapshots, so a run that got
            # slower says which kernel got slower. Off for the CPU matrix,
            # where there is no device to attribute.
            if os.environ.get("LONGBOW_GPU_KERNEL_PROF", "1") not in ("0", "false", "no", "off"):
                env["LONGBOW_GPU_KERNEL_PROF"] = "1"
        else:
            env["LONGBOW_GPU_ENABLED"] = "false"

        if getattr(self.args, "emlgo", False):
            env["LONGBOW_MATH_DISPATCH"] = "emlgo"

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
        env["LONGBOW_HYBRID_TEXT_COLUMNS"] = "text,category"
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
            # Use auto-spill instead of forced disk mode. Forced LONGBOW_USE_DISK=1
            # makes HNSW graph construction painfully slow because every distance
            # computation during indexing requires a disk read. Auto-spill lets the
            # HNSW build happen in-memory and only spills vectors to disk after
            # indexing is complete (when memory pressure exceeds the threshold).
            env["LONGBOW_AUTO_SPILL_DISK"] = "true"
            env["LONGBOW_SPILL_THRESHOLD_RATIO"] = "0.60"
        # Auto-enable disk spillover for large sets (500k+) to prevent OOM
        max_count_env = max(int(c) for c in self.args.counts.split(","))
        if max_count_env >= 500000:
            env["LONGBOW_AUTO_SPILL_DISK"] = "true"
            env["LONGBOW_SPILL_THRESHOLD_RATIO"] = "0.60"
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
            env["LONGBOW_GRPC_MAX_RECV_MSG_SIZE"] = "21474836470"
            env["LONGBOW_GRPC_MAX_SEND_MSG_SIZE"] = "21474836470"
            print(f"  Scaling gRPC message size for {max_count} vectors")

        # ── Autoshard and sharding (disabled for single-node benchmarks) ──
        env["LONGBOW_AUTO_SHARDING_ENABLED"] = "false"
        env["LONGBOW_RING_SHARDING_ENABLED"] = "true"

        # ── Caller overrides (applied last so they win over computed defaults) ─
        if env_overrides:
            env.update(env_overrides)

        log_file = os.path.join(self.log_dir, f"longbow_{current_mode}_{label}.log")
        cmd = [server_bin]
        if getattr(self.args, "numa_bind", False) and platform.system() == "Linux":
            cmd = ["numactl", "--cpunodebind=0", "--membind=0", server_bin]
            env["LONGBOW_NUMA_NODE"] = "0"
        cpu_affinity = getattr(self.args, "cpu_affinity", None) or os.environ.get("LONGBOW_CPU_AFFINITY")
        if cpu_affinity and platform.system() == "Linux":
            cmd = ["taskset", "-c", str(cpu_affinity)] + cmd

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
                                  f"({connection_refused_retries} polls before /ready answered OK)")
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
    def stop_server(self, force=False):
        if not force and hasattr(self, "args") and self.args and getattr(self.args, "pprof", False):
            print("  Waiting 2 seconds for active pprof collections to complete...")
            time.sleep(2)
        if self.server_pid:
            try:
                if force:
                    os.kill(self.server_pid, signal.SIGKILL)
                    os.waitpid(self.server_pid, 0)
                else:
                    os.kill(self.server_pid, signal.SIGTERM)
                    # Wait up to 25 seconds for graceful stop
                    for _ in range(50):
                        time.sleep(0.5)
                        try:
                            pid_reaped, status = os.waitpid(self.server_pid, os.WNOHANG)
                            if pid_reaped == self.server_pid:
                                self.server_pid = None
                                print("  Waiting 1 seconds for port cooling...")
                                time.sleep(0.1)
                                return
                            os.kill(self.server_pid, 0)
                        except (ProcessLookupError, ChildProcessError):
                            self.server_pid = None
                            print("  Waiting 1 seconds for port cooling...")
                            time.sleep(0.1)
                            return
                    
                    # Fallback to kill -9
                    print(f"  Server PID {self.server_pid} didn't stop gracefully, killing -9")
                    os.kill(self.server_pid, signal.SIGKILL)
                    time.sleep(1)
            except (ProcessLookupError, ChildProcessError):
                pass
            self.server_pid = None
            print("  Waiting 1 seconds for port cooling...")
            time.sleep(0.1)
