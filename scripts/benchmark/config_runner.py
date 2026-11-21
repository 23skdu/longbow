"""Single-configuration benchmark execution, pprof capture, and NUMA discovery."""

from datetime import datetime
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

from .process import ResourceExhaustedException, _kill_port, parse_bench_json, run_command
from .telemetry import derive_config_seed


class ConfigRunnerMixin:
    """Per-configuration benchmark execution and profiling helpers."""

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
            client.connect()
            # Create namespace via SDK
            client.create_namespace(label, dims=dim, data_type=dtype)

            # Generate test vectors
            import numpy as np

            vectors = np.random.rand(count, dim).astype(np.float32)

            # Normalize
            norms = np.linalg.norm(vectors, axis=1, keepdims=True)
            vectors = vectors / np.maximum(norms, 1e-12)

            # Insert via SDK
            client.insert(
                label,
                [{"id": str(i), "vector": vec.tolist()} for i, vec in enumerate(vectors)],
            )

            # Search via SDK
            query = vectors[0].tolist()
            for _ in range(min(100, self.args.queries)):
                client.search(label, query, k=10)

            print(f" SDK done")
            return True
        except Exception as e:
            print(f" SDK error: {e}")
            return False
        finally:
            try:
                client.close()
            except Exception:
                pass

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

        # Handle TurboQuant bit-packs (R15, H5).
        #
        # This used to reassign `dtype`, so `turboquant4` and `turboquant8` were
        # both recorded as "turboquant" and every "turboquant" row in the results
        # was two configurations. Only tq_bits told them apart, and the comparison
        # key did not include it - so half the TurboQuant matrix could not be
        # attributed. The requested dtype string is now preserved and the wire
        # form is a separate variable.
        is_turboquant = False
        tq_bits = 0
        requested_dtype = dtype
        wire_dtype = dtype
        if dtype == "turboquant4":
            wire_dtype = "turboquant"
            tq_bits = 4
            is_turboquant = True
        elif dtype == "turboquant8":
            wire_dtype = "turboquant"
            tq_bits = 8
            is_turboquant = True

        # Run benchmark-tool (does ingest + search + all modes)
        uri = self.server_addr
        if not uri.startswith("grpc://"):
            uri = f"grpc://{self.server_addr}"

        search_modes = self.args.search_modes
        if search_modes == "all":
            search_modes = "all"

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
        # Remove stale result file so we don't silently reuse old data
        if os.path.exists(json_file):
            os.remove(json_file)

        # R12: record the exact, ordered mode list handed to the client, so a
        # later comparison can tell whether two runs used the same modes in the
        # same order. bench-tool normalises and reorders the list it is given.
        self.args.resolved_search_modes = [m for m in str(search_modes).split(",") if m]
        base_seed = getattr(self.args, "seed", None)
        if base_seed is not None:
            config_seed = derive_config_seed(base_seed, label)
            seed_arg = f" -seed {config_seed}"
            self.current_config_seed = config_seed
        else:
            config_seed = None
            seed_arg = ""
            self.current_config_seed = None
        shuffle_arg = " -shuffle-modes" if getattr(self.args, "shuffle_modes", False) else ""
        # Item 34: on a GPU run, ask the server for its per-kernel device time
        # and keep it beside the pprof snapshots, so a run that got slower can
        # be attributed to a kernel rather than to "GPU".
        gpu_profile_arg = ""
        self.current_gpu_profile = None
        if getattr(self, "current_mode", self.args.mode) in ("cuda", "metal"):
            gpu_profile_path = os.path.join(self.log_dir, f"gpu_kernel_profile_{label}.json")
            gpu_profile_arg = f" -gpu-profile-out {gpu_profile_path}"
            self.current_gpu_profile = gpu_profile_path
        cmd = f"{bench_tool} -mode vec -uri {uri} -dim {dim} -dtype {wire_dtype}{tq_arg}{seed_arg}{shuffle_arg} -scale {batch_size} -queries {self.args.queries} -workers {self.args.workers} -dataset {label} -json {json_file} -search-modes {search_modes}{extra_args}{gpu_profile_arg}"
        cpu_affinity = getattr(self.args, "cpu_affinity", None) or os.environ.get("LONGBOW_CPU_AFFINITY")
        if cpu_affinity and platform.system() == "Linux":
            cmd = f"taskset -c {cpu_affinity} {cmd}"
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

        # HNSW build time dominates at large scales. Add a multiplier for
        # slow data types where distance computations are bandwidth-bound.
        hnsw_build_multiplier = 1.0
        if batch_size >= 50000 and dtype in ("float64", "complex64", "complex128", "int64", "uint64"):
            hnsw_build_multiplier = 4.0 if batch_size >= 500000 else 2.0

        scaled_timeout = int((batch_size / base_speed) * dim_factor * hnsw_build_multiplier)
        search_overhead = int(self.args.queries / 100.0 * dim_factor)
        scaled_timeout += max(search_overhead, 60)

        # HNSW indexing at 500k+ scale dominates; ensure generous ceiling
        if batch_size >= 500000:
            scaled_timeout = max(scaled_timeout, 7200)

        # Scaled timeout is primary; clamp between base_timeout and 12x base_timeout
        timeout = max(base_timeout, scaled_timeout)
        timeout = min(timeout, base_timeout * 12)

        # Override bench-tool's internal HNSW indexing timeout (default 3600s
        # is too short for 500k+ vectors). Set to match the outer timeout.
        os.environ["LONGBOW_BENCH_HNSW_TIMEOUT"] = str(timeout)
        
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

        # Item 34: fold the server's per-kernel device time into the result row,
        # so the perf matrix carries the attribution next to the QPS it explains
        # and not only in a side file someone has to know to open.
        gpu_profile = getattr(self, "current_gpu_profile", None)
        if gpu_profile and os.path.exists(gpu_profile):
            try:
                with open(gpu_profile) as _pf:
                    payload = json.load(_pf)
                ops = (payload.get("profile") or {}).get("ops") or []
                if ops:
                    metrics["gpu_kernel_profile"] = {
                        "attributed_pct": payload.get("attributed_pct", 0.0),
                        "total_ms": (payload.get("profile") or {}).get("total_ms", 0.0),
                        "untimed_calls": (payload.get("profile") or {}).get("untimed_calls", 0),
                        "ops": [
                            {
                                "name": o.get("name"),
                                "calls": o.get("calls", 0),
                                "total_ms": round(float(o.get("total_ms", 0.0)), 3),
                                "share_pct": round(float(o.get("share_pct", 0.0)), 2),
                            }
                            for o in ops
                        ],
                    }
            except (json.JSONDecodeError, OSError, TypeError, ValueError):
                print("  (GPU kernel profile unreadable; continuing)")
        if not metrics:
            # R17 (H8): a SIGKILLed client, a timeout and a crash all used to be
            # reported identically - "FAILED" with an empty error - which is how
            # H1's collateral damage was indistinguishable from a genuine
            # failure. The exit code and signal are what separate them.
            rc = getattr(result, "returncode", None) if result else None
            signal_name = None
            if rc is not None and rc < 0:
                try:
                    import signal as _signal

                    signal_name = _signal.Signals(-rc).name
                except (ValueError, AttributeError):
                    signal_name = f"SIG{-rc}"
            elif rc is not None and rc > 128:
                # Shell convention: 128+n means terminated by signal n.
                try:
                    import signal as _signal

                    signal_name = _signal.Signals(rc - 128).name
                except (ValueError, AttributeError):
                    signal_name = f"SIG{rc - 128}"

            if signal_name:
                reason = f"client terminated by {signal_name}"
            elif rc == 124:
                reason = "client timed out"
            elif rc is not None and rc != 0:
                reason = f"client exited {rc}"
            elif rc is None:
                reason = "client produced no output and no result file"
            else:
                reason = "client exited 0 but wrote no parsable metrics"

            print(f" FAILED ({reason})")
            if result and result.stderr:
                print(f"    Error: {result.stderr.strip()}")
            self.last_failure_reason = reason
            return False

        # Extract all search types
        search_metrics = {}
        # Only validate modes relevant to the current benchmark mode
        current_mode = getattr(self, "current_mode", self.args.mode)
        resolved_modes = getattr(self.args, "resolved_search_modes", ["all"])
        if current_mode == "temporal":
            expected_modes = ["temporal_as_of", "temporal_range", "temporal_window"]
        elif resolved_modes == ["all"]:
            expected_modes = [
                "dense", "hybrid", "sparse", "filtered", "filteredbool",
                "filteredstring", "byid", "range", "advancedtext", "graphrag",
                "globalgraphrag", "recommend", "geo", "geobox", "geohybrid",
                "temporal", "temporalrange", "temporalwindow", "multimodal",
                "federated", "learnedindex",
            ]
        else:
            expected_modes = [m.lower().replace("_", "").replace("-", "") for m in resolved_modes]
        for key, value in metrics.items():
            if "_qps" in key:
                prefix = key.replace("_qps", "")
                search_metrics[prefix] = {
                    "qps": value,
                    "mean": metrics.get(f"{prefix}_mean_ms", 0),
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

        # Measure on-disk vector store usage if --use-disk is active
        disk_mb = 0.0
        if getattr(self.args, "use_disk", False):
            disk_mb = self._measure_disk_usage(label)

        # Measure peak memory from /proc/<pid>/status
        peak_mb = 0.0
        if getattr(self, "server_pid", None):
            try:
                with open(f"/proc/{self.server_pid}/status", "r") as f:
                    for line in f:
                        if line.startswith("VmHWM:"):
                            peak_mb = float(line.split()[1]) / 1024.0
                            break
            except Exception:
                pass
        if peak_mb > 0:
            print(f"  [MEMORY] Peak Server RSS: {peak_mb:.1f} MB")

        result_entry = {
            "dim": dim,
            "dtype": dtype,
            "count": batch_size,
            "mode": self.args.mode,
            "ingest": {
                "vec_per_sec": metrics.get("ingest_vec_per_sec", 0),
                "duration_seconds": metrics.get("ingest_duration_seconds", 0),
                "indexing_duration_seconds": metrics.get("indexing_duration_seconds", 0),
                "indexing_vec_per_sec": metrics.get("indexing_vec_per_sec", 0),
            },
            "search": search_metrics,
            "disk_usage_mb": disk_mb,
            "peak_memory_mb": round(peak_mb, 2),
            # R15: both fields are needed. requested_dtype keeps turboquant4 and
            # turboquant8 distinguishable by name; tq_bits makes the comparison
            # key explicit rather than depending on parsing the dtype string.
            "dtype": requested_dtype,
            "requested_dtype": requested_dtype,
            "wire_dtype": wire_dtype,
            "tq_bits": tq_bits,
            "seed": self.args.seed if getattr(self.args, "seed", None) is not None else None,
            # Item 31: the seed actually handed to the client for this config.
            # Recorded next to the run seed because the two are not equal and a
            # reproduction needs the one the corpus was drawn from.
            "config_seed": getattr(self, "current_config_seed", None),
            # Item 34: the per-kernel device time for this row, when the run was
            # a GPU one. Absent for CPU runs rather than an empty table, so a
            # reader can tell "not measured" from "measured, nothing ran".
            **({"gpu_kernel_profile": metrics["gpu_kernel_profile"]}
               if metrics.get("gpu_kernel_profile") else {}),
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

        idx_sec = metrics.get("indexing_duration_seconds", 0)
        if idx_sec > 0:
            print(f" {vec_per_sec:.0f} vec/s (index: {idx_sec:.2f}s)")
        else:
            print(f" {vec_per_sec:.0f} vec/s")
        return True
    def get_numa_topology(self):
        try:
            if platform.system() == "Linux":
                res = subprocess.run("numactl --hardware", shell=True, capture_output=True, text=True)
                if res.returncode == 0:
                    return res.stdout.strip()
        except Exception:
            pass
        return "Single NUMA node detected (no NUMA)"
