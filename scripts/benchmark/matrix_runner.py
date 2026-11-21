"""Benchmark matrix orchestration and multi-run statistical aggregation."""

import json
import os
import platform
import subprocess
import sys
import time
from datetime import datetime

from .memory_guard import MemoryExceededException
from .process import ResourceExhaustedException
from .telemetry import collect_provenance, validate_concurrency_invariant


class MatrixRunnerMixin:
    """Matrix execution across dimensions, dtypes, counts, and search modalities."""

    def _aggregate_runs(self, run_results, dim, dtype, count, label):
        """Aggregate multiple benchmark runs into a single result with mean/stdev."""
        import math

        if not run_results:
            return None

        # Aggregate search metrics across runs
        all_search_modes = set()
        for r in run_results:
            all_search_modes.update(r.get("search", {}).keys())

        aggregated_search = {}
        for mode in all_search_modes:
            qps_values = [r["search"][mode]["qps"] for r in run_results if mode in r.get("search", {})]
            p50_values = [r["search"][mode]["p50"] for r in run_results if mode in r.get("search", {})]
            p95_values = [r["search"][mode]["p95"] for r in run_results if mode in r.get("search", {})]
            p99_values = [r["search"][mode]["p99"] for r in run_results if mode in r.get("search", {})]

            def mean_stdev(vals):
                if len(vals) < 2:
                    return vals[0] if vals else 0, 0
                m = sum(vals) / len(vals)
                var = sum((x - m) ** 2 for x in vals) / (len(vals) - 1)
                return m, math.sqrt(var)

            qps_mean, qps_stdev = mean_stdev(qps_values)
            p50_mean, p50_stdev = mean_stdev(p50_values)
            p95_mean, p95_stdev = mean_stdev(p95_values)
            p99_mean, p99_stdev = mean_stdev(p99_values)

            aggregated_search[mode] = {
                "qps": round(qps_mean, 1),
                "qps_stdev": round(qps_stdev, 1),
                "p50": round(p50_mean, 3),
                "p50_stdev": round(p50_stdev, 3),
                "p95": round(p95_mean, 3),
                "p95_stdev": round(p95_stdev, 3),
                "p99": round(p99_mean, 3),
                "p99_stdev": round(p99_stdev, 3),
                "runs": len(qps_values),
            }

        # Aggregate ingest
        ingest_values = [r.get("ingest", {}).get("vec_per_sec", 0) for r in run_results]
        ingest_mean, ingest_stdev = 0, 0
        if ingest_values:
            ingest_mean = sum(ingest_values) / len(ingest_values)
            if len(ingest_values) >= 2:
                var = sum((x - ingest_mean) ** 2 for x in ingest_values) / (len(ingest_values) - 1)
                ingest_stdev = math.sqrt(var)

        idx_values = [
            r.get("ingest", {}).get("indexing_duration_seconds", 0)
            for r in run_results
            if r.get("ingest", {}).get("indexing_duration_seconds", 0) > 0
        ]
        idx_mean, idx_stdev = 0, 0
        if idx_values:
            idx_mean = sum(idx_values) / len(idx_values)
            if len(idx_values) >= 2:
                var = sum((x - idx_mean) ** 2 for x in idx_values) / (len(idx_values) - 1)
                idx_stdev = math.sqrt(var)

        # Peak memory
        peak_mbs = [r.get("peak_memory_mb", 0) for r in run_results]
        peak_mb = max(peak_mbs) if peak_mbs else 0

        return {
            "dim": dim,
            "dtype": dtype,
            "count": count,
            "mode": run_results[0].get("mode", "cpu"),
            "ingest": {
                "vec_per_sec": round(ingest_mean, 1),
                "vec_per_sec_stdev": round(ingest_stdev, 1),
                "indexing_duration_seconds": round(idx_mean, 3),
                "indexing_duration_seconds_stdev": round(idx_stdev, 3),
            },
            "search": aggregated_search,
            "peak_memory_mb": round(peak_mb, 2),
            "tq_bits": run_results[0].get("tq_bits", 0),
            "num_runs": len(run_results),
            "timestamp": datetime.now().isoformat(),
        }
    def execute(self):
        modes = self.args.mode.split(",")
        
        # Handle --memory-soak flag as a mode shortcut
        if getattr(self.args, "memory_soak", False):
            if "memory_soak" not in modes:
                modes = ["memory_soak"]
        
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
                self.results = []
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
                if mode == "range":
                    self.execute_range()
                    continue
                if mode == "federated":
                    self.execute_federated()
                    continue
                if mode == "text":
                    self.execute_text()
                    continue
                if mode == "multimodal":
                    self.execute_multimodal()
                    continue
                if mode == "memory_soak":
                    self.execute_memory_soak()
                    continue

                # Default logic for cpu/metal/cuda

                # Load checkpoint for resume — skip previously completed configs
                checkpoint_results = self._load_checkpoint()
                if checkpoint_results:
                    self.results = checkpoint_results

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

                            config_key = (mode, dtype, dim, count)
                            if config_key in self.exhausted_configs:
                                print(f"  [SKIPPED] skipping {label} due to prior ResourceExhausted error")
                                continue
                            if config_key in self.completed_configs:
                                print(f"  [SKIPPED] skipping {label} — already completed (resume)")
                                continue
                            if not self._check_memory_limit(dtype, dim, count):
                                self.exhausted_configs.add(config_key)
                                continue

                            # Skip server startup if only generating data
                            if self.args.generate_only:
                                try:
                                    self.run_benchmark(dim, dtype, count, label)
                                except Exception as e:
                                    print(f"  Generation failed: {e}")
                                continue

                            # Start fresh server with scale-adaptive HNSW settings
                            if not self.start_server(label, count=count):
                                print("  Failed to start server!")
                                continue

                            if self.args.cache:
                                json_file = os.path.join(self.log_dir, f"result_{label}.json")
                                if os.path.exists(json_file):
                                    print(f"  [CACHE HIT] Loading cached result for {label}")
                                    if hasattr(self, 'results'):
                                        try:
                                            with open(json_file) as _jf:
                                                _jr = json.load(_jf)
                                            if isinstance(_jr, dict):
                                                self.results.append(_jr)
                                            elif isinstance(_jr, list):
                                                for _entry in _jr:
                                                    self.results.append(_entry)
                                        except Exception:
                                            pass
                                    continue

                            pprof_thread = None
                            exhausted = False
                            server_crashed = False
                            try:
                                max_retries = getattr(self.args, "max_retries", 1)
                                num_runs = getattr(self.args, "runs", 1)
                                success = False
                                for attempt in range(max_retries):
                                    try:
                                        if self.args.pprof:
                                            pprof_thread = self.collect_pprof(label)

                                        # Multi-run support: run N times and aggregate
                                        if num_runs > 1:
                                            run_results = []
                                            for run_idx in range(num_runs):
                                                print(f"  --- Run {run_idx+1}/{num_runs} ---")
                                                run_ok = self.run_benchmark(dim, dtype, count, f"{label}_run{run_idx}")
                                                if run_ok:
                                                    run_results.append(self.results[-1])
                                            if run_results:
                                                # Aggregate: compute mean/stdev across runs
                                                aggregated = self._aggregate_runs(run_results, dim, dtype, count, label)
                                                # Remove individual runs, add aggregated result
                                                self.results = [r for r in self.results if r not in run_results]
                                                self.results.append(aggregated)
                                                success = True
                                                self.completed_configs.add(config_key)
                                        else:
                                            success = self.run_benchmark(dim, dtype, count, label)
                                            if success:
                                                self.completed_configs.add(config_key)
                                        if success:
                                            break
                                        print(f"  Benchmark run failed (attempt {attempt+1}/{max_retries})")
                                        # Detect server crash: if server process is dead,
                                        # skip retries and flag for cleanup.
                                        if self.server_pid is not None:
                                            try:
                                                os.kill(self.server_pid, 0)
                                            except (ProcessLookupError, ChildProcessError):
                                                print(f"  [CRASH] Server for {label} died unexpectedly")
                                                server_crashed = True
                                                break
                                    except ResourceExhaustedException as re_err:
                                        print(f"  [EARLY ABORT] {re_err}")
                                        self.exhausted_configs.add(config_key)
                                        exhausted = True
                                        break
                                    finally:
                                        if pprof_thread:
                                            pprof_thread.join()
                                            pprof_thread = None

                                # Track failed configs for retry-failed
                                if not success and not exhausted and not server_crashed:
                                    self.failed_configs.add(config_key)
                                    # R17 (H8): keep why it failed. A
                                    # SIGKILLed client, a timeout and a crash
                                    # were all "FAILED" with an empty error,
                                    # which is how collateral damage from H1
                                    # looked identical to a real failure.
                                    reason = getattr(self, "last_failure_reason", None)
                                    if reason:
                                        self.failure_reasons[config_key] = reason

                            finally:
                                # Save results BEFORE pprof snapshot — don't let pprof
                                # hang or fail lose the benchmark results.
                                self._save_checkpoint()

                                if server_crashed:
                                    print(f"  [CRASH] Server died — cleaning up without pprof snapshot")
                                    self.stop_server(force=True)
                                elif exhausted:
                                    # OOM'd server is unrecoverable, force-kill immediately.
                                    print("  [OOM] Force-killing server immediately...")
                                    self.stop_server(force=True)
                                else:
                                    if getattr(self.args, "pprof", False):
                                        self.save_pprof_snapshot(label)
                                    if pprof_thread:
                                        pprof_thread.join()
                                    self.stop_server()

                                # Clean up data directory
                                data_root = os.path.join(self.data_dir, label)
                                subprocess.run(f"rm -rf {data_root}", shell=True)

        self.print_summary()

        # Self-validate before writing (roadmap R3). A report that violates
        # QPS x mean_latency ~= workers is refused rather than emitted, because a
        # number that cannot be true of its own worker count cannot gate a
        # regression: every later comparison against it is meaningless.
        provenance = collect_provenance(self.args)
        violations = validate_concurrency_invariant(self.results, provenance.get("workers"))

        payload = {
            "mode": self.args.mode,
            "timestamp": self.timestamp,
            "platform": f"{platform.system()} {platform.machine()}",
            "config": {
                "dims": dims,
                "counts": counts,
                "dtypes": dtypes,
                "duration": self.args.duration,
            },
            "provenance": provenance,
            "validation": {
                "invariant": "qps * mean_latency_ms implies >=75% and <=102% of recorded workers",
                "workers": provenance.get("workers"),
                "ok": not violations,
                "violations": violations,
            },
            "results": self.results,
            "completed_configs": [list(c) for c in self.completed_configs],
            "failed_configs": [list(c) for c in self.failed_configs],
            "exhausted_configs": [list(c) for c in self.exhausted_configs],
            "failure_reasons": {str(k): v for k, v in self.failure_reasons.items()},
        }

        if violations:
            print("\n" + "=" * 80)
            print("REFUSING TO WRITE REPORT: self-validation failed")
            print("=" * 80)
            print(f"Invoked with --workers {provenance.get('workers')}, which caps in-flight")
            print("requests. These rows claim more concurrency than that allows:")
            for v in violations[:25]:
                print(f"  - {v}")
            if len(violations) > 25:
                print(f"  ... and {len(violations) - 25} more")
            print()
            print("Either the worker count was recorded wrongly, or QPS and latency came")
            print("from different runs. Re-run with an accurate --workers. The previous")
            print(f"report at {self.output_file} is left untouched.")
            sys.exit(2)

        # Save results
        with open(self.output_file, "w") as f:
            json.dump(payload, f, indent=2)

        # Print summary
        self.print_summary()
        self.generate_markdown_report()

        print("\n" + "=" * 80)
        print(f"Results saved to: {self.output_file}")
        print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
