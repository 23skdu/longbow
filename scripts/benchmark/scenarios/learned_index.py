import os
import sys
import json
import time
import math
import shutil
import platform
import subprocess
from datetime import datetime

try:
    import numpy as np
    import pandas as pd
    HAS_ANALYSIS_LIBS = True
except ImportError:
    HAS_ANALYSIS_LIBS = False

try:
    import pyarrow as pa
    import pyarrow.flight as flight
    import longbow
    HAS_LONGBOW_SDK = True
except ImportError:
    HAS_LONGBOW_SDK = False

class LearnedIndexScenarioMixin:
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


