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

class GraphragScenarioMixin:
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

                                        "mean": sum(latencies) / len(latencies),

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


