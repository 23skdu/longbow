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

class RecommendScenarioMixin:
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

                                    "mean": sum(latencies) / len(latencies),

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


