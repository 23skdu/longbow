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

class ClusterScenarioMixin:
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

                            create_kwargs = {}

                            if dtype == "turboquant":

                                vtype = "turboquant"

                            elif dtype == "turboquant4":

                                vtype = "turboquant"

                                create_kwargs["turboquant_bits"] = 4

                            elif dtype == "turboquant8":

                                vtype = "turboquant"

                                create_kwargs["turboquant_bits"] = 8


                            client.create_dataset(

                                dataset_name,

                                dimensions=dim,

                                vector_type=vtype,

                                metric="cosine",

                                **create_kwargs

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

                                    "mean": sum(latencies) / len(latencies),

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

                            # R13 (H1): no global `pkill -9 longbow` here either.

                            # The loop above already SIGKILLs every node this run

                            # started, by PID; matching by name additionally killed

                            # any longbow on the host, including one belonging to a

                            # concurrent benchmark run, whose client was then

                            # recorded as FAILED with an empty error. Cluster teardown

                            # is scoped to the PIDs this run owns.

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


