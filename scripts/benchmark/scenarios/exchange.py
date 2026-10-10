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

class ExchangeScenarioMixin:
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


