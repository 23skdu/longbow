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

class TemporalScenarioMixin:
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

                            dataset_name = f"temporal_{dtype}_{dim}"

                            for stype in search_types:

                                try:

                                    if stype == "as_of":

                                        res = client.temporal_search(

                                            search_type=stype,

                                            dataset=dataset_name,

                                            timestamp=base_timestamp + count * 500000000,

                                            k=10,

                                        )

                                    elif stype == "range":

                                        res = client.temporal_search(

                                            search_type=stype,

                                            dataset=dataset_name,

                                            start_time=base_timestamp,

                                            end_time=base_timestamp + count * 1000000000,

                                            k=10,

                                        )

                                    elif stype == "sliding_window":

                                        res = client.temporal_search(

                                            search_type=stype,

                                            dataset=dataset_name,

                                            window_size=100,

                                            k=10,

                                        )

                                    elif stype == "sliding_window_time":

                                        res = client.temporal_search(

                                            search_type=stype,

                                            dataset=dataset_name,

                                            duration=3600 * 1000000000,

                                            k=10,

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

                                history = client.temporal_version_history(vector_id=0, dataset=dataset_name)

                                print(f"    Version history: {len(history) if history else 0} versions")

                                results.append(

                                    {"version_history_count": len(history) if history else 0}

                                )

                            except Exception as e:

                                print(f"    Version history: ERROR - {e}")


                            try:

                                agg = client.temporal_aggregation(

                                    aggregation_type="count",

                                    dataset=dataset_name,

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


