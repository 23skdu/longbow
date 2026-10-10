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

class GeoScenarioMixin:
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


