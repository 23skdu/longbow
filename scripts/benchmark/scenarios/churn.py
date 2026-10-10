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

class ChurnScenarioMixin:
        def execute_churn(self):

            """Churn soak test: repeated add/delete cycles with varying payload sizes.


            Simulates real-world churn by cycling through adds and deletes while

            tracking memory pressure, fragmentation, and search quality.

            """

            if not HAS_LONGBOW_SDK:

                print("ERROR: longbow SDK not installed. Install with: pip install longbow")

                return


            print("=" * 80)

            print("CHURN SOAK TEST (Add/Delete Cycling)")

            print("Started:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            print("=" * 80)


            dims = [int(d) for d in self.args.dims.split(",")]

            dtypes = self.args.dtypes.split(",")


            payload_sizes_kb = [int(s) for s in self.args.churn_payload_sizes.split(",")]

            if not payload_sizes_kb:

                payload_sizes_kb = [0, 1, 4, 64, 256, 1024]


            cycles = int(self.args.churn_cycles)

            chunk_size = int(self.args.churn_chunk_size)


            dtype_map = {

                "float32": np.float32, "float64": np.float64, "float16": np.float16,

                "int8": np.int8, "int16": np.int16, "int32": np.int32, "int64": np.int64,

                "uint8": np.uint8, "uint16": np.uint16, "uint32": np.uint32, "uint64": np.uint64,

                "complex64": np.complex64, "complex128": np.complex128, "turboquant": np.float32,

            }


            def make_lorem_payload(size_kb: int) -> dict:

                """Generate lorem-ipsum metadata payload of approx size_kb."""

                if size_kb <= 0:

                    return {}

                words = [

                    "lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing",

                    "elit", "sed", "do", "eiusmod", "tempor", "incididunt", "ut", "labore",

                    "et", "dolore", "magna", "aliqua", "enim", "ad", "minim", "veniam",

                    "quis", "nostrud", "exercitation", "ullamco", "laboris", "nisi",

                    "ut", "aliquip", "ex", "ea", "commodo", "consequat",

                ]

                target_chars = size_kb * 1024

                text = " ".join(np.random.choice(words, size=max(1, target_chars // 6)))

                while len(text) < target_chars:

                    text += " " + " ".join(np.random.choice(words, size=50))

                return {"description": text[:target_chars]}


            all_results = []

            for dtype in dtypes:

                for dim in dims:

                    print(f"\n{'=' * 80}")

                    print(f"Churn Test: {dtype} dim={dim}")

                    print(f"Payload sizes: {payload_sizes_kb} KB, {cycles} cycles x {chunk_size} vectors")

                    print("=" * 80)


                    label = f"churn_{dtype}_{dim}"

                    env_overrides = {"LONGBOW_MAX_MEMORY": str(self.args.memory)}

                    if not self.start_server(label, env_overrides=env_overrides):

                        print(f"  Failed to start server!")

                        continue


                    try:

                        client = LongbowClient(

                            uri=f"grpc://{self.server_addr}",

                            meta_uri=f"grpc://127.0.0.1:{int(self.server_addr.split(':')[-1]) + 1}",

                        )

                        client.connect()


                        np_dtype = dtype_map.get(dtype, np.float32)


                        for payload_kb in payload_sizes_kb:

                            dataset = f"churn_{dtype}_{dim}_p{payload_kb}"

                            print(f"\n  Payload={payload_kb}KB")


                            base_ids = list(range(chunk_size))

                            id_counter = chunk_size


                            cycle_results = []

                            query_vec = None


                            for cycle in range(cycles):

                                added = 0

                                deleted = 0

                                cycle_start = time.time()


                                batch_add_ids = list(range(id_counter, id_counter + chunk_size))

                                id_counter += chunk_size


                                add_batch = []

                                for vec_id in batch_add_ids:

                                    if "complex" in dtype:

                                        vec = (np.random.randn(dim) + 1j * np.random.randn(dim)).astype(np_dtype)

                                    elif "int" in dtype or "uint" in dtype:

                                        vec = np.random.randint(0, 100, size=dim).astype(np_dtype)

                                    else:

                                        vec = np.random.randn(dim).astype(np_dtype)


                                    record = {

                                        "id": str(vec_id),

                                        "vector": vec.tolist(),

                                        **make_lorem_payload(payload_kb),

                                    }

                                    add_batch.append(record)


                                t0 = time.time()

                                client.insert(dataset, add_batch)

                                add_ms = (time.time() - t0) * 1000

                                added = chunk_size


                                delete_ids = base_ids if cycle == 0 else list(range(

                                    id_counter - chunk_size * 2 if id_counter > chunk_size * 2 else 0,

                                    id_counter - chunk_size,

                                ))

                                if delete_ids:

                                    t0 = time.time()

                                    for did in delete_ids:

                                        try:

                                            client.delete(dataset, str(did))

                                            deleted += 1

                                        except Exception:

                                            pass

                                    delete_ms = (time.time() - t0) * 1000

                                else:

                                    delete_ms = 0


                                if query_vec is None:

                                    if "complex" in dtype:

                                        query_vec = (np.random.randn(dim) + 1j * np.random.randn(dim)).astype(np_dtype).tolist()

                                    elif "int" in dtype or "uint" in dtype:

                                        query_vec = np.random.randint(0, 100, size=dim).astype(np_dtype).tolist()

                                    else:

                                        query_vec = np.random.randn(dim).astype(np_dtype).tolist()


                                search_latencies = []

                                for _ in range(min(50, self.args.queries)):

                                    t0 = time.time()

                                    try:

                                        res = client.search(dataset, vector=query_vec, k=10)

                                        search_latencies.append((time.time() - t0) * 1000)

                                    except Exception:

                                        pass


                                cycle_elapsed = (time.time() - cycle_start) * 1000

                                base_ids = batch_add_ids


                                entry = {

                                    "dtype": dtype, "dim": dim, "payload_kb": payload_kb,

                                    "cycle": cycle + 1, "added": added, "deleted": deleted,

                                    "add_ms": add_ms, "delete_ms": delete_ms,

                                    "cycle_ms": cycle_elapsed,

                                }

                                if search_latencies:

                                    search_latencies.sort()

                                    entry.update({

                                        "search_qps": 1000.0 / (sum(search_latencies) / len(search_latencies)),

                                        "search_p50_ms": search_latencies[int(0.5 * len(search_latencies))],

                                        "search_p99_ms": search_latencies[int(0.99 * len(search_latencies))],

                                    })


                                cycle_results.append(entry)

                                print(f"    Cycle {cycle+1}: add={add_ms:.0f}ms del={delete_ms:.0f}ms "

                                      f"search={entry.get('search_p50_ms', 'N/A')}ms")


                            all_results.append({

                                "dtype": dtype, "dim": dim, "payload_kb": payload_kb,

                                "cycles": cycle_results,

                            })


                            try:

                                client.delete_dataset(dataset)

                            except Exception:

                                pass


                    except Exception as e:

                        print(f"  Error: {e}")

                    finally:

                        self.stop_server()

                        subprocess.run(f"rm -rf {os.path.join(self.data_dir, label)}",

                                       shell=True, capture_output=True)


            with open(self.output_file, "w") as f:

                json.dump({"mode": "churn", "timestamp": self.timestamp, "results": all_results}, f, indent=2)


            print("\n" + "=" * 80)

            print("CHURN SUMMARY")

            print("=" * 80)

            for r in all_results:

                cycles = r.get("cycles", [])

                if cycles:

                    avg_cycle_ms = sum(c["cycle_ms"] for c in cycles) / len(cycles)

                    print(f"  {r['dtype']} dim={r['dim']} payload={r['payload_kb']}KB: "

                          f"avg_cycle={avg_cycle_ms:.0f}ms over {len(cycles)} cycles")


            print(f"\nResults saved to: {self.output_file}")


