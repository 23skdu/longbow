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

class DeletionScenarioMixin:
        def execute_deletion(self):

            """Test deletion and tombstone operations."""

            if not HAS_LONGBOW_SDK:

                print(

                    "Error: longbow Python SDK not installed. Install with: pip install longbow"

                )

                return


            dims = [int(d) for d in self.args.dims.split(",")]

            counts = [int(c) for c in self.args.counts.split(",")]

            delete_counts = [int(d) for d in self.args.delete_counts.split(",")]


            count = counts[0] if counts else 10000

            dim = dims[0] if dims else 128


            print("=" * 80)

            print(f"DELETION BENCHMARK (Tombstone Operations)")

            print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            print(f"Dim: {dim}, Total Count: {count}")

            print(f"Delete counts: {delete_counts}")

            print("=" * 80)


            label = f"del_{dim}_{count}"

            if not self.start_server(label):

                print("  Failed to start server!")

                return


            try:

                client = LongbowClient(

                    uri=f"grpc://{self.server_addr}",

                    meta_uri=f"grpc://127.0.0.1:{int(self.server_addr.split(':')[-1]) + 1}",

                )


                dataset_name = f"del_bench_{dim}d"

                print(f"\nCreating dataset {dataset_name} with {count} vectors...")


                vectors = np.random.rand(count, dim).astype(np.float32).tolist()

                ids = [str(i) for i in range(count)]


                client.insert(

                    dataset_name,

                    [{"id": id, "vector": vec} for id, vec in zip(ids, vectors)],

                )

                time.sleep(3)  # Wait for indexing


                # Test different delete counts

                for del_count in delete_counts:

                    del_ids = [str(i) for i in range(del_count)]

                    print(f"\nDeleting {del_count} vectors...")


                    start = time.time()

                    try:

                        client.delete(dataset_name, del_ids)

                        del_time = (time.time() - start) * 1000

                    except Exception as e:

                        print(f"  Delete error: {e}")

                        continue


                    # Verify search still works after deletion

                    query_vec = np.random.rand(dim).astype(np.float32).tolist()

                    start = time.time()

                    try:

                        results = client.search(dataset_name, vector=query_vec, k=10)

                        search_time = (time.time() - start) * 1000

                    except Exception as e:

                        print(f"  Search after delete error: {e}")

                        continue


                    self.results.append(

                        {

                            "dim": dim,

                            "count": count,

                            "deleted": del_count,

                            "delete_time_ms": del_time,

                            "search_time_ms": search_time,

                            "timestamp": datetime.now().isoformat(),

                        }

                    )

                    print(f"  Delete: {del_time:.2f}ms, Search: {search_time:.2f}ms")


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

                        "mode": "deletion",

                        "timestamp": self.timestamp,

                        "config": {

                            "dim": dim,

                            "count": count,

                            "delete_counts": delete_counts,

                        },

                        "results": self.results,

                    },

                    f,

                    indent=2,

                )


            self.print_summary()

            print(f"\nResults saved to: {self.output_file}")


