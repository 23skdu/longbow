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

class OnnxScenarioMixin:
        def execute_onnx(self):

            """Test ONNX reranker benchmarks via Go test binary."""

            print("=" * 80)

            print("ONNX RERANKER BENCHMARK")

            print("Started:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            print("=" * 80)


            bench_bin = os.path.join(self.bin_dir, "longbow")

            if not os.path.exists(bench_bin):

                bench_bin = os.path.join(self.bin_dir, "longbow-metal")

            if not os.path.exists(bench_bin):

                print("  Error: No longbow binary found")

                return


            run_cmd = f"{bench_bin} test -bench=BenchmarkMetalReranker -benchtime={self.args.duration}x -run=^$"

            print(f"  Running: {run_cmd}")


            result = run_command(run_cmd, timeout=self.args.timeout)


            if result and result.returncode == 0:

                self.results.append(

                    {

                        "mode": "onnx",

                        "output": result.stdout,

                        "timestamp": datetime.now().isoformat(),

                    }

                )

                print(f"  COMPLETED")

                print(result.stdout)

            else:

                print(f"  FAILED: {result.stderr if result else 'no output'}")


            with open(self.output_file, "w") as f:

                json.dump(

                    {

                        "mode": "onnx",

                        "timestamp": self.timestamp,

                        "results": self.results,

                    },

                    f,

                    indent=2,

                )

            print(f"\nResults saved to {self.output_file}")


