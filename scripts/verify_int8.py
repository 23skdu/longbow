import sys
import os
import subprocess

# Add scripts dir to path if needed (though we call subprocess)
ROWS = 15000
TYPES = ["int8"]
DIMS = [128, 384]
PYTHON_EXE = "venv/bin/python" if os.path.exists("venv/bin/python") else "python3"

for dtype in TYPES:
    for dim in DIMS:
        print(f"Running {dtype} @ {dim}d")
        ds_name = f"bench_{dtype}_{dim}_{ROWS}"
        cmd = [
             PYTHON_EXE, "scripts/perf_test.py",
             "--rows", str(ROWS),
             "--dim", str(dim),
             "--dtype", dtype,
             "--dataset", ds_name,
             "--search",
             "--json", f"res_int8_{dim}.json",  
             "--k", "10",
             "--queries", "100"
        ]
        subprocess.run(cmd, check=True)
