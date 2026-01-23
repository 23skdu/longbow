import sys
import os
import subprocess

ROWS = 15000
TYPES = ["complex64"]
DIMS = [128, 384] # Complex64 means 2x floats, so 128d = 256 floats = 1024 bytes.
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
             "--json", f"res_complex_{dim}.json",  
             "--k", "10",
             "--queries", "100"
        ]
        subprocess.run(cmd, check=True)
