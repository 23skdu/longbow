#!/usr/bin/env python3
import subprocess
import json
import os

PYTHON_EXE = "venv/bin/python" if os.path.exists("venv/bin/python") else "python3"

def main():
    print("Running Debug Test...")
    cmd = [
        PYTHON_EXE, "scripts/perf_test.py",
        "--rows", "1000",
        "--dim", "128",
        "--dtype", "float32",
        "--dataset", "debug_test",
        "--search",
        "--json", "debug_res.json",
        "--k", "10",
        "--queries", "10"
    ]
    subprocess.run(cmd, check=True)
    
    with open("debug_res.json", 'r') as f:
        data = json.load(f)
        print(json.dumps(data, indent=2))
        
        doput = next((r for r in data if r['name'] == 'DoPut'), None)
        print(f"DoPut found: {doput is not None}")
        if doput:
            print(f"Name: '{doput['name']}'")

if __name__ == "__main__":
    main()
