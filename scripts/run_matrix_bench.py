import subprocess
import os
import sys
import time
import threading

# Configuration
LOCAL_DTYPES = "float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
REMOTE_DTYPES = LOCAL_DTYPES

DIMS_LOW = "128,384"
COUNTS_LOW = "1000,5000,10000,50000,100000"

DIMS_HIGH = "768,1024,3072"
COUNTS_HIGH = "1000,5000,10000,50000,100000"

MODES = ["cpu", "metal", "cuda", "temporal", "geo", "graphrag", "learned_index"]

MAX_MEMORY = 19327352832  # 18GB

def run_local_bench():
    print("[LOCAL] Starting benchmarks...")
    # Build local (already done manually)
    # print("[LOCAL] Building binaries...")
    # subprocess.run("make clean && make build-gpu && go build -o bin/bench-tool ./cmd/bench-tool", shell=True)

    for mode in MODES:
        if mode == "cuda": continue
        
        # Low dimensions
        cmd_low = f"venv/bin/python3 scripts/unified_benchmark.py --mode {mode} --dims {DIMS_LOW} --counts {COUNTS_LOW} --dtypes {LOCAL_DTYPES} --memory {MAX_MEMORY} --timeout 300 --label local_low_{mode}"
        print(f"[LOCAL] Running Low: {mode}")
        subprocess.run(cmd_low, shell=True)

        # High dimensions
        cmd_high = f"venv/bin/python3 scripts/unified_benchmark.py --mode {mode} --dims {DIMS_HIGH} --counts {COUNTS_HIGH} --dtypes {LOCAL_DTYPES} --memory {MAX_MEMORY} --timeout 300 --label local_high_{mode}"
        print(f"[LOCAL] Running High: {mode}")
        subprocess.run(cmd_high, shell=True)
    print("[LOCAL] Completed.")

def run_remote_bench():
    print("[REMOTE] Syncing and starting benchmarks on ancalagon...")
    # Sync
    subprocess.run("ssh ancalagon 'mkdir -p ~/longbow_bench'", shell=True)
    # Exclude bin to avoid arch mismatch
    subprocess.run("rsync -avz --exclude '.git' --exclude 'venv' --exclude 'data' --exclude 'bin' . ancalagon:~/longbow_bench/", shell=True)
    
    # Setup venv on remote if not exists
    print("[REMOTE] Setting up venv...")
    setup_cmd = (
        "ssh ancalagon 'cd ~/longbow_bench && "
        "if [ ! -d venv ]; then "
        "python3 -m venv venv --without-pip && "
        "curl -sS https://bootstrap.pypa.io/get-pip.py | venv/bin/python3 && "
        "venv/bin/pip install numpy pandas tabulate; "
        "fi'"
    )
    subprocess.run(setup_cmd, shell=True)

    # Build on remote (already done manually)
    # print("[REMOTE] Building binaries...")
    # subprocess.run("ssh ancalagon 'cd ~/longbow_bench && make clean && make build-gpu && go build -o bin/bench-tool ./cmd/bench-tool'", shell=True)
    
    # Run benchmarks sequentially to avoid port conflicts
    for mode in MODES:
        if mode == "metal": continue
        
        # Low dimensions
        cmd_low = f"ssh ancalagon 'cd ~/longbow_bench && venv/bin/python3 scripts/unified_benchmark.py --mode {mode} --dims {DIMS_LOW} --counts {COUNTS_LOW} --dtypes {REMOTE_DTYPES} --memory {MAX_MEMORY} --timeout 300 --label remote_low_{mode}'"
        print(f"[REMOTE] Running Low: {mode}")
        subprocess.run(cmd_low, shell=True)

        # High dimensions
        cmd_high = f"ssh ancalagon 'cd ~/longbow_bench && venv/bin/python3 scripts/unified_benchmark.py --mode {mode} --dims {DIMS_HIGH} --counts {COUNTS_HIGH} --dtypes {REMOTE_DTYPES} --memory {MAX_MEMORY} --timeout 300 --label remote_high_{mode}'"
        print(f"[REMOTE] Running High: {mode}")
        subprocess.run(cmd_high, shell=True)
    print("[REMOTE] Completed.")

if __name__ == "__main__":
    t_local = threading.Thread(target=run_local_bench)
    t_remote = threading.Thread(target=run_remote_bench)
    
    t_local.start()
    t_remote.start()
    
    t_local.join()
    t_remote.join()
    print("All benchmarks finished.")
