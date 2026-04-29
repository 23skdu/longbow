import subprocess
import os
import json
import time
import sys
import requests

# Configuration
DIMS = [128, 384, 768, 1024, 3072]
COUNTS = [1000, 5000, 10000, 50000, 100000, 500000]
DTYPES = [
    'float32', 'float16', 'int8', 
    'turboquant2', 'turboquant4', 'turboquant8',
    'complex64'
]

METRICS_ADDR = "http://127.0.0.1:9090"

def run_server(backend="cpu"):
    env = os.environ.copy()
    env["LONGBOW_MAX_MEMORY"] = "19327352832" # 18GB
    env["LONGBOW_LOG_LEVEL"] = "info"
    env["LONGBOW_GPU_ENABLED"] = "true" if backend in ["metal", "cuda"] else "false"
    
    bin_path = "./bin/longbow-metal" if backend == "metal" else "./bin/longbow"
    if backend == "cuda":
        bin_path = "./bin/longbow-cuda"
         
    cmd = [bin_path]
    print(f"Starting server: {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    # Wait for readiness
    for _ in range(30):
        try:
            r = requests.get(f"{METRICS_ADDR}/ready", timeout=1)
            if r.status_code == 200:
                print("Server is ready.")
                break
        except:
            pass
        time.sleep(1)
    else:
        print("Server failed to start.")
        proc.terminate()
        sys.exit(1)
        
    return proc

def run_bench(dtype, dim, scale):
    tq_bits = 4
    actual_dtype = dtype
    if dtype.startswith("turboquant"):
        tq_bits = int(dtype.replace("turboquant", ""))
        actual_dtype = "turboquant"
        
    dataset_name = f"bench_{dtype}_{dim}_{scale}"
    cmd = [
        "./bin/bench-tool",
        "-dataset", dataset_name,
        "-dtype", actual_dtype,
        "-dim", str(dim),
        "-scale", str(scale),
        "-tq-bits", str(tq_bits),
        "-json", "tmp_res.json"
    ]
    try:
        subprocess.run(cmd, check=True, timeout=600)
        with open("tmp_res.json", "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error: {e}")
        return None

def collect_pprof(name):
    try:
        r = requests.get(f"{METRICS_ADDR}/debug/pprof/profile?seconds=5")
        with open(f"pprof/cpu_{name}.prof", "wb") as f:
            f.write(r.content)
        r = requests.get(f"{METRICS_ADDR}/debug/pprof/heap")
        with open(f"pprof/heap_{name}.prof", "wb") as f:
            f.write(r.content)
    except:
        pass

def main():
    backend = sys.argv[1] if len(sys.argv) > 1 else "cpu"
    if not os.path.exists("pprof"):
        os.makedirs("pprof")
        
    server_proc = run_server(backend)
    results = []
    try:
        for dtype in DTYPES:
            for dim in DIMS:
                for scale in COUNTS:
                    print(f"[{backend}] Testing {dtype} dim={dim} scale={scale}")
                    res = run_bench(dtype, dim, scale)
                    if res:
                        results.append({"dtype": dtype, "dim": dim, "scale": scale, "data": res})
                        # Collect pprof during large tests
                        if scale >= 50000:
                            collect_pprof(f"{backend}_{dtype}_{dim}_{scale}")
                        
                        with open(f"results_{backend}.json", "w") as f:
                            json.dump(results, f, indent=2)
    finally:
        print("Shutting down server...")
        server_proc.terminate()
        server_proc.wait()

if __name__ == "__main__":
    main()
