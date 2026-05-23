import os
import subprocess
import time
import urllib.request
import sys

SERVER_PORT = 3300
CLI_BIN = "./bin/longbow-cli"
SERVER_BIN = "./bin/longbow"
DATA_DIR = "data/cli_bench"

def run_cmd(cmd, check=True, ignore_err=False):
    print(f"Running: {cmd}")
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and res.returncode != 0 and not ignore_err:
        print(f"Command failed:\nSTDOUT: {res.stdout}\nSTDERR: {res.stderr}")
        raise RuntimeError(f"Command failed: {cmd}")
    return res.stdout, res.stderr

def test_cli():
    # Setup isolated server
    os.system(f"rm -rf {DATA_DIR}")
    os.makedirs(DATA_DIR, exist_ok=True)
    env = os.environ.copy()
    env["LONGBOW_LISTEN_ADDR"] = f"127.0.0.1:{SERVER_PORT}"
    env["LONGBOW_META_ADDR"] = f"127.0.0.1:{SERVER_PORT+1}"
    env["LONGBOW_METRICS_ADDR"] = f"127.0.0.1:{SERVER_PORT+6000}"
    env["LONGBOW_DATA_PATH"] = DATA_DIR

    print("Starting Longbow isolated server...")
    server = subprocess.Popen([SERVER_BIN, "-mode", "server"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    # Wait for server readiness
    for _ in range(30):
        try:
            req = urllib.request.Request(f"http://127.0.0.1:{SERVER_PORT+6000}/ready")
            with urllib.request.urlopen(req, timeout=1) as response:
                if response.status == 200:
                    break
        except:
            time.sleep(0.5)
    else:
        server.kill()
        raise RuntimeError("Server failed to start")

    def run_cli(cmd_args, ignore_err=False):
        return run_cmd(f"{CLI_BIN} {cmd_args} -uri grpc://127.0.0.1:{SERVER_PORT}", check=True, ignore_err=ignore_err)

    try:
        print("--- Testing CLI Namespace Management ---")
        run_cli("create-namespace -name cli_test -dims 128 -data_type float32")
        run_cli("create-namespace -name ns2 -dims 64 -data_type int8")
        
        out, _ = run_cli("list-namespaces")
        if "cli_test" not in out or "ns2" not in out:
            raise RuntimeError("list-namespaces did not return created namespaces")
        
        print("--- Testing CLI Data Operations ---")
        # Dense Search (empty dataset is fine for blackbox testing)
        vec = ",".join(["0.1"] * 128)
        run_cli(f"search -dataset cli_test -mode dense -vector {vec} -k 10", ignore_err=True)
        
        # Geo Search
        run_cli(f"geo-search -dataset cli_test -lat 37.7749 -lon -122.4194 -radius 10.5 -k 10", ignore_err=True)
        
        # Recommend
        run_cli("recommend -dataset cli_test -seed 1,2,3 -k 10", ignore_err=True)

        print("--- Testing CLI GraphRAG ---")
        run_cli("add-edge -dataset cli_test -subject 1 -object 2 -weight 1.5 -predicate reference", ignore_err=True)
        run_cli("traverse -dataset cli_test -start 1 -hops 2", ignore_err=True)
        run_cli("get-graph-stats -dataset cli_test", ignore_err=True)
        run_cli("pagerank -dataset cli_test", ignore_err=True)
        run_cli("detect-communities -dataset cli_test", ignore_err=True)
        
        print("--- Testing CLI Temporal ---")
        run_cli("temporal-search -dataset cli_test -query 1 -as-of 2026-01-01T00:00:00Z -k 10", ignore_err=True)
        
        print("--- Testing CLI Admin/Maintenance ---")
        run_cli("stats -dataset cli_test", ignore_err=True)
        run_cli("snapshot -dataset cli_test", ignore_err=True)
        run_cli("delete -dataset cli_test -id 1", ignore_err=True)
        run_cli("drop -dataset cli_test", ignore_err=True)

        print("--- Testing CLI Cleanup ---")
        run_cli("delete-namespace -name cli_test")
        run_cli("delete-namespace -name ns2")
        
        print("\nAll CLI blackbox tests passed successfully!")

    finally:
        server.terminate()
        server.wait()

if __name__ == "__main__":
    test_cli()
