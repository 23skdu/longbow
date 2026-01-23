import subprocess
import json
import os
import sys
import time
import signal

# Configuration
DTYPES = [
    "int8", "int16", "int32", "int64",
    "uint8", "uint16", "uint32", "uint64",
    "float16", "float32", "float64",
    "complex64", "complex128"
]
DIM = 384
ROWS = 50000

# Setup SDK Path
current_dir = os.getcwd()
sdk_src = os.path.join(current_dir, "longbowclientsdk", "src")
if sdk_src not in sys.path:
    sys.path.insert(0, sdk_src)
os.environ["PYTHONPATH"] = f"{sdk_src}:{os.environ.get('PYTHONPATH', '')}"

def start_cluster():
    print("Building Longbow binary...")
    subprocess.run(["go", "build", "-o", "./longbow", "./cmd/longbow"], check=True)
    
    print("Cleaning up old data...")
    subprocess.run(["rm", "-rf", "./data1", "./data2", "./data3"], check=True)
    
    nodes = []
    env_base = {
        "LONGBOW_GOSSIP_ENABLED": "true",
        "LONGBOW_LOG_LEVEL": "error",
        "GOMEMLIMIT": "8GiB"
    }
    
    # Node 1
    env1 = env_base.copy()
    env1.update({
        "LONGBOW_LISTEN_ADDR": "0.0.0.0:3000",
        "LONGBOW_META_ADDR": "0.0.0.0:3001",
        "LONGBOW_GOSSIP_PORT": "7946",
        "LONGBOW_DATA_PATH": "./data1",
        "LONGBOW_NODE_ID": "node1",
        "LONGBOW_HTTP_PORT": "9090",
        "LONGBOW_GOSSIP_STATIC_PEERS": "127.0.0.1:7947,127.0.0.1:7948"
    })
    p1 = subprocess.Popen(["./longbow"], env=env1, stdout=open("node1.log", "w"), stderr=subprocess.STDOUT)
    nodes.append(p1)
    
    time.sleep(2)
    
    # Node 2
    env2 = env_base.copy()
    env2.update({
        "LONGBOW_LISTEN_ADDR": "0.0.0.0:3010",
        "LONGBOW_META_ADDR": "0.0.0.0:3011",
        "LONGBOW_GOSSIP_PORT": "7947",
        "LONGBOW_DATA_PATH": "./data2",
        "LONGBOW_NODE_ID": "node2",
        "LONGBOW_HTTP_PORT": "9091",
        "LONGBOW_GOSSIP_STATIC_PEERS": "127.0.0.1:7946,127.0.0.1:7948"
    })
    p2 = subprocess.Popen(["./longbow"], env=env2, stdout=open("node2.log", "w"), stderr=subprocess.STDOUT)
    nodes.append(p2)
    
    time.sleep(2)
    
    # Node 3
    env3 = env_base.copy()
    env3.update({
        "LONGBOW_LISTEN_ADDR": "0.0.0.0:3020",
        "LONGBOW_META_ADDR": "0.0.0.0:3021",
        "LONGBOW_GOSSIP_PORT": "7948",
        "LONGBOW_DATA_PATH": "./data3",
        "LONGBOW_NODE_ID": "node3",
        "LONGBOW_HTTP_PORT": "9092",
        "LONGBOW_GOSSIP_STATIC_PEERS": "127.0.0.1:7946,127.0.0.1:7947"
    })
    p3 = subprocess.Popen(["./longbow"], env=env3, stdout=open("node3.log", "w"), stderr=subprocess.STDOUT)
    nodes.append(p3)
    
    print("Waiting for cluster to stabilize...")
    time.sleep(10)
    return nodes

def stop_cluster(nodes):
    print("Stopping cluster nodes...")
    for p in nodes:
        p.terminate()
    for p in nodes:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()
    subprocess.run(["pkill", "longbow"], stderr=subprocess.DEVNULL)

def run_bench(dtype):
    dataset_name = f"bench_{dtype}_{DIM}"
    output_file = f"results_{dtype}.json"
    
    print(f"\n>>> Benchmarking {dtype}...")
    cmd = [
        sys.executable, "scripts/perf_test.py",
        "--dataset", dataset_name,
        "--rows", str(ROWS),
        "--dim", str(DIM),
        "--dtype", dtype,
        "--search",
        "--json", output_file,
        "--k", "10",
        "--queries", "100"
    ]
    
    try:
        subprocess.run(cmd, check=True)
        if os.path.exists(output_file):
            with open(output_file, 'r') as f:
                data = json.load(f)
                put = next((r for r in data if r['name'].startswith('DoPut')), None)
                get = next((r for r in data if r['name'].startswith('DoGet')), None)
                search = next((r for r in data if r['name'].startswith('VectorSearch')), None)
                return {
                    "put_mb": put['throughput'] if put else 0,
                    "get_mb": get['throughput'] if get else 0,
                    "search_qps": search['throughput'] if search else 0
                }
    except Exception as e:
        print(f"  Error benchmarking {dtype}: {e}")
    return {"put_mb": 0, "get_mb": 0, "search_qps": 0}

def main():
    nodes = []
    try:
        nodes = start_cluster()
        results = []
        for dtype in DTYPES:
            res = run_bench(dtype)
            results.append({
                "dtype": dtype,
                "dim": DIM,
                **res
            })
            
        print("\n" + "="*80)
        print(f"{'DType':<12} | {'Dim':<6} | {'Put MB/s':<10} | {'Get MB/s':<10} | {'Search QPS':<10}")
        print("-" * 80)
        for r in results:
            print(f"{r['dtype']:<12} | {r['dim']:<6} | {r['put_mb']:<10.2f} | {r['get_mb']:<10.2f} | {r['search_qps']:<10.2f}")
        print("="*80)
        
        with open("final_bench_results.json", "w") as f:
            json.dump(results, f, indent=2)
            
    finally:
        if nodes:
            stop_cluster(nodes)

if __name__ == "__main__":
    main()
