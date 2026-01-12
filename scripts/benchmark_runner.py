import signal
import shutil
import subprocess
import re
import time
import json
import os
import sys

# Configuration
DIMS = [128, 384]
ROW_COUNTS = [3000, 9000, 15000, 25000, 50000]
QUERIES = 2000

# Regex patterns
RE_PUT_THROUGHPUT = re.compile(r"\[PUT\] Throughput: ([\d\.]+) MB/s \(([\d\.]+) rows/s\)")
RE_GET_THROUGHPUT = re.compile(r"\[GET\] Throughput: ([\d\.]+) MB/s \(([\d\.]+) rows/s\)")

def parse_summary_table(output):
    """
    Parses the summary table from perf_test output.
    Returns a dict mapping Name -> {throughput, p50, p99}
    """
    lines = output.splitlines()
    data = {}
    in_table = False
    for line in lines:
        if "BENCHMARK SUMMARY" in line:
            in_table = True
            continue
        if not in_table:
            continue
        if "-----" in line or "=====" in line or "Name" in line:
            continue
        
        parts = [p.strip() for p in line.split("|")]
        if len(parts) >= 5:
            name = parts[0]
            t_str = parts[1]
            p50 = parts[2]
            p95 = parts[3]
            p99 = parts[4]
            t_val = t_str.split()[0]
            data[name] = {"throughput_val": t_val, "throughput_str": t_str, "p50": p50, "p95": p95, "p99": p99}
    return data

def run_cmd(args):
    """Runs a command and returns stdout."""
    print("Running: " + " ".join(args))
    try:
        return subprocess.check_output(args, stderr=subprocess.STDOUT).decode()
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {e}")
        print("Output:", e.output.decode())
        raise

def start_server():
    print("🚀 Starting Longbow Server...")
    # Clean artifacts before starting
    if os.path.exists("data"):
        shutil.rmtree("data")
    
    # Use os.setsid to create a new process group for easier cleanup
    # Redirect output to file for debugging
    log_file = open("server.log", "w")
    env = os.environ.copy()
    env = os.environ.copy()
    # env["COMPACTION_ENABLED"] = "false"
    # env["LONGBOW_COMPACTION_ENABLED"] = "false"
    
    proc = subprocess.Popen(
        ["go", "run", "cmd/longbow/main.go"],
        cwd=os.getcwd(),
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid
    )
    time.sleep(5) # Wait for startup
    return proc, log_file

def stop_server(proc_tuple):
    proc, log_file = proc_tuple
    print("🛑 Stopping Longbow Server...")
    if proc:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGINT)
            proc.wait(timeout=10)
        except Exception as e:
            print(f"Error stopping server: {e}")
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except:
                pass
    if log_file:
        log_file.close()
    
    # Cleanup artifacts after stopping
    if os.path.exists("data"):
        shutil.rmtree("data")

results_agg = []

# Ensure cleanup on interrupt
def signal_handler(sig, frame):
    print("\nAborting...")
    # We can't easily access 'proc' here without globals, but the server subprocess 
    # should die if we die due to group kill hopefully, or we leak.
    # For now, just exit.
    sys.exit(1)

signal.signal(signal.SIGINT, signal_handler)

for dim in DIMS:
    for rows in ROW_COUNTS:
        print(f"\n👉 STARTING TEST: Dim={dim} Rows={rows}")
        
        # 1. Start Server (Fresh Instance)
        server_proc = start_server()
        
        try:
            dataset = f"bench_{dim}_{rows}_{int(time.time())}"
            
            # --- BENCHMARK STEPS ---
            
            # 1. LOAD
            cmd_load = [sys.executable, "scripts/perf_test.py", "--dataset", dataset, "--dim", str(dim), "--rows", str(rows), "--with-text", "--skip-search"]
            out_load = run_cmd(cmd_load)
            data_load = parse_summary_table(out_load)
            
            metrics_put = data_load.get("DoPut", {})
            put_rows_sec = "N/A"
            m = RE_PUT_THROUGHPUT.search(out_load)
            if m: put_rows_sec = m.group(2)
            
            # 2. DENSE
            cmd_dense = [sys.executable, "scripts/perf_test.py", "--dataset", dataset, "--dim", str(dim), "--rows", str(rows), "--skip-put", "--skip-get", "--queries", str(QUERIES)]
            out_dense = run_cmd(cmd_dense)
            data_dense = parse_summary_table(out_dense)
            
            # 3. FILTERED
            cmd_filter = [sys.executable, "scripts/perf_test.py", "--dataset", dataset, "--dim", str(dim), "--rows", str(rows), "--skip-put", "--skip-get", "--queries", str(QUERIES), "--filter", "id:ge:0"]
            out_filter = run_cmd(cmd_filter)
            data_filter = parse_summary_table(out_filter)
            
            # 4. HYBRID
            cmd_hybrid = [sys.executable, "scripts/perf_test.py", "--dataset", dataset, "--dim", str(dim), "--rows", str(rows), "--with-text", "--skip-put", "--skip-get", "--queries", str(QUERIES), "--alpha", "0.5"]
            out_hybrid = run_cmd(cmd_hybrid)
            data_hybrid = parse_summary_table(out_hybrid)
            
            # 5. SPARSE
            cmd_sparse = [sys.executable, "scripts/perf_test.py", "--dataset", dataset, "--dim", str(dim), "--rows", str(rows), "--with-text", "--skip-put", "--skip-get", "--queries", str(QUERIES), "--alpha", "1.0"]
            out_sparse = run_cmd(cmd_sparse)
            data_sparse = parse_summary_table(out_sparse)
            
            # --- AGGREGATE ---
            get_rows_sec = "N/A"
            m_get = RE_GET_THROUGHPUT.search(out_load)
            if m_get: get_rows_sec = m_get.group(2)

            record = {
                "dim": dim,
                "rows": rows,
                "put_mb_s": metrics_put.get("throughput_val", "0"),
                "put_vec_s": put_rows_sec,
                "get_mb_s": data_load.get("DoGet", {}).get("throughput_val", "0"),
                "get_vec_s": get_rows_sec,
                
                "scan_p50": data_dense.get("VectorSearch_f32", {}).get("p50", "0"),
                "scan_p95": data_dense.get("VectorSearch_f32", {}).get("p95", "0"),
                "scan_p99": data_dense.get("VectorSearch_f32", {}).get("p99", "0"),
                
                "filter_p50": data_filter.get("VectorSearch_f32", {}).get("p50", "0"),
                "filter_p95": data_filter.get("VectorSearch_f32", {}).get("p95", "0"),
                "filter_p99": data_filter.get("VectorSearch_f32", {}).get("p99", "0"),
                
                "hybrid_p50": data_hybrid.get("HybridSearch", {}).get("p50", "0"),
                "hybrid_p95": data_hybrid.get("HybridSearch", {}).get("p95", "0"),
                "hybrid_p99": data_hybrid.get("HybridSearch", {}).get("p99", "0"),
                
                "sparse_p50": data_sparse.get("HybridSearch", {}).get("p50", "0"),
                "sparse_p95": data_sparse.get("HybridSearch", {}).get("p95", "0"),
                "sparse_p99": data_sparse.get("HybridSearch", {}).get("p99", "0"),
            }
            results_agg.append(record)
            print(f"✅ DONE: {dim}x{rows} -> {record}")
            
        except Exception as e:
            print(f"❌ TEST FAILED: {dataset} -> {e}")
        finally:
            stop_server(server_proc)

# Dump raw results
print("\nFINAL JSON:")
print(json.dumps(results_agg, indent=2))
