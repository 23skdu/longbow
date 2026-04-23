#!/usr/bin/env python3
import subprocess, json, os, time, shutil
from datetime import datetime

DTYPES = ["float32","float64","float16","int8","int16","int32","int64","uint8","uint16","uint32","uint64","complex64","complex128","turboquant"]
DIMS   = [128, 384]
COUNTS = [500, 1000, 3000, 7000, 15000, 25000, 50000, 100000]
MEMORY = 19327352832
BIN    = "bin/longbow"
MBIN   = "bin/longbow-metal"
BT     = "bin/bench-tool"
LOGDIR = "data/perf_logs"
DATADIR= "data/bench"

os.makedirs(LOGDIR, exist_ok=True)

def timeout_for(count):
    if count <= 1000:    return 30
    if count <= 3000:    return 60
    if count <= 15000:   return 120
    if count <= 50000:   return 180
    return 300

def stop_server():
    subprocess.run("pkill -9 longbow 2>/dev/null || true", shell=True)
    time.sleep(2)

def start_server(mode, label, port=3000):
    stop_server()
    data_root = os.path.join(DATADIR, label)
    shutil.rmtree(data_root, ignore_errors=True)
    os.makedirs(data_root, exist_ok=True)
    log_file = os.path.join(LOGDIR, f"longbow_{mode}_{label}.log")
    env = os.environ.copy()
    env["LONGBOW_MAX_MEMORY"]   = str(MEMORY)
    env["LONGBOW_LISTEN_ADDR"] = f"127.0.0.1:{port}"
    env["LONGBOW_META_ADDR"]   = f"127.0.0.1:{port+1}"
    env["LONGBOW_REST_ADDR"]   = f"127.0.0.1:{port+80}"
    env["LONGBOW_METRICS_ADDR"]= f"127.0.0.1:{port+6000}"
    env["LONGBOW_DATA_PATH"]   = data_root
    env["LONGBOW_NODE_ID"]     = "bench1"
    env["ARROW_DISABLE_LOCKING"] = "1"
    with open(log_file, "w") as f:
        proc = subprocess.Popen(
            [MBIN if mode=="metal" else BIN],
            env=env, stdout=f, stderr=subprocess.STDOUT
        )
    for i in range(60):
        if proc.poll() is not None:
            print(f"  Server crashed with code {proc.returncode}")
            return None
        r = subprocess.run(f"lsof -i :{port} 2>/dev/null | grep LISTEN", shell=True, capture_output=True)
        if r.returncode == 0:
            time.sleep(3)
            return proc
        time.sleep(1)
    print(f"  Server startup timeout on port {port}")
    return None

def run_test(dim, dtype, count, mode, label):
    proc = start_server(mode, label)
    if not proc:
        return None
    json_file = os.path.join(LOGDIR, f"result_{label}.json")
    pprof_file = os.path.join(LOGDIR, f"profile_{label}.pprof")
    metrics_port = 3000 + 6000
    to = timeout_for(count)
    try:
        pprof = subprocess.Popen(
            f"curl -s -o {pprof_file} http://127.0.0.1:{metrics_port}/debug/pprof/profile?seconds=20",
            shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        r = subprocess.run(
            f"{BT} --uri=127.0.0.1:3000 --dim={dim} --dtype={dtype} --scale={count} --queries=1000 --dataset={label} --json={json_file}",
            capture_output=True, text=True, timeout=to+30, shell=True
        )
        pprof.wait()
        if r.returncode != 0:
            print(f"  FAILED: {r.stderr[:200]}")
            with open(os.path.join(LOGDIR, f"longbow_{mode}_{label}.log")) as lf:
                logs = lf.read()
                if "ERROR" in logs or "PANIC" in logs:
                    print(f"  Log errors: {logs[-500:]}")
            return None
        try:
            with open(json_file) as f:
                data = json.load(f)
        except:
            print(f"  NO JSON OUTPUT")
            return None
        return data
    except subprocess.TimeoutExpired:
        print(f"  TIMED OUT after {to}s")
        return None
    finally:
        stop_server()

def extract_metrics(data):
    m = {}
    if isinstance(data, list):
        for e in data:
            n = e.get("name","")
            if n == "DoPut":    m["ingest_qps"] = e.get("throughput",0)
            elif n == "DoGet": m["get_qps"]    = e.get("throughput",0)
            elif n.startswith("Search_"):
                k = n.replace("Search_","").lower()
                m[f"{k}_qps"]  = e.get("throughput",0)
                m[f"{k}_p50"]  = e.get("p50_latency_ms",0)
                m[f"{k}_p95"]  = e.get("p95_latency_ms",0)
                m[f"{k}_p99"]  = e.get("p99_latency_ms",0)
    return m

all_results = []
for mode in ["cpu","metal"]:
    print(f"\n{'='*80}")
    print(f"MODE: {mode.upper()}")
    print(f"{'='*80}")
    for count in COUNTS:
        print(f"\n--- Count: {count} ---")
        for dtype in DTYPES:
            for dim in DIMS:
                label = f"{mode}_{dtype}_{dim}_{count}"
                print(f"\n[{dtype} dim={dim} count={count} {mode}] ...", end="", flush=True)
                data = run_test(dim, dtype, count, mode, label)
                if data:
                    m = extract_metrics(data)
                    if m:
                        entry = {"mode":mode,"dtype":dtype,"dim":dim,"count":count}
                        entry.update(m)
                        all_results.append(entry)
                        print(f" ingest={m.get('ingest_qps',0):.0f} vec/s", end="")
                        for k in ["dense","hnsw","ivf","hybrid"]:
                            q = m.get(f"{k}_qps",0)
                            if q: print(f" {k}={q:.0f} qps", end="")
                        print(" OK")
                    else:
                        print(" NO METRICS")
                else:
                    print(" FAILED")

ts = datetime.now().strftime("%Y%m%d_%H%M%S")
out = os.path.join(LOGDIR, f"full_matrix_{ts}.json")
with open(out,"w") as f:
    json.dump({"timestamp":ts,"results":all_results}, f, indent=2)
print(f"\n\nSaved to {out}")