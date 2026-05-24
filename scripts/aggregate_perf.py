import json
import glob
import os

def load_results(directory, platform):
    results = []
    for f in glob.glob(f"{directory}/result_*.json"):
        try:
            with open(f, 'r') as fp:
                data_list = json.load(fp)
                for data in data_list:
                    data['platform'] = platform
                    results.append(data)
        except Exception as e:
            pass
    return results

local_res = load_results('data/perf_logs', 'Local (M3)')
remote_res = load_results('data/perf_logs/remote', 'Remote (Ancalagon)')
all_res = local_res + remote_res

print(f"Loaded {len(local_res)} local, {len(remote_res)} remote")

print("| Platform | Dtype | Dim | Count | Ingest (vec/s) | QPS | P50 (ms) | P95 (ms) | P99 (ms) |")
print("|---|---|---|---|---|---|---|---|---|")

all_res.sort(key=lambda x: (x.get('dim',0), x.get('scale',0), x.get('dtype',''), x['platform']))

for r in all_res:
    plat = r['platform']
    dtype = r.get('dtype', 'N/A')
    dim = r.get('dim', 0)
    count = r.get('scale', 0)
    ingest = r.get('ingest_rate', 0)
    qps = r.get('qps', 0)
    p50 = r.get('p50_latency_ms', 0)
    p95 = r.get('p95_latency_ms', 0)
    p99 = r.get('p99_latency_ms', 0)
    print(f"| {plat} | {dtype} | {dim} | {count} | {ingest:.0f} | {qps:.0f} | {p50:.2f} | {p95:.2f} | {p99:.2f} |")
