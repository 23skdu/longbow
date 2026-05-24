import json
import glob
import os

def process_dir(directory, env_name):
    files = glob.glob(os.path.join(directory, 'result_*.json'))
    results = []
    for f in files:
        try:
            with open(f, 'r') as fh:
                data = json.load(fh)
                if isinstance(data, list):
                    for d in data:
                        d['env'] = env_name
                        if 'mode' not in d:
                            # Try to infer mode from filename
                            basename = os.path.basename(f)
                            if 'metal' in basename: d['mode'] = 'metal'
                            elif 'cuda' in basename: d['mode'] = 'cuda'
                            elif 'cpu' in basename: d['mode'] = 'cpu'
                            else: d['mode'] = 'unknown'
                        results.append(d)
                elif isinstance(data, dict):
                    data['env'] = env_name
                    results.append(data)
        except Exception as e:
            print(f"Error reading {f}: {e}")
    return results

local_results = process_dir('data/perf_logs', 'Local (Mac)')
remote_results = process_dir('data/remote_perf_logs', 'Ancalagon (Linux)')

all_results = local_results + remote_results

# Group by env, mode
summary = {}
for r in all_results:
    env_mode = f"{r.get('env', 'Unknown')} - {r.get('mode', 'unknown')}"
    if env_mode not in summary:
        summary[env_mode] = []
    summary[env_mode].append(r)

output = ["# Longbow Performance Benchmarks (0.2.1-rc4)\n"]
output.append("This document contains the latest performance benchmarking results collated from the local macOS machine and the high-end multi-socket AMD64 server `ancalagon`.\n")
output.append("The tests cover CPU, Metal, and CUDA backends across various datatypes and vector counts for 384 dimensions.\n\n")

for env_mode, results in sorted(summary.items()):
    output.append(f"## {env_mode.upper()}\n")
    output.append("| Dtype | Dim | Count | Ingest (vec/s) | QPS | P50 (ms) | P95 (ms) | P99 (ms) |")
    output.append("|-------|-----|-------|----------------|-----|----------|----------|----------|")
    
    results.sort(key=lambda x: (x.get('count', 0), x.get('dtype', '')))
    
    for r in results:
        dtype = r.get('dtype', 'N/A')
        dim = r.get('dim', 384)
        count = r.get('count', 0)
        
        # Extract metrics
        ingest_vps = 0
        if 'ingest' in r and 'vec_per_sec' in r['ingest']:
            ingest_vps = r['ingest']['vec_per_sec']
        elif 'vec_per_sec' in r:
             ingest_vps = r['vec_per_sec']
             
        qps = r.get('qps', 0)
        p50 = r.get('p50_latency_ms', 0)
        p95 = r.get('p95_latency_ms', 0)
        p99 = r.get('p99_latency_ms', 0)
        
        output.append(f"| {dtype} | {dim} | {count} | {ingest_vps:,.0f} | {qps:,.0f} | {p50:.2f} | {p95:.2f} | {p99:.2f} |")
    
    output.append("\n")

with open('docs/performance.md', 'w') as f:
    f.write('\n'.join(output))

print("Successfully generated docs/performance.md")
