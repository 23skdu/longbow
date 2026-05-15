import json
import sys
import os

def preview(cpu_file, metal_file):
    with open(cpu_file, 'r') as f:
        cpu_data = json.load(f)
    with open(metal_file, 'r') as f:
        metal_data = json.load(f)

    print("# Longbow Performance Preview (Local M3)")
    print("-" * 80)
    print(f"{'DType':12} | {'Dim':5} | {'Count':8} | {'CPU Ingest':12} | {'Metal Ingest':12} | {'Speedup':8}")
    print("-" * 80)

    # Index by (dtype, dim, count)
    metal_map = {}
    for r in metal_data['results']:
        key = (r['dtype'], r['dim'], r['count'])
        metal_map[key] = r

    for r in cpu_data['results']:
        key = (r['dtype'], r['dim'], r['count'])
        if key in metal_map:
            m = metal_map[key]
            cpu_ingest = r.get('ingest', {}).get('vec_per_sec', 0)
            metal_ingest = m.get('ingest', {}).get('vec_per_sec', 0)
            speedup = metal_ingest / cpu_ingest if cpu_ingest > 0 else 0
            
            # Only show 128d and 3072d for brevity in preview, at 100k scale
            if r['count'] == 100000 and r['dim'] in [128, 3072]:
                print(f"{r['dtype']:12} | {r['dim']:5} | {r['count']:8} | {cpu_ingest:12.0f} | {metal_ingest:12.0f} | {speedup:7.2f}x")

    print("-" * 80)
    print("\n# Search Performance (Dense, 100k vectors, 768d)")
    print(f"{'DType':12} | {'CPU QPS':10} | {'Metal QPS':10} | {'Speedup':8}")
    print("-" * 80)
    
    for r in cpu_data['results']:
        key = (r['dtype'], r['dim'], r['count'])
        if r['count'] == 100000 and r['dim'] == 768:
            m = metal_map.get(key)
            if m:
                cpu_qps = r.get('search', {}).get('dense', {}).get('qps', 0)
                metal_qps = m.get('search', {}).get('dense', {}).get('qps', 0)
                speedup = metal_qps / cpu_qps if cpu_qps > 0 else 0
                print(f"{r['dtype']:12} | {cpu_qps:10.0f} | {metal_qps:10.0f} | {speedup:7.2f}x")

if __name__ == "__main__":
    cpu = "data/perf_logs/aggregated_cpu.json"
    metal = "data/perf_logs/aggregated_metal.json"
    preview(cpu, metal)
