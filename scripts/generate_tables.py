import json
import glob

def get_matrix(filepath):
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            benchmarks = data.get('results', [])
            for b in benchmarks:
                if 'cuda' in b.get('mode', ''):
                    b['platform'] = 'Remote (Ancalagon)'
                else:
                    b['platform'] = 'Local (M3)'
            return benchmarks
    except Exception as e:
        print(f"Error {e}")
        return []

all_files = glob.glob('data/perf_logs/perf_matrix_*.json')

all_data = []
for f in all_files:
    all_data.extend(get_matrix(f))

def print_table(dim, count):
    print(f"## Performance (dim={dim}, count={count})")
    print("| Platform | Dtype | Search Mode | Ingest (vec/s) | QPS | p50 (ms) | p95 (ms) | p99 (ms) |")
    print("|---|---|---|---|---|---|---|---|")
    
    # Sort for consistency
    all_data.sort(key=lambda x: (x.get('platform', ''), x.get('dtype', '')))
    
    for b in all_data:
        if b.get('dim') == dim and b.get('count') == count:
            plat = b['platform']
            dtype = b.get('dtype')
            ingest = b.get('ingest', {}).get('vec_per_sec', 0)
            search = b.get('search', {})
            for s_mode in ['dense', 'sparse', 'temporal']:
                metrics = search.get(s_mode, {})
                print(f"| {plat} | {dtype} | {s_mode.capitalize()} | {ingest:.0f} | {metrics.get('qps',0):.0f} | {metrics.get('p50',0):.2f} | {metrics.get('p95',0):.2f} | {metrics.get('p99',0):.2f} |")
    print("\n")

print_table(128, 50000)
print_table(128, 100000)
print_table(384, 50000)
print_table(384, 100000)
