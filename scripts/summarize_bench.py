import json
import sys
import glob
import os

def parse_results(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    print(f"Results for {os.path.basename(file_path)}:")
    for r in data:
        name = r.get('name', 'Unknown')
        if 'Search' in name or 'DoPut' in name:
            qps = r.get('throughput', 0)
            p95 = r.get('p95_latency_ms', 0)
            mbs = r.get('throughput_mbs', 0)
            if 'DoPut' in name:
                print(f"  {name:25} | MB/s: {mbs:10.2f}")
            else:
                print(f"  {name:25} | QPS: {qps:10.2f} | P95: {p95:8.2f}ms")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        for pattern in sys.argv[1:]:
            for f in glob.glob(pattern):
                parse_results(f)
