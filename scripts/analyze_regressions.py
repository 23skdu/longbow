import re
import sys

def parse_table(filename):
    data = {}
    with open(filename, 'r') as f:
        for line in f:
            if line.startswith('|') and '---' not in line and 'Mode' not in line:
                parts = [p.strip() for p in line.split('|')[1:-1]]
                if len(parts) >= 8:
                    try:
                        mode, dtype, dim, count = parts[0], parts[1], parts[2], parts[3]
                        ingest, search = parts[4], parts[6]
                        key = f"{mode}_{dtype}_{dim}_{count}"
                        data[key] = {
                            'ingest': float(ingest),
                            'search': float(search)
                        }
                    except ValueError:
                        pass
    return data

baseline = parse_table('docs/performance_0.2.0.md')
current = parse_table('docs/performance.md')

print("IMPROVEMENTS (>20% vs 0.2.0):")
print(f"{'Config':<40} | {'Metric':<10} | {'0.2.0':<10} | {'Current':<10} | {'Change'}")
print("-" * 80)
matches = 0
for key, cur_vals in current.items():
    if key in baseline:
        base_vals = baseline[key]
        for metric in ['ingest', 'search']:
            b = base_vals[metric]
            c = cur_vals[metric]
            if b > 0:
                change = (c - b) / b * 100
                if change >= 20:
                    matches += 1
                    if matches <= 10:
                        print(f"{key:<40} | {metric:<10} | {b:<10.0f} | {c:<10.0f} | +{change:.1f}%")
print(f"Total improvements >20%: {matches}")
