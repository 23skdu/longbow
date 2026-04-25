import re, json, glob

# Extract metrics from go file
with open('internal/metrics/metrics.go') as f:
    go_content = f.read()

# Match Name: "metric_name"
metrics = set(re.findall(r'Name:\s*"([^"]+)"', go_content))
print(f"Found {len(metrics)} metrics in Go code.")

# Extract metrics from dashboards
dash_metrics = set()
for file in glob.glob('grafana/dashboards/*.json'):
    with open(file) as f:
        content = f.read()
    # Simple regex to find words starting with longbow_
    found = re.findall(r'longbow_[a-zA-Z0-9_]+', content)
    dash_metrics.update(found)

missing = sorted(metrics - dash_metrics)
print(f"Missing {len(missing)} metrics in dashboards:")
for m in missing:
    print(" - " + m)

