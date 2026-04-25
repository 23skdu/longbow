#!/bin/bash
# Aggregate remote results and generate documentation
mkdir -p data/perf_logs
scp -r ancalagon:REPOS/longbow/data/perf_logs/*.json data/perf_logs/
source venv/bin/activate
python3 scripts/generate_perf_doc.py
echo "docs/performance.md updated with latest results from both hosts."
