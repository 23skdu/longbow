#!/bin/bash
# Comprehensive local benchmark
source venv/bin/activate
python3 scripts/full_matrix_runner.py metal local > local_run.log 2>&1 &
echo "Local benchmark started. Tail local_run.log to monitor."
