#!/bin/bash
# Report benchmark status every 15 minutes
while true; do
  echo "========================================"
  date
  echo "========================================"

  echo "=== Localhost CPU ==="
  grep -c "vec/s" data/perf_logs/bench_cpu_localhost.log 2>/dev/null || echo "0"
  grep "vec/s" data/perf_logs/bench_cpu_localhost.log 2>/dev/null | tail -1

  echo "=== Localhost Metal ==="
  grep -c "vec/s" data/perf_logs/bench_metal_localhost.log 2>/dev/null || echo "0"
  grep "vec/s" data/perf_logs/bench_metal_localhost.log 2>/dev/null | tail -1

  echo "=== Ancalagon CPU ==="
  ssh -o ConnectTimeout=5 ancalagon 'grep -c "vec/s" ~/REPOS/longbow/data/perf_logs/bench_cpu_ancalagon.log 2>/dev/null || echo "0"' 2>/dev/null
  ssh -o ConnectTimeout=5 ancalagon 'grep "vec/s" ~/REPOS/longbow/data/perf_logs/bench_cpu_ancalagon.log 2>/dev/null | tail -1' 2>/dev/null

  echo "=== Ancalagon CUDA ==="
  ssh -o ConnectTimeout=5 ancalagon 'grep -c "vec/s" ~/REPOS/longbow/data/perf_logs/bench_cuda_ancalagon.log 2>/dev/null || echo "0"' 2>/dev/null

  echo "=== Errors (all hosts) ==="
  for f in data/perf_logs/bench_cpu_localhost.log data/perf_logs/bench_metal_localhost.log; do
    egrep -i "error|fail|panic|oom|exhausted" "$f" 2>/dev/null | head -3
  done
  ssh -o ConnectTimeout=5 ancalagon 'for f in ~/REPOS/longbow/data/perf_logs/bench_cpu_ancalagon.log ~/REPOS/longbow/data/perf_logs/bench_cuda_ancalagon.log; do if [ -f "$f" ]; then egrep -i "error|fail|panic|oom|exhausted" "$f" 2>/dev/null | head -3; fi; done' 2>/dev/null

  echo "-- $(date) -- end report"
  sleep 900
done
