#!/bin/bash
# Monitor all benchmark progress
INTERVAL=300  # 5 minutes
LOG="data/perf_logs/monitor.log"

while true; do
  echo "=== $(date) ===" | tee -a "$LOG"
  
  echo "=== Local CPU ===" | tee -a "$LOG"
  if [ -f data/perf_logs/bench_cpu_localhost.log ]; then
    TOTAL=$(grep -c "vec/s" data/perf_logs/bench_cpu_localhost.log 2>/dev/null || echo 0)
    ERRORS=$(grep -ci "error\|fail\|exhausted\|panic" data/perf_logs/bench_cpu_localhost.log 2>/dev/null || echo 0)
    echo "Tests done: $TOTAL, Errors: $ERRORS" | tee -a "$LOG"
  fi
  
  echo "=== Local Metal ===" | tee -a "$LOG"
  if [ -f data/perf_logs/bench_metal_localhost.log ]; then
    TOTAL=$(grep -c "vec/s" data/perf_logs/bench_metal_localhost.log 2>/dev/null || echo 0)
    ERRORS=$(grep -ci "error\|fail\|exhausted\|panic" data/perf_logs/bench_metal_localhost.log 2>/dev/null || echo 0)
    echo "Tests done: $TOTAL, Errors: $ERRORS" | tee -a "$LOG"
  fi
  
  # Check errors in detail
  for f in data/perf_logs/bench_cpu_localhost.log data/perf_logs/bench_metal_localhost.log; do
    if [ -f "$f" ]; then
      egrep -i "resourceexhausted|panic|oom|killed" "$f" | tail -3 >> "$LOG"
    fi
  done
  
  echo "---" >> "$LOG"
  sleep $INTERVAL
done
