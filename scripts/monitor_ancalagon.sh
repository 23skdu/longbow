#!/bin/bash
INTERVAL=300
mkdir -p data/perf_logs
LOG="data/perf_logs/monitor.log"
while true; do
  echo "=== $(date) ===" | tee -a "$LOG"
  for f in data/perf_logs/bench_cpu_ancalagon.log data/perf_logs/bench_cuda_ancalagon.log; do
    if [ -f "$f" ]; then
      TOTAL=$(grep -c "vec/s" "$f" 2>/dev/null || echo 0)
      ERRORS=$(grep -ci "error\|fail\|exhausted\|panic" "$f" 2>/dev/null || echo 0)
      echo "$(basename $f): Tests=$TOTAL Errors=$ERRORS" | tee -a "$LOG"
      egrep -i "resourceexhausted|panic|oom|killed" "$f" | tail -2 >> "$LOG"
    fi
  done
  echo "---" >> "$LOG"
  sleep $INTERVAL
done
