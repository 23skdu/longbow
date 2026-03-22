#!/bin/zsh
set -e
TIMEOUT=900
URI="127.0.0.1:3000"
LOG_DIR="data/perf_logs/bahamut"

# Missing tests
typeset -a MISSING_TESTS
MISSING_TESTS=(
  "int8:384:25000"
  "int16:384:1000"
  "int16:384:5000"
  "int16:384:10000"
  "int16:384:25000"
  "int32:384:1000"
  "int32:384:5000"
  "int32:384:10000"
  "int32:384:25000"
  "uint32:384:1000"
  "uint32:384:5000"
  "uint32:384:10000"
  "uint32:384:25000"
  "complex64:384:1000"
  "complex64:384:5000"
  "complex64:384:10000"
  "complex64:384:25000"
  "complex128:384:1000"
  "complex128:384:5000"
  "complex128:384:10000"
  "complex128:384:25000"
)

restart_server() {
  pkill -f longbow 2>/dev/null || true
  sleep 2
  rm -rf data/wal.log data/snapshots data/bench
  mkdir -p data/bench
  LONGBOW_MAX_MEMORY=21474836480 ARROW_DISABLE_LOCKING=1 nohup ./bin/longbow --listen-addr 127.0.0.1:3000 --data-path data/bench --node-id bench1 > /tmp/longbow_run.log 2>&1 &
  SERVER_PID=$!
  sleep 4
  echo "  Server restarted (pid=$SERVER_PID)"
}

run_test() {
  local dtype=$1 dim=$2 count=$3
  local dataset="bench_${dtype}_${dim}_${count}"
  local json_file="${LOG_DIR}/result_${dtype}_${dim}_${count}.json"
  
  echo "  Running ${dtype} dim=${dim} count=${count}..."
  timeout $TIMEOUT ./bin/benchmark-tool \
    --uri="$URI" \
    --dim="$dim" \
    --dtype="$dtype" \
    --scale="$count" \
    --queries=200 \
    --dataset="$dataset" \
    --json="$json_file" \
    > "/tmp/bench_${dtype}_${dim}_${count}.log" 2>&1
  
  if [ $? -eq 0 ] && [ -f "$json_file" ]; then
    doput=$(grep "DoPut" "/tmp/bench_${dtype}_${dim}_${count}.log" | awk '{print $2}' | tr -d ',' | head -1)
    dense=$(grep "Search_Dense" "/tmp/bench_${dtype}_${dim}_${count}.log" | awk '{print $2}' | tr -d ',' | head -1)
    idx=$(grep "Indexing" "/tmp/bench_${dtype}_${dim}_${count}.log" | grep -oP '[\d.]+(?=s)' | head -1)
    echo "    -> DoPut: ${doput:-?} vec/s | Dense: ${dense:-?} QPS | Index: ${idx:-?}s"
  else
    echo "    -> FAILED or TIMEOUT (check /tmp/bench_${dtype}_${dim}_${count}.log)"
  fi
}

TOTAL=${#MISSING_TESTS[@]}
for i in {1..$TOTAL}; do
  test_spec="${MISSING_TESTS[$i]}"
  dtype=$(echo $test_spec | cut -d: -f1)
  dim=$(echo $test_spec | cut -d: -f2)
  count=$(echo $test_spec | cut -d: -f3)
  
  echo ""
  echo "=== [${i}/${TOTAL}] ${dtype} dim=${dim} count=${count} ==="
  restart_server
  run_test "$dtype" "$dim" "$count"
done

echo ""
echo "=== ALL MISSING TESTS DONE ==="
