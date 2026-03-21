#!/bin/bash
# run_go_benchmarks.sh

DIMS=(128 384)
COUNTS=(1000 3000 5000 10000 15000)
DTYPES=("float32" "int32" "uint32" "complex64")

OUTPUT_FILE="benchmark_results.log"
echo "Starting Clean Isolated Benchmarks" > $OUTPUT_FILE
echo "===================" >> $OUTPUT_FILE

go build -o bin/benchmark_tool ./benchmark_tool

URI="127.0.0.1:3000"

# 1. Start continuous server once
echo "Waiting for port 3000 to clear..."
while nc -z 127.0.0.1 3000; do
  sleep 1
done

for dtype in "${DTYPES[@]}"; do
  for dim in "${DIMS[@]}"; do
    for gcount in "${COUNTS[@]}"; do
      echo "Running $dtype dim=$dim scale=$gcount" | tee -a $OUTPUT_FILE
      DS="bench_${dtype}_${dim}_${gcount}"

      # A. Start Isolated Server
      rm -rf ./data
      LONGBOW_MAX_MEMORY=21474836480 ./bin/longbow > server_single.log 2>&1 &
      SERVER_PID=$!
      sleep 5 # Wait fully to bind and prewarm

      # B. Run test
      ./bin/benchmark_tool -uri "$URI" -dtype "$dtype" -dim "$dim" -scale "$gcount" -queries 10 -dataset "$DS" -json "results_${dtype}_${dim}_${gcount}.json" 2>&1 | grep -E "Dataset|DoPut|DoGet|dense|sparse|filtered|hybrid|BENCHMARK|Throughput" >> $OUTPUT_FILE
      
      # C. Kill Server for fresh state
      kill $SERVER_PID 2>/dev/null || true
      sleep 2 # Let port clear

      echo "-------------------" >> $OUTPUT_FILE
    done
  done
done

echo "Benchmarks complete" | tee -a $OUTPUT_FILE
