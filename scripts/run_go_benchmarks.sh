#!/bin/bash
# run_go_benchmarks.sh

DIMS=(128 384)
COUNTS=(1000 3000)
DTYPES=("float32" "int32" "uint32" "complex64" "int16" "int8" "float64" "complex128")

OUTPUT_FILE="benchmark_results.log"
echo "Starting Clean Isolated Benchmarks" > $OUTPUT_FILE
echo "===================" >> $OUTPUT_FILE

go build -o bin/benchmark-tool ./benchmark_tool

URI="127.0.0.1"
PORT=3000

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
      PORT=$((PORT + 1))
      LONGBOW_MAX_MEMORY=21474836480 ./bin/longbow -port $PORT > server_single.log 2>&1 &
      SERVER_PID=$!
      sleep 5 # Wait fully to bind and prewarm

      # B. Run test
      if [ "$gcount" -eq 20000 ]; then
         (sleep 7 && curl -s http://127.0.0.1:6060/debug/pprof/profile?seconds=5 > cpu_${dtype}_${dim}_20k.prof) &
      fi

      ./bin/benchmark-tool -uri "$URI:$PORT" -dtype "$dtype" -dim "$dim" -scale "$gcount" -queries 10 -dataset "$DS" -json "results_${dtype}_${dim}_${gcount}.json" 2>&1 >> $OUTPUT_FILE
      
      # C. Kill Server for fresh state
      kill $SERVER_PID 2>/dev/null || true
      echo "Waiting for server PID $SERVER_PID to exit..."
      wait $SERVER_PID 2>/dev/null || true
      sleep 1 # Residual port clear safety

      echo "-------------------" >> $OUTPUT_FILE
    done
  done
done

echo "Benchmarks complete" | tee -a $OUTPUT_FILE
