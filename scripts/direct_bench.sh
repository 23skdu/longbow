#!/bin/bash

# Configuration
DTYPES=("float32" "float64" "float16" "int8" "int16" "int32" "int64" "uint8" "uint16" "uint32" "uint64" "complex64" "complex128" "turboquant2" "turboquant4" "turboquant8")
DIMS=(128 384 768 1024 3072)
SCALES=(500000 1000 5000 10000 50000 100000)

SUFFIX=$1
if [ -z "$SUFFIX" ]; then
  SUFFIX="direct"
fi

RESULTS_FILE="results_${SUFFIX}.json"
echo "[]" > $RESULTS_FILE

# Use the correct binary for the platform
SERVER_BIN="./bin/longbow"
if [ "$SUFFIX" == "cuda" ]; then
  SERVER_BIN="./bin/longbow-cuda"
fi

# Ensure max memory is set
export LONGBOW_MAX_MEMORY=19327352832

for dtype in "${DTYPES[@]}"; do
  for dim in "${DIMS[@]}"; do
    for scale in "${SCALES[@]}"; do
      echo "--------------------------------------------------------"
      echo "[$(date)] Testing $dtype dim=$dim scale=$scale"
      echo "--------------------------------------------------------"
      
      # Clean data and logs before starting server
      rm -rf data/ logs/ server.log
      
      # Start server in background
      $SERVER_BIN > server.log 2>&1 &
      SERVER_PID=$!
      
      # Wait for server to be ready (look for "Listening for Data gRPC connections")
      echo "Waiting for server to start..."
      MAX_WAIT=30
      COUNT=0
      while ! grep -q "Listening for Data gRPC connections" server.log; do
        sleep 1
        COUNT=$((COUNT+1))
        if [ $COUNT -ge $MAX_WAIT ]; then
          echo "TIMEOUT waiting for server to start"
          kill -9 $SERVER_PID
          continue 3
        fi
      done
      echo "Server is ready (PID: $SERVER_PID)"
      
      # Use unique dataset name
      DATASET="bench_${dtype}_${dim}_${scale}_${SUFFIX}"
      
      # Run bench-tool
      ./bin/bench-tool -dataset "$DATASET" -dtype "$dtype" -dim "$dim" -scale "$scale" -json "tmp_${SUFFIX}.json"
      
      if [ $? -eq 0 ]; then
        # Merge results
        python3 -c "
import json, os
try:
    with open('$RESULTS_FILE', 'r') as f:
        all_results = json.load(f)
except:
    all_results = []

if os.path.exists('tmp_${SUFFIX}.json'):
    with open('tmp_${SUFFIX}.json', 'r') as f:
        new_result = json.load(f)
    all_results.append(new_result)
    with open('$RESULTS_FILE', 'w') as f:
        json.dump(all_results, f, indent=2)
"
        rm "tmp_${SUFFIX}.json"
      else:
        echo "ERROR: Benchmark failed for $dtype $dim $scale"
      fi
      
      # Kill server and cleanup
      echo "Stopping server..."
      kill -9 $SERVER_PID
      wait $SERVER_PID 2>/dev/null
      rm -rf data/
    done
  done
done

echo "Benchmarks completed. Results saved to $RESULTS_FILE"
