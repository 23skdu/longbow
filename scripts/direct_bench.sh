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
      
      # Ensure ports are free
      if [[ "$OSTYPE" == "darwin"* ]]; then
        lsof -ti:3000,3001,9090 | xargs kill -9 2>/dev/null
      else
        fuser -k 3000/tcp 3001/tcp 9090/tcp 2>/dev/null
      fi
      sleep 2

      # Clean data and logs before starting server
      rm -rf data/ logs/
      
      # Start server in background with unique log for failure analysis
      LOG_FILE="logs/server_${dtype}_${dim}_${scale}.log"
      mkdir -p logs
      $SERVER_BIN > "$LOG_FILE" 2>&1 &
      SERVER_PID=$!
      
      # Wait for server to be ready
      echo "Waiting for server to start..."
      MAX_WAIT=30
      COUNT=0
      READY=0
      while [ $COUNT -lt $MAX_WAIT ]; do
        if grep -q "Listening for Data gRPC connections" "$LOG_FILE"; then
          READY=1
          break
        fi
        if ! ps -p $SERVER_PID > /dev/null; then
          echo "Server CRASHED during startup (see $LOG_FILE)"
          break
        fi
        sleep 1
        COUNT=$((COUNT+1))
      done
      
      if [ $READY -eq 0 ]; then
        echo "TIMEOUT or CRASH waiting for server to start"
        kill -9 $SERVER_PID 2>/dev/null
        continue
      fi
      
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
        # Success! Remove the server log to save space, unless it's a huge run we want to keep
        if [ "$scale" -lt 500000 ]; then
            rm "$LOG_FILE"
        fi
      else
        echo "ERROR: Benchmark failed for $dtype $dim $scale (see $LOG_FILE)"
      fi
      
      # Kill server and cleanup
      echo "Stopping server..."
      kill -9 $SERVER_PID 2>/dev/null
      wait $SERVER_PID 2>/dev/null
      rm -rf data/
      sleep 1
    done
  done
done

echo "Benchmarks completed. Results saved to $RESULTS_FILE"
