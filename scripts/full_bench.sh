#!/bin/bash

# Configuration
DTYPES=("float32" "float64" "float16" "int8" "int16" "int32" "int64" "uint8" "uint16" "uint32" "uint64" "complex64" "complex128" "turboquant2" "turboquant4" "turboquant8")
DIMS=(128 384 768 1024 3072)
SCALES=(1000 5000 10000 50000 100000)

SUFFIX=$1
if [ -z "$SUFFIX" ]; then
  SUFFIX="cpu"
fi

RESULTS_FILE="results_${SUFFIX}.json"
echo "[]" > $RESULTS_FILE

# Use the correct binary for the platform
SERVER_BIN="./bin/longbow"
if [ "$SUFFIX" == "metal" ]; then
  SERVER_BIN="./bin/longbow-metal"
elif [ "$SUFFIX" == "cuda" ]; then
  SERVER_BIN="./bin/longbow-cuda"
fi

# Allocate 18GB to Longbow
export LONGBOW_MAX_MEMORY=19327352832

mkdir -p logs
mkdir -p profiles

for dtype in "${DTYPES[@]}"; do
  TQ_BITS=4
  ACTUAL_DTYPE=$dtype
  if [[ "$dtype" == "turboquant"* ]]; then
    ACTUAL_DTYPE="turboquant"
    TQ_BITS=${dtype#turboquant}
  fi

  for dim in "${DIMS[@]}"; do
    for scale in "${SCALES[@]}"; do
      echo "--------------------------------------------------------"
      echo "[$(date)] Testing $dtype dim=$dim scale=$scale on $SUFFIX"
      echo "--------------------------------------------------------"
      
      # Robust cleanup: kill EVERYTHING using the ports or our binaries
      pkill -9 -f longbow 2>/dev/null
      pkill -9 -f bench-tool 2>/dev/null
      if [[ "$OSTYPE" == "darwin"* ]]; then
        lsof -ti:3000,3001,9090 | xargs kill -9 2>/dev/null
      else
        fuser -k 3000/tcp 3001/tcp 9090/tcp 2>/dev/null
      fi
      sleep 3

      # Clean data
      rm -rf data/
      
      # Start server in background
      LOG_FILE="logs/server_${dtype}_${dim}_${scale}.log"
      $SERVER_BIN > "$LOG_FILE" 2>&1 &
      SERVER_PID=$!
      
      # Wait for server to be ready
      READY=0
      for i in {1..30}; do
        if grep -q "Listening for Data gRPC connections" "$LOG_FILE"; then
          READY=1
          break
        fi
        if ! ps -p $SERVER_PID > /dev/null; then
          echo "Server CRASHED during startup"
          break
        fi
        sleep 1
      done
      
      if [ $READY -eq 0 ]; then
        echo "TIMEOUT waiting for server"
        kill -9 $SERVER_PID 2>/dev/null
        continue
      fi
      
      # Start CPU profiling in background (record for 10s during benchmark)
      curl -s "http://localhost:9090/debug/pprof/profile?seconds=10" > "profiles/cpu_${dtype}_${dim}_${scale}.prof" &
      PROF_PID=$!

      # Run bench-tool
      DATASET="bench_${dtype}_${dim}_${scale}_${SUFFIX}"
      ./bin/bench-tool -dataset "$DATASET" -dtype "$ACTUAL_DTYPE" -dim "$dim" -scale "$scale" -tq-bits "$TQ_BITS" -json "tmp_${SUFFIX}.json"
      
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
        new_results = json.load(f)
    for res in new_results:
        res['dtype'] = '$dtype'
        res['dim'] = $dim
        res['scale'] = $scale
        res['platform'] = '$SUFFIX'
    all_results.extend(new_results)
    with open('$RESULTS_FILE', 'w') as f:
        json.dump(all_results, f, indent=2)
"
        rm "tmp_${SUFFIX}.json"
      fi
      
      # Wait for CPU profile to finish
      wait $PROF_PID 2>/dev/null
      
      # Collect final heap profile
      curl -s http://localhost:9090/debug/pprof/heap > "profiles/heap_${dtype}_${dim}_${scale}.prof"
      
      # Stop server
      kill -9 $SERVER_PID 2>/dev/null
      
      # Check logs for errors
      if grep -Ei "error|panic|fatal" "$LOG_FILE" | grep -v "Ingestion queue is BACKPRESSURED" > /dev/null; then
        echo "!!! ERRORS FOUND IN LOG: $LOG_FILE !!!"
        grep -Ei "error|panic|fatal" "$LOG_FILE" | grep -v "Ingestion queue is BACKPRESSURED" | head -n 5
      fi
      
      sleep 2
    done
  done
done
