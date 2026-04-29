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

# Ensure max memory is set for the in-process server in bench-tool
export LONGBOW_MAX_MEMORY=19327352832

for dtype in "${DTYPES[@]}"; do
  for dim in "${DIMS[@]}"; do
    for scale in "${SCALES[@]}"; do
      echo "--------------------------------------------------------"
      echo "[$(date)] Testing $dtype dim=$dim scale=$scale"
      echo "--------------------------------------------------------"
      
      # Use unique dataset name to avoid schema collisions and lingering data
      DATASET="bench_${dtype}_${dim}_${scale}_${SUFFIX}"
      
      # Run bench-tool directly
      ./bin/bench-tool -dataset "$DATASET" -dtype "$dtype" -dim "$dim" -scale "$scale" -json "tmp_${SUFFIX}.json"
      
      if [ $? -eq 0 ]; then
        # Merge results into main file using python (simple append logic)
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
      
      # Small cleanup between runs to ensure memory is released and disk space is managed
      rm -rf "data/$DATASET"
    done
  done
done

echo "Benchmarks completed. Results saved to $RESULTS_FILE"
