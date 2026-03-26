#!/bin/bash
# Metal GPU Benchmark Matrix
# DTypes: float32, float64, int8, int16, int32, uint32, complex64, complex128
# Dimensions: 128, 384
# Counts: 1000, 5000, 10000, 25000

cd /Users/rsd/REPOS/longbow
rm -f data/wal.log

OUTPUT_DIR="data/perf_logs_metal"
mkdir -p "$OUTPUT_DIR"

DTYPES=("float32" "float64" "int8" "int16" "int32" "uint32" "complex64" "complex128")
DIMS=(128 384)
COUNTS=(1000 5000 10000 25000)

URI="127.0.0.1:3000"

echo "Starting Metal GPU benchmark matrix..."
total=0
completed=0

for dtype in "${DTYPES[@]}"; do
  for dim in "${DIMS[@]}"; do
    for count in "${COUNTS[@]}"; do
      total=$((total + 1))
      output_file="$OUTPUT_DIR/result_${dtype}_${dim}_${count}.json"
      
      # Skip if already exists
      if [ -f "$output_file" ]; then
        echo "[$total/64] Skipping ${dtype}_${dim}_${count} (exists)"
        completed=$((completed + 1))
        continue
      fi
      
      echo "[$total/64] Testing ${dtype}_${dim}_${count}..."
      
      # Run benchmark with 120s timeout
      timeout 120 ./bin/bench-go \
        --dataset "metal_${dtype}_${dim}_${count}" \
        --scale "$count" \
        --dim "$dim" \
        --dtype "$dtype" \
        --uri "$URI" \
        --json "$output_file" 2>/dev/null
      
      if [ $? -eq 0 ] && [ -f "$output_file" ]; then
        echo "  ✓ Completed"
        completed=$((completed + 1))
      else
        echo "  ✗ Failed or timeout"
      fi
      
      # Small delay between tests
      sleep 0.5
    done
  done
done

echo ""
echo "========================================"
echo "Metal GPU Benchmark Complete"
echo "Completed: $completed/64"
echo "Results: $OUTPUT_DIR"
echo "========================================"