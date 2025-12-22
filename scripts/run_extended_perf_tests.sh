#!/bin/bash
# Extended Performance Test Suite
# Runs comprehensive benchmarks as requested

set -e

DATA_URI="grpc://localhost:3000"
META_URI="grpc://localhost:3001"
RESULTS_DIR="/tmp/longbow_extended_perf"

mkdir -p "$RESULTS_DIR"

echo "========================================="
echo "LONGBOW EXTENDED PERFORMANCE TEST SUITE"
echo "========================================="
echo ""

# Test 1: 100K vectors, dim=128, with warmup
echo "Test 1: Extended Search Benchmark (100K vectors, dim=128)"
echo "-----------------------------------------------------------"
python3 perf_test.py \
  --data-uri "$DATA_URI" \
  --meta-uri "$META_URI" \
  --rows 100000 \
  --dim 128 \
  --name "extended_100k_d128" \
  --search \
  --search-k 10 \
  --query-count 1000 \
  --check-cluster \
  --json "$RESULTS_DIR/test1_100k_d128.json" \
  2>&1 | tee "$RESULTS_DIR/test1_100k_d128.log"

echo ""
echo "Test 2: Large Dimension Vectors (OpenAI 1536-dim)"
echo "-----------------------------------------------------------"
python3 perf_test.py \
  --data-uri "$DATA_URI" \
  --meta-uri "$META_URI" \
  --rows 50000 \
  --dim 1536 \
  --name "large_dim_1536" \
  --search \
  --search-k 10 \
  --query-count 500 \
  --check-cluster \
  --json "$RESULTS_DIR/test2_50k_d1536.json" \
  2>&1 | tee "$RESULTS_DIR/test2_50k_d1536.log"

echo ""
echo "Test 3: Very Large Dimension Vectors (3072-dim)"
echo "-----------------------------------------------------------"
python3 perf_test.py \
  --data-uri "$DATA_URI" \
  --meta-uri "$META_URI" \
  --rows 25000 \
  --dim 3072 \
  --name "large_dim_3072" \
  --search \
  --search-k 10 \
  --query-count 250 \
  --check-cluster \
  --json "$RESULTS_DIR/test3_25k_d3072.json" \
  2>&1 | tee "$RESULTS_DIR/test3_25k_d3072.log"

echo ""
echo "Test 4: Delete Operations Benchmark"
echo "-----------------------------------------------------------"
python3 perf_test.py \
  --data-uri "$DATA_URI" \
  --meta-uri "$META_URI" \
  --rows 50000 \
  --dim 128 \
  --name "delete_test" \
  --delete \
  --delete-count 10000 \
  --check-cluster \
  --json "$RESULTS_DIR/test4_delete.json" \
  2>&1 | tee "$RESULTS_DIR/test4_delete.log"

echo ""
echo "========================================="
echo "ALL TESTS COMPLETE"
echo "========================================="
echo "Results saved to: $RESULTS_DIR"
echo ""
ls -lh "$RESULTS_DIR"
