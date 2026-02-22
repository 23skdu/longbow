#!/bin/bash
set -e

# io_uring Performance Comparison Script
# Compares WAL performance with and without io_uring

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${PROJECT_DIR}/reports"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "=============================================="
echo "io_uring Performance Comparison"
echo "=============================================="
echo "Timestamp: $TIMESTAMP"
echo "Output: $OUTPUT_DIR"
echo ""

mkdir -p "$OUTPUT_DIR"

# Function to run WAL backend benchmark
run_wal_benchmark() {
    local use_iouring=$1
    local output_file=$2
    
    echo "Running WAL benchmarks with io_uring=$use_iouring..."
    
    # Run only the WAL benchmark tests
    if [ "$use_iouring" = "true" ]; then
        # Run both standard and io_uring benchmarks
        cd "$PROJECT_DIR"
        go test -v -bench=BenchmarkWAL -run=^$ ./internal/storage/benchmark/... 2>&1 | tee "$output_file"
    else
        # Run only standard benchmark
        cd "$PROJECT_DIR"
        go test -v -bench=BenchmarkWALStandard -run=^$ ./internal/storage/benchmark/... 2>&1 | tee "$output_file"
    fi
}

# Function to run integration tests with specific io_uring setting
run_integration_test() {
    local use_iouring=$1
    local output_file=$2
    
    echo "Running integration tests with io_uring=$use_iouring..."
    
    cd "$PROJECT_DIR"
    if [ "$use_iouring" = "true" ]; then
        LONGBOW_STORAGE_USE_IOURING=true go test -v -run=TestWALPerformance ./internal/storage/benchmark/... 2>&1 | tee "$output_file"
    else
        LONGBOW_STORAGE_USE_IOURING=false go test -v -run=TestWALPerformance ./internal/storage/benchmark/... 2>&1 | tee "$output_file"
    fi
}

# Function to run comprehensive benchmark suite
run_benchmark_suite() {
    local use_iouring=$1
    local suffix=$2
    
    echo ""
    echo "=============================================="
    echo "Running Benchmark Suite: io_uring=$use_iouring"
    echo "=============================================="
    
    # Clean up any existing data
    rm -rf "${PROJECT_DIR}/data"
    mkdir -p "${PROJECT_DIR}/data"
    
    # Build with appropriate settings
    cd "$PROJECT_DIR"
    echo "Building Longbow..."
    go build -o bin/longbow ./cmd/longbow
    
    # Start a single node for testing
    echo "Starting Longbow node with io_uring=$use_iouring..."
    
    export LONGBOW_LISTEN_ADDR=127.0.0.1:3000
    export LONGBOW_META_ADDR=127.0.0.1:3001
    export LONGBOW_METRICS_ADDR=127.0.0.1:9090
    export LONGBOW_GOSSIP_PORT=7946
    export LONGBOW_GOSSIP_ENABLED=false
    export LONGBOW_MAX_MEMORY=$((4 * 1024 * 1024 * 1024))  # 4GB
    export LONGBOW_DATA_PATH="${PROJECT_DIR}/data/node1"
    export LONGBOW_NODE_ID="node1"
    export LONGBOW_STORAGE_USE_IOURING="$use_iouring"
    
    # Start server in background
    ./bin/longbow > "${OUTPUT_DIR}/node_${suffix}.log" 2>&1 &
    SERVER_PID=$!
    
    echo "Server started with PID $SERVER_PID"
    echo "Waiting for server to be ready..."
    sleep 10
    
    # Run performance test
    echo "Running performance tests..."
    
    # Test different vector sizes
    SIZES=(1000 5000 10000)
    DIM=384
    
    for SIZE in "${SIZES[@]}"; do
        echo "Testing with $SIZE vectors (dim=$DIM)..."
        
        REPORT_FILE="${OUTPUT_DIR}/report_${suffix}_${SIZE}.json"
        
        python3 scripts/perf_test.py \
            --data-uri grpc://127.0.0.1:3000 \
            --meta-uri grpc://127.0.0.1:3001 \
            --rows $SIZE \
            --dim $DIM \
            --dataset "bench_${suffix}_${SIZE}" \
            --json "$REPORT_FILE" 2>&1 || echo "Test failed for size $SIZE"
        
        sleep 2
    done
    
    # Stop server
    echo "Stopping server..."
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
    
    echo "Benchmark suite complete for io_uring=$use_iouring"
}

# Run comparison
echo "Starting comparison tests..."
echo ""

# 1. Run Go WAL-specific benchmarks
echo "=============================================="
echo "Phase 1: Go WAL Backend Benchmarks"
echo "=============================================="

echo "Running WITHOUT io_uring..."
run_wal_benchmark "false" "${OUTPUT_DIR}/wal_bench_standard_${TIMESTAMP}.log"

echo ""
echo "Running WITH io_uring..."
run_wal_benchmark "true" "${OUTPUT_DIR}/wal_bench_iouring_${TIMESTAMP}.log"

# 2. Run full benchmark suite comparison
echo ""
echo "=============================================="
echo "Phase 2: Full Application Benchmarks"
echo "=============================================="

run_benchmark_suite "false" "standard"
run_benchmark_suite "true" "iouring"

# Generate comparison report
echo ""
echo "=============================================="
echo "Generating Comparison Report"
echo "=============================================="

cat > "${OUTPUT_DIR}/comparison_report_${TIMESTAMP}.md" << 'EOF'
# io_uring Performance Comparison Report

Generated: TIMESTAMP

## Test Configuration
- Platform: Linux (io_uring requires Linux)
- Test Scales: 1000, 5000, 10000 vectors
- Vector Dimension: 384
- Memory Limit: 4GB per node

## Results

### WAL Backend Benchmarks

See log files:
- Standard: wal_bench_standard_TIMESTAMP.log
- io_uring: wal_bench_iouring_TIMESTAMP.log

### Application Benchmarks

Report files generated:
EOF

# List generated report files
for file in "${OUTPUT_DIR}"/report_*.json; do
    if [ -f "$file" ]; then
        echo "- $(basename "$file")" >> "${OUTPUT_DIR}/comparison_report_${TIMESTAMP}.md"
    fi
done

cat >> "${OUTPUT_DIR}/comparison_report_${TIMESTAMP}.md" << 'EOF'

## Summary

Compare the results by examining:
1. Throughput (vectors/second)
2. Latency percentiles (P50, P95, P99)
3. Memory usage
4. CPU utilization

### Key Metrics to Compare

- **Ingestion Throughput**: Higher is better
- **Search Latency**: Lower is better
- **Resource Usage**: Lower CPU/memory for same throughput is better

## Notes

- io_uring is Linux-specific and requires kernel 5.1+
- Benefits are most visible with high-throughput I/O workloads
- WAL (Write-Ahead Logging) performance impacts overall ingestion speed
EOF

# Replace timestamp in report
sed -i "s/TIMESTAMP/${TIMESTAMP}/g" "${OUTPUT_DIR}/comparison_report_${TIMESTAMP}.md"

echo ""
echo "=============================================="
echo "Comparison Complete!"
echo "=============================================="
echo "Results saved to: ${OUTPUT_DIR}/"
echo ""
echo "Files generated:"
ls -lh "${OUTPUT_DIR}"/*${TIMESTAMP}* 2>/dev/null || echo "No timestamped files found"
echo ""
echo "To compare results, examine:"
echo "1. ${OUTPUT_DIR}/wal_bench_standard_${TIMESTAMP}.log"
echo "2. ${OUTPUT_DIR}/wal_bench_iouring_${TIMESTAMP}.log"
echo "3. ${OUTPUT_DIR}/report_standard_*.json"
echo "4. ${OUTPUT_DIR}/report_iouring_*.json"
echo "5. ${OUTPUT_DIR}/comparison_report_${TIMESTAMP}.md"
