#!/bin/bash
set -e

# Benchmark script for full dimension matrix (128-3072)
# Uses smaller batches for higher dimensions to fit memory

BINARY="${1:-./bin/longbow}"
BENCHMARK="${2:-./bin/benchmark-tool}"
URI="${3:-127.0.0.1:3000}"

# Dimensions to test
DIMS=(128 256 384 512 768 1024 1536 2048 3072)

# Data types to test
DTYPES=(float32 float64 int8 int16 int32 int64 uint8 uint16 uint32 uint64 complex64 complex128 turboquant)

# Scale based on dimension (higher dims = smaller scale)
get_scale() {
    local dim=$1
    case $dim in
        128) echo "5000" ;;
        256) echo "3000" ;;
        384) echo "2000" ;;
        512) echo "1500" ;;
        768) echo "1000" ;;
        1024) echo "800" ;;
        1536) echo "500" ;;
        2048) echo "300" ;;
        3072) echo "200" ;;
        *) echo "1000" ;;
    esac
}

# Queries (smaller for high dims)
get_queries() {
    local dim=$1
    if [ $dim -ge 1024 ]; then
        echo "100"
    else
        echo "200"
    fi
}

# Start server in background if not running
start_server() {
    if ! curl -s http://localhost:9090/metrics > /dev/null 2>&1; then
        echo "Starting server..."
        $BINARY > /tmp/longbow.log 2>&1 &
        sleep 10
    fi
}

# Stop and clean server
cleanup() {
    pkill -9 longbow 2>/dev/null || true
    sleep 2
    rm -rf /tmp/longbow*.data 2>/dev/null || true
}

# Run a single benchmark
run_bench() {
    local dim=$1
    local dtype=$2
    local scale=$3
    local queries=$4
    local dataset="perf_${dtype}_${dim}"
    
    echo "Running: dim=$dim dtype=$dtype scale=$scale queries=$queries"
    
    $BENCHMARK \
        -uri="$URI" \
        -dim=$dim \
        -scale=$scale \
        -dtype=$dtype \
        -queries=$queries \
        -dataset="$dataset" 2>&1 | tail -10
}

# Main execution
echo "=== Full Dimension Benchmark Matrix ==="
echo "Binary: $BINARY"
echo "Benchmark: $BENCHMARK"
echo ""

# Cleanup first
cleanup
start_server

# Run benchmarks
for dim in "${DIMS[@]}"; do
    scale=$(get_scale $dim)
    queries=$(get_queries $dim)
    
    echo "=== Dimension: $dim (scale=$scale, queries=$queries) ==="
    
    for dtype in "${DTYPES[@]}"; do
        run_bench $dim $dtype $scale $queries
    done
    
    echo ""
done

echo "=== Benchmark Complete ==="
