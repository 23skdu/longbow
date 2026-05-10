#!/bin/bash
# Targeted verification of temporal search optimization

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="$REPO_DIR/bin"
OUTPUT_DIR="$REPO_DIR/bench_results/verification_temporal_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTPUT_DIR"

URI="127.0.0.1:4000"
METRICS_URI="127.0.0.1:9095"
PORT=4000
METRICS_PORT=9095
META_PORT=4001

echo "Starting verification server with 24GB memory limit..."
export LONGBOW_MAX_MEMORY=25769803776 # 24GB
export LONGBOW_TEMPORAL_ENABLED=true
export LONGBOW_LISTEN_ADDR="0.0.0.0:$PORT"
export LONGBOW_METRICS_ADDR="0.0.0.0:$METRICS_PORT"
export LONGBOW_META_ADDR="0.0.0.0:$META_PORT"
export LONGBOW_DATA_PATH="$OUTPUT_DIR/data"
export GOTRACEBACK=all

mkdir -p "$LONGBOW_DATA_PATH"

"$BIN_DIR/longbow-cpu" > "$OUTPUT_DIR/server.log" 2>&1 &
SERVER_PID=$!

cleanup() {
    echo "Stopping server (PID: $SERVER_PID)..."
    kill $SERVER_PID
    wait $SERVER_PID 2>/dev/null
}
trap cleanup EXIT

echo "Waiting for server to be ready..."
for i in {1..30}; do
    if curl -s "http://$URI/health" &>/dev/null || curl -s "http://$METRICS_URI/health" &>/dev/null; then
        echo "Server is ready"
        break
    fi
    sleep 1
done

echo "Running targeted temporal benchmark (25k vectors, 3072 dimensions)..."
"$BIN_DIR/bench-tool" \
    -uri "$URI" \
    -dataset "verify_temporal_25k" \
    -dim 3072 \
    -dtype "float32" \
    -scale 25000 \
    -queries 1000 \
    -workers 4 \
    -search-modes "temporal" \
    -json "$OUTPUT_DIR/verify_results.json" \
    -drop

echo "Benchmark complete."
echo "Results:"
cat "$OUTPUT_DIR/verify_results.json"
