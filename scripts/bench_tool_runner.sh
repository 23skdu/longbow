#!/bin/bash
# Comprehensive bench-tool runner for all data types, dimensions, and search modes
# Supports both local (CPU/Metal) and remote (CPU/CUDA) execution



SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
BIN_DIR="$REPO_DIR/bin"
OUTPUT_DIR="${OUTPUT_DIR:-bench_results}"

# Default configuration
DTYPES=("float32" "float64" "float16" "int8" "int16" "int32" "int64" "uint8" "uint16" "uint32" "uint64" "complex64" "complex128" "turboquant2" "turboquant4" "turboquant8")
DIMS=(384)
COUNTS=(100000 250000 500000 1000000)
QUERIES=1000
URI="${URI:-127.0.0.1:4000}"
METRICS_URI="${METRICS_URI:-127.0.0.1:9095}"
HOST=$(hostname)
MODE="${MODE:-cpu}"

usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -m, --mode MODE       Execution mode: cpu, metal, cuda (default: cpu)"
    echo "  -u, --uri URI        Target URI (default: 127.0.0.1:3000)"
    echo "  -U, --metrics-uri URI Metrics URI (default: 127.0.0.1:9090)"
    echo "  -o, --output DIR     Output directory (default: bench_results)"
    echo "  -d, --dims DIMS     Comma-separated dimensions (default: 128,384,768,1024,3072)"
    echo "  -c, --counts COUNTS  Comma-separated counts (default: 1000,5000,10000,50000,100000)"
    echo "  -t, --types TYPES   Comma-separated data types"
    echo "  -q, --queries N     Number of queries (default: 1000)"
    echo "  -r, --remote HOST  Remote host for execution (skips local server start)"
    echo "  -h, --help         Show this help"
    echo ""
    echo "Examples:"
    echo "  $0 --mode cpu --dims 128,384 --counts 1000,5000"
    echo "  $0 --mode metal --dims 128 --counts 10000"
    echo "  REMOTE_HOST=ancalagon $0 --mode cuda --dims 128,384"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -m|--mode) MODE="$2"; shift 2 ;;
        -u|--uri) URI="$2"; shift 2 ;;
        -U|--metrics-uri) METRICS_URI="$2"; shift 2 ;;
        -o|--output) OUTPUT_DIR="$2"; shift 2 ;;
        -d|--dims) IFS=',' read -ra DIMS <<< "$2"; shift 2 ;;
        -c|--counts) IFS=',' read -ra COUNTS <<< "$2"; shift 2 ;;
        -t|--types) IFS=',' read -ra DTYPES <<< "$2"; shift 2 ;;
        -q|--queries) QUERIES="$2"; shift 2 ;;
        -r|--remote) REMOTE_HOST="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1"; usage; exit 1 ;;
    esac
done

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_DIR="$OUTPUT_DIR/${HOST}_${MODE}_${TIMESTAMP}"
mkdir -p "$OUTPUT_DIR"/{profiles,logs}

echo "========================================="
echo "Benchmark Configuration"
echo "========================================="
echo "Host: $HOST"
echo "Mode: $MODE"
echo "URI: $URI"
echo "Metrics: $METRICS_URI"
echo "Dimensions: ${DIMS[*]}"
echo "Counts: ${COUNTS[*]}"
echo "Data Types: ${DTYPES[*]}"
echo "Queries: $QUERIES"
echo "Output: $OUTPUT_DIR"
echo "========================================="

# Function to get bench-tool binary
get_bench_tool() {
    if [[ -x "$BIN_DIR/bench-tool" ]]; then
        echo "$BIN_DIR/bench-tool"
    elif [[ -x "$REPO_DIR/bin/bench-tool" ]]; then
        echo "$REPO_DIR/bin/bench-tool"
    elif command -v bench-tool &>/dev/null; then
        echo "bench-tool"
    else
        echo "ERROR: bench-tool not found. Run 'go build -o bin/bench-tool ./cmd/bench-tool'" >&2
        exit 1
    fi
}

BENCH_TOOL=$(get_bench_tool)
echo "Using bench-tool: $BENCH_TOOL"

# Pprof collection in background
start_pprof() {
    echo "Starting pprof collection..." >&2
    (
        while true; do
            ts=$(date +%H%M%S)
            curl -s "http://$METRICS_URI/debug/pprof/profile?seconds=30" -o "$OUTPUT_DIR/profiles/cpu_${ts}.prof" 2>/dev/null || true
            curl -s "http://$METRICS_URI/debug/pprof/heap" -o "$OUTPUT_DIR/profiles/heap_${ts}.prof" 2>/dev/null || true
            curl -s "http://$METRICS_URI/debug/pprof/goroutine?debug=1" -o "$OUTPUT_DIR/profiles/goroutine_${ts}.txt" 2>/dev/null || true
            sleep 60
        done
    ) > /dev/null 2>&1 &
    echo $!
}

## Global PID tracking
SERVER_PID=""
PPROF_PID=""

# Function to start the local Longbow server
start_local_server() {
    if [[ -z "$REMOTE_HOST" ]] && [[ "$URI" == "127.0.0.1"* ]]; then
        echo "Starting local longbow server for mode: $MODE"
        # Extract ports from URI and METRICS_URI
        PORT=$(echo "$URI" | cut -d: -f2)
        METRICS_PORT=$(echo "$METRICS_URI" | cut -d: -f2)
        META_PORT=$((PORT + 1))
        
        export LONGBOW_MAX_MEMORY=${LONGBOW_MAX_MEMORY:-19327352832} # Default 18GB
        export LONGBOW_AUTOSCALE_ENABLED=false
        export LONGBOW_TEMPORAL_ENABLED=true
        export LONGBOW_SPARSE_ENABLED=true
        export LONGBOW_GEOSPATIAL_ENABLED=true
        export LONGBOW_GRAPHRAG_ENABLED=true
        export LONGBOW_LEARNED_INDEX_ENABLED=true
        export LONGBOW_LISTEN_ADDR="0.0.0.0:$PORT"
        export LONGBOW_METRICS_ADDR="0.0.0.0:$METRICS_PORT"
        export LONGBOW_META_ADDR="0.0.0.0:$META_PORT"
        
        # Select binary based on mode
        case "$MODE" in
            metal) SERVER_BIN="$REPO_DIR/bin/longbow-metal" ;;
            cuda)  SERVER_BIN="$REPO_DIR/bin/longbow-cuda" ;;
            *)     
                if [[ -x "$REPO_DIR/bin/longbow-avx2" ]] && [[ "$(uname -m)" == "x86_64" ]]; then
                    SERVER_BIN="$REPO_DIR/bin/longbow-avx2"
                else
                    SERVER_BIN="$REPO_DIR/bin/longbow-cpu"
                fi
                ;;
        esac
        
        if [[ ! -x "$SERVER_BIN" ]]; then
            echo "ERROR: Server binary $SERVER_BIN not found!"
            exit 1
        fi
        
        # Set environment variables for server
        export LONGBOW_DATA_PATH="$OUTPUT_DIR/data"
        rm -rf "$LONGBOW_DATA_PATH"
        mkdir -p "$LONGBOW_DATA_PATH"
        
        export GOTRACEBACK=all
        nohup "$SERVER_BIN" > "$OUTPUT_DIR/logs/server_$(date +%H%M%S).log" 2>&1 &
        SERVER_PID=$!
        
        # Wait for server to be ready
        for i in {1..30}; do
            if curl -s "http://$URI/health" &>/dev/null || curl -s "http://$METRICS_URI/health" &>/dev/null; then
                echo "Server ready (PID: $SERVER_PID)"
                break
            fi
            sleep 1
        done
        
        # Start pprof for this instance
        PPROF_PID=$(start_pprof)
    fi
}

# Function to stop the local Longbow server
stop_local_server() {
    if [[ -n "$PPROF_PID" ]]; then
        kill $PPROF_PID 2>/dev/null || true
        PPROF_PID=""
    fi
    if [[ -n "$SERVER_PID" ]]; then
        echo "Stopping server (PID: $SERVER_PID)..."
        kill $SERVER_PID 2>/dev/null || true
        # Wait for it to actually stop and release resources
        for i in {1..10}; do
            if ! kill -0 $SERVER_PID 2>/dev/null; then
                break
            fi
            sleep 1
        done
        SERVER_PID=""
        # Aggressive cleanup of any orphaned data files
        rm -rf "$OUTPUT_DIR/data"
    fi
}

# Cleanup function for traps
cleanup() {
    echo "Emergency cleanup..."
    stop_local_server
}
trap cleanup SIGINT SIGTERM

# Run benchmarks
LOG_FILE="$OUTPUT_DIR/bench.log"
ERROR_LOG="$OUTPUT_DIR/errors.log"

for count in "${COUNTS[@]}"; do
    for dim in "${DIMS[@]}"; do
        for dtype_raw in "${DTYPES[@]}"; do
            # Restart server for EACH dataset to ensure 0MB heap baseline
            stop_local_server
            start_local_server

            dtype=$dtype_raw
            tq_bits=4

            # Handle turboquant special naming
            if [[ $dtype_raw == turboquant* ]]; then
                dtype="turboquant"
                tq_bits=${dtype_raw#turboquant}
            fi

            dataset="bench_${dtype_raw}_${dim}_${count}"
            json_out="$OUTPUT_DIR/${dataset}.json"

            echo "[$(date +%H:%M:%S)] Running: $dataset (dim=$dim, dtype=$dtype, bits=$tq_bits, count=$count)"

            SEARCH_MODES="all"
            WORKERS=4

            $BENCH_TOOL \
                -uri "$URI" \
                -dataset "$dataset" \
                -dim "$dim" \
                -dtype "$dtype" \
                -tq-bits $tq_bits \
                -scale $count \
                -queries $QUERIES \
                -workers $WORKERS \
                -search-modes "$SEARCH_MODES" \
                -json "$json_out" \
                -drop \
                >> "$LOG_FILE" 2>&1

            if [[ $? -ne 0 ]]; then
                echo "[ERROR] $dataset failed" | tee -a "$ERROR_LOG"
            else
                echo "[OK] $dataset completed"
            fi
        done
    done
done

# Final cleanup
stop_local_server

echo "Benchmark run completed at $(date)"
echo "Results saved to: $OUTPUT_DIR"