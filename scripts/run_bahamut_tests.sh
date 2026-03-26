#!/bin/bash
set -e

MEMORY="${1:-21474836480}"
LOG_DIR="/Users/rsd/REPOS/longbow/data/perf_logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/bahamut_${TIMESTAMP}.log"

DIMS=(128 384)
DTYPES=(float32 float64 int8 int16 int32 uint32 complex64 complex128)
COUNTS=(1000 5000 10000 25000)

TOTAL=64
echo "======================================================================"
echo "FULL PERFORMANCE MATRIX (macOS, arm64, ${MEMORY} bytes)"
echo "Started: $(date)"
echo "Log: $LOG_FILE"
echo "======================================================================"

run_num=0
for dim in "${DIMS[@]}"; do
    for dtype in "${DTYPES[@]}"; do
        for count in "${COUNTS[@]}"; do
            run_num=$((run_num + 1))
            dataset="bench_${dtype}_${dim}_${count}"
            echo ""
            echo "[${run_num}/${TOTAL}] ${dtype} dim=${dim} count=${count}"
            
            # Clean up old data and restart server
            pkill -9 longbow 2>/dev/null || true
            sleep 2
            rm -rf /Users/rsd/REPOS/longbow/data/bench /Users/rsd/REPOS/longbow/data/snapshots /Users/rsd/REPOS/longbow/data/wal.log
            mkdir -p /Users/rsd/REPOS/longbow/data/bench
            
            LONGBOW_MAX_MEMORY="$MEMORY" ARROW_DISABLE_LOCKING=1 \
                ./bin/longbow --listen-addr 127.0.0.1:3000 --data-path /Users/rsd/REPOS/longbow/data/bench --node-id bench1 \
                >> "$LOG_FILE" 2>&1 &
            sleep 5
            
            # Run benchmark
            if ./bin/benchmark-tool --uri=127.0.0.1:3000 --dim="$dim" --dtype="$dtype" --scale="$count" --queries=200 --dataset="$dataset" \
                --json="$LOG_DIR/result_${dtype}_${dim}_${count}.json" >> "$LOG_FILE" 2>&1; then
                echo "  DONE"
            else
                echo "  FAILED (check $LOG_FILE)"
            fi
        done
    done
done

pkill -9 longbow 2>/dev/null || true
echo ""
echo "======================================================================"
echo "ALL DONE — $(date)"
echo "Log: $LOG_FILE"
echo "======================================================================"
