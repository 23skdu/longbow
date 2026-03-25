#!/bin/bash
set -e
export LONGBOW_MAX_MEMORY=12884901888
export ARROW_DISABLE_LOCKING=1
export GOGC=100

URI="127.0.0.1:3000"
LOG_DIR="/home/rsd/REPOS/longbow/data/perf_logs/ancalagon_$(date +%Y%m%d_%H%M%S)"
REPO_DIR="/home/rsd/REPOS/longbow"
BIN_DIR="${REPO_DIR}/bin"
DATA_DIR="${REPO_DIR}/data/bench"

DIMS=(128 384)
DTYPES=(float32 float64 int8 int16 int32 uint32 complex64 complex128)
COUNTS=(1000 5000 10000 25000)
TOTAL=64

mkdir -p "$LOG_DIR"

echo "=============================================="
echo "FULL PERFORMANCE MATRIX — ancalagon (Linux AVX2)"
echo "Started: $(date)"
echo "Memory: $((LONGBOW_MAX_MEMORY / 1024**3))GB"
echo "Total configs: $TOTAL"
echo "Output: $LOG_DIR"
echo "=============================================="

restart_server() {
    pkill -f longbow 2>/dev/null || true
    sleep 2
    rm -rf "${REPO_DIR}/data/wal.log" "${REPO_DIR}/data/snapshots" "$DATA_DIR" "${REPO_DIR}/data/node"*
    mkdir -p "$DATA_DIR"
    LONGBOW_MAX_MEMORY="$LONGBOW_MAX_MEMORY" ARROW_DISABLE_LOCKING=1 \
        nohup "${BIN_DIR}/longbow" \
        --listen-addr="$URI" \
        --data-path="$DATA_DIR" \
        --node-id=bench1 \
        </dev/null >/tmp/longbow_server.log 2>&1 &
    # Wait for server to be ready
    for i in {1..30}; do
        sleep 1
        if lsof -i :3000 2>/dev/null | grep -q LISTEN; then
            return 0
        fi
    done
    echo "  WARNING: Server may not be ready"
    return 0
}

run_test() {
    local dtype=$1 dim=$2 count=$3
    local dataset="bench_${dtype}_${dim}_${count}"
    local json_file="${LOG_DIR}/result_${dtype}_${dim}_${count}.json"
    local log_file="/tmp/bench_${dtype}_${dim}_${count}.log"

    "${BIN_DIR}/benchmark-tool" \
        --uri="$URI" \
        --dim="$dim" \
        --dtype="$dtype" \
        --scale="$count" \
        --queries=200 \
        --dataset="$dataset" \
        --json="$json_file" \
        > "$log_file" 2>&1

    if [ $? -eq 0 ] && [ -f "$json_file" ]; then
        doput=$(python3 -c "
import json
with open('$json_file') as f:
    for r in json.load(f):
        if r.get('name') == 'DoPut':
            print(r.get('throughput', 0))
            break
" 2>/dev/null || echo "?")
        dense=$(python3 -c "
import json
with open('$json_file') as f:
    for r in json.load(f):
        if r.get('name') == 'Search_Dense':
            print(r.get('throughput', 0))
            break
" 2>/dev/null || echo "?")
        idx=$(python3 -c "
import json
with open('$log_file') as f:
    for line in f:
        if 'Indexing' in line and 'complete' in line:
            import re
            m = re.search(r'([\d.]+)s', line)
            if m: print(m.group(1))
            break
" 2>/dev/null || echo "?")
        echo "  DoPut=${doput} Dense=${dense} Index=${idx}s"
    else
        echo "  FAILED (check $log_file)"
    fi
}

run_num=0
for dim in "${DIMS[@]}"; do
    for dtype in "${DTYPES[@]}"; do
        for count in "${COUNTS[@]}"; do
            run_num=$((run_num + 1))
            echo ""
            echo "[${run_num}/${TOTAL}] ${dtype} dim=${dim} count=${count}"
            restart_server
            run_test "$dtype" "$dim" "$count"
        done
    done
done

pkill -f longbow 2>/dev/null || true

echo ""
echo "=============================================="
echo "ALL DONE — $(date)"
echo "Results: $LOG_DIR"
echo "=============================================="

# Generate summary JSON
python3 - <<PYEOF
import json, os, glob

log_dir = "$LOG_DIR"
summary = {"date": "$(date -Iseconds)", "platform": "ancalagon", "memory_gb": 12, "results": []}
for f in sorted(glob.glob(os.path.join(log_dir, "result_*.json"))):
    try:
        with open(f) as fp:
            data = json.load(fp)
        fname = os.path.basename(f)
        parts = fname.replace("result_","").replace(".json","").split("_")
        summary["results"].append({
            "dtype": parts[0],
            "dim": int(parts[1]),
            "count": int(parts[2]),
            "data": data
        })
    except: pass

summary_file = os.path.join(log_dir, "summary.json")
with open(summary_file, "w") as f:
    json.dump(summary, f, indent=2)
print(f"Summary saved: {summary_file}")
PYEOF
