#!/bin/bash
# Orchestrates benchmarking on local host and ancalagon in parallel.

set -e

DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant"
MEMORY=19327352832
QUERIES=1000
SMALL_DURATION=30
LARGE_DURATION=60

run_tests() {
    local prefix=$1
    local mode=$2
    local dims=$3
    local counts=$4

    ( [ -f venv/bin/activate ] && source venv/bin/activate || true )
    
    for count in $counts; do
        if [ $count -le 5000 ]; then
            duration=15
            timeout=30
        else
            duration=30
            timeout=60
        fi
        
        echo "[$prefix] Running mode=$mode dims=$dims count=$count..."
        python3 scripts/unified_benchmark.py --mode "$mode" --dims "$dims" --counts "$count" --dtypes "$DTYPES" --memory "$MEMORY" --queries "$QUERIES" --duration "$duration" --timeout "$timeout" || echo "[$prefix] $mode count=$count failed"
    done
}

run_local() {
    echo "[LOCAL] Starting benchmarks..."
    # Set 1
    for mode in cpu metal geo temporal graphrag recommend learned_index; do
        run_tests "LOCAL" $mode "128,384" "500 1000 5000 15000 50000 100000"
    done
    # Set 2
    for mode in cpu metal geo temporal graphrag recommend learned_index; do
        run_tests "LOCAL" $mode "768,1024,3072" "500 1000 5000 10000"
    done
    echo "[LOCAL] Benchmarks complete."
}

run_remote() {
    echo "[REMOTE] Starting benchmarks on ancalagon..."
    
    ssh ancalagon "cd REPOS/longbow && cat > /tmp/run_remote_bench.sh << 'EOF'
#!/bin/bash
DTYPES=\"$DTYPES\"
MEMORY=\"$MEMORY\"
QUERIES=\"$QUERIES\"
SMALL_DURATION=\"$SMALL_DURATION\"
LARGE_DURATION=\"$LARGE_DURATION\"

run_tests() {
    local prefix=\$1
    local mode=\$2
    local dims=\$3
    local counts=\$4

    ( [ -f venv/bin/activate ] && source venv/bin/activate || true )
    
    for count in \$counts; do
        if [ \$count -le 5000 ]; then
            duration=\$SMALL_DURATION
            timeout=30
        else
            duration=\$LARGE_DURATION
            timeout=60
        fi
        
        echo \"[\$prefix] Running mode=\$mode dims=\$dims count=\$count...\"
        python3 scripts/unified_benchmark.py --mode \"\$mode\" --dims \"\$dims\" --counts \"\$count\" --dtypes \"\$DTYPES\" --memory \"\$MEMORY\" --queries \"\$QUERIES\" --duration \"\$duration\" --timeout \"\$timeout\" || echo \"[\$prefix] \$mode count=\$count failed\"
    done
}

# Set 1
for mode in cpu cuda geo temporal graphrag recommend learned_index; do
    run_tests \"REMOTE\" \$mode \"128,384\" \"500 1000 5000 15000 50000 100000\"
done
# Set 2
for mode in cpu cuda geo temporal graphrag recommend learned_index; do
    run_tests \"REMOTE\" \$mode \"768,1024,3072\" \"500 1000 5000 10000\"
done
EOF
chmod +x /tmp/run_remote_bench.sh && /tmp/run_remote_bench.sh"
    
    # Pull remote results back
    echo "[REMOTE] Fetching results..."
    scp -r ancalagon:REPOS/longbow/data/perf_logs/*.json data/perf_logs/
    echo "[REMOTE] Benchmarks complete."
}

mkdir -p data/perf_logs

run_local &
LOCAL_PID=$!

run_remote &
REMOTE_PID=$!

echo "Local benchmarks PID: $LOCAL_PID"
echo "Remote benchmarks PID: $REMOTE_PID"

wait $LOCAL_PID
wait $REMOTE_PID

echo "All benchmarks finished!"
