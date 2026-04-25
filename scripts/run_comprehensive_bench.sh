#!/bin/bash
# run_comprehensive_bench.sh
# Orchestrates benchmarking on local host and ancalagon in parallel.

set -e

# Configuration
DIMS="128,384,768"
DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant"
MEMORY=19327352832
QUERIES=1000

# Small batch counts (30s duration)
SMALL_COUNTS="500,1000,5000"
SMALL_DURATION=30

# Large batch counts (60s duration)
LARGE_COUNTS="15000,50000"
LARGE_DURATION=60

# Modes to test
MODES="cpu geo temporal graphrag recommend learned_index"

# Function to run local benchmarks
run_local() {
    echo "[LOCAL] Starting benchmarks..."
    ( [ -f venv/bin/activate ] && source venv/bin/activate || echo '[LOCAL] Using system python' )

    
    for count in 500 1000 5000 15000 50000; do
        if [ $count -le 5000 ]; then
            duration=$SMALL_DURATION
        else
            duration=$LARGE_DURATION
        fi
        
        # 1. CPU and Metal modes
        python3 scripts/unified_benchmark.py --mode cpu --dims "$DIMS" --counts "$count" --dtypes "$DTYPES" --memory "$MEMORY" --queries "$QUERIES" --duration "$duration"
        python3 scripts/unified_benchmark.py --mode metal --dims "$DIMS" --counts "$count" --dtypes "$DTYPES" --memory "$MEMORY" --queries "$QUERIES" --duration "$duration"
        
        # 2. Specialized modes
        for mode in geo temporal graphrag recommend learned_index; do
            echo "[LOCAL] Running mode=$mode count=$count..."
            python3 scripts/unified_benchmark.py --mode "$mode" --dims "$DIMS" --counts "$count" --dtypes "$DTYPES" --memory "$MEMORY" --queries "$QUERIES" --duration "$duration" || echo "[LOCAL] $mode count=$count failed"
        done
    done
    echo "[LOCAL] Benchmarks complete."
}

# Function to run remote benchmarks on ancalagon
run_remote() {
    echo "[REMOTE] Starting benchmarks on ancalagon..."
    
    # Use system python with --break-system-packages (pre-installed) if venv is missing
    ssh ancalagon "cd REPOS/longbow && \
    ( [ -f venv/bin/activate ] && source venv/bin/activate || echo '[REMOTE] Using system python' ) && \
    for count in 500 1000 5000 15000 50000; do \
        if [ \$count -le 5000 ]; then \
            duration=$SMALL_DURATION; \
        else \
            duration=$LARGE_DURATION; \
        fi; \
        python3 scripts/unified_benchmark.py --mode cpu --dims \"$DIMS\" --counts \"\$count\" --dtypes \"$DTYPES\" --memory \"$MEMORY\" --queries \"$QUERIES\" --duration \"\$duration\"; \
        python3 scripts/unified_benchmark.py --mode cuda --dims \"$DIMS\" --counts \"\$count\" --dtypes \"$DTYPES\" --memory \"$MEMORY\" --queries \"$QUERIES\" --duration \"\$duration\"; \
        for mode in geo temporal graphrag recommend learned_index; do \
            echo \"[REMOTE] Running mode=\$mode count=\$count...\"; \
            python3 scripts/unified_benchmark.py --mode \"\$mode\" --dims \"$DIMS\" --counts \"\$count\" --dtypes \"$DTYPES\" --memory \"$MEMORY\" --queries \"$QUERIES\" --duration \"\$duration\" || echo \"[REMOTE] \$mode count=\$count failed\"; \
        done; \
    done"
    
    # Pull remote results back
    echo "[REMOTE] Fetching results..."
    scp -r ancalagon:REPOS/longbow/data/perf_logs/*.json data/perf_logs/
    echo "[REMOTE] Benchmarks complete."
}

# Ensure data/perf_logs exists
mkdir -p data/perf_logs

# Run in parallel
run_local &
LOCAL_PID=$!

run_remote &
REMOTE_PID=$!

echo "Local benchmarks PID: $LOCAL_PID"
echo "Remote benchmarks PID: $REMOTE_PID"

wait $LOCAL_PID
wait $REMOTE_PID

echo "All benchmarks finished!"
