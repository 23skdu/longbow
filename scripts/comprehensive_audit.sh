#!/bin/bash
# scripts/comprehensive_audit.sh

# User-requested parameters
DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
DIMS="128,384,768,1024,3072"
COUNTS="100000,250000,500000,750000"
MAX_MEM=19327352832 # 18 GB

# 1. Local Cleanup
echo "Performing local environment cleanup..."
pkill -f longbow
pkill -f bench-tool
pkill -f unified_benchmark
rm -rf bin/ longbow longbow-server longbow-gpu longbow-metal data/ logs/ profiles/ local_server.log
mkdir -p bin logs data/perf_logs

# 2. Remote Cleanup
echo "Performing remote environment cleanup on ancalagon..."
ssh ancalagon "pkill -u rsd -f longbow; pkill -u rsd -f bench-tool; pkill -u rsd -f unified_benchmark; rm -rf REPOS/longbow/bin/ REPOS/longbow/longbow REPOS/longbow/data/ REPOS/longbow/logs/ REPOS/longbow/profiles/ REPOS/longbow/local_server.log; mkdir -p REPOS/longbow/bin REPOS/longbow/logs REPOS/longbow/data/perf_logs"

# 3. Build local binaries
echo "Building local binaries..."
go build -o bin/longbow-cli ./cmd/cli
go build -o bin/bench-tool ./cmd/bench-tool
CGO_ENABLED=1 go build -tags "gpu onnx" -o bin/longbow-metal ./cmd/longbow
ln -sf longbow-metal bin/longbow

# Function to run local benchmarks
run_local() {
    echo "Starting Local CPU and specialized benchmarks..."
    python3 scripts/unified_benchmark.py \
        --mode cpu,temporal,geo,graphrag,learned_index \
        --dtypes "$DTYPES" \
        --dims "$DIMS" \
        --counts "$COUNTS" \
        --search-modes all \
        --label local_cpu \
        --duration 2 \
        --queries 100 \
        --memory $MAX_MEM \
        --pprof > logs/local_cpu_audit.log 2>&1

    # Organize local_cpu results
    mkdir -p data/perf_logs/local_cpu
    mv data/perf_logs/result_*.json data/perf_logs/local_cpu/ 2>/dev/null
    mv data/perf_logs/profile_*.pprof data/perf_logs/local_cpu/ 2>/dev/null
    mv data/perf_logs/bench_*.log data/perf_logs/local_cpu/ 2>/dev/null
    mv data/perf_logs/longbow_*.log data/perf_logs/local_cpu/ 2>/dev/null

    echo "Starting Local Metal benchmarks..."
    python3 scripts/unified_benchmark.py \
        --mode metal \
        --dtypes "$DTYPES" \
        --dims "$DIMS" \
        --counts "$COUNTS" \
        --search-modes all \
        --label local_metal \
        --duration 2 \
        --queries 100 \
        --memory $MAX_MEM \
        --pprof > logs/local_metal_audit.log 2>&1

    # Organize local_metal results
    mkdir -p data/perf_logs/local_metal
    mv data/perf_logs/result_*.json data/perf_logs/local_metal/ 2>/dev/null
    mv data/perf_logs/profile_*.pprof data/perf_logs/local_metal/ 2>/dev/null
    mv data/perf_logs/bench_*.log data/perf_logs/local_metal/ 2>/dev/null
    mv data/perf_logs/longbow_*.log data/perf_logs/local_metal/ 2>/dev/null
}

# Function to run remote benchmarks on ancalagon
run_remote() {
    echo "Starting Remote Benchmark on ancalagon..."
    ssh ancalagon "cd REPOS/longbow && \
        go build -o bin/longbow-cli ./cmd/cli && \
        go build -o bin/bench-tool ./cmd/bench-tool && \
        make build-cuda && \
        echo 'Starting Remote CPU and specialized benchmarks...' && \
        python3 scripts/unified_benchmark.py \
            --mode cpu,temporal,geo,graphrag,learned_index \
            --dtypes \"$DTYPES\" \
            --dims \"$DIMS\" \
            --counts \"$COUNTS\" \
            --search-modes all \
            --label remote_cpu \
            --duration 2 \
            --queries 100 \
            --memory $MAX_MEM \
            --pprof > logs/remote_cpu_audit.log 2>&1 && \
        mkdir -p data/perf_logs/remote_cpu && \
        mv data/perf_logs/result_*.json data/perf_logs/remote_cpu/ 2>/dev/null && \
        mv data/perf_logs/profile_*.pprof data/perf_logs/remote_cpu/ 2>/dev/null && \
        mv data/perf_logs/bench_*.log data/perf_logs/remote_cpu/ 2>/dev/null && \
        mv data/perf_logs/longbow_*.log data/perf_logs/remote_cpu/ 2>/dev/null && \
        echo 'Starting Remote CUDA benchmarks...' && \
        python3 scripts/unified_benchmark.py \
            --mode cuda \
            --dtypes \"$DTYPES\" \
            --dims \"$DIMS\" \
            --counts \"$COUNTS\" \
            --search-modes all \
            --label remote_cuda \
            --duration 2 \
            --queries 100 \
            --memory $MAX_MEM \
            --pprof > logs/remote_cuda_audit.log 2>&1 && \
        mkdir -p data/perf_logs/remote_cuda && \
        mv data/perf_logs/result_*.json data/perf_logs/remote_cuda/ 2>/dev/null && \
        mv data/perf_logs/profile_*.pprof data/perf_logs/remote_cuda/ 2>/dev/null && \
        mv data/perf_logs/bench_*.log data/perf_logs/remote_cuda/ 2>/dev/null && \
        mv data/perf_logs/longbow_*.log data/perf_logs/remote_cuda/ 2>/dev/null"
}

# Run in parallel
echo "Launching parallel host benchmarks..."
run_local &
LOCAL_PID=$!

run_remote &
REMOTE_PID=$!

wait $LOCAL_PID
echo "Local benchmarks completed."

wait $REMOTE_PID
echo "Remote benchmarks completed."

echo "Syncing remote results..."
rsync -avz ancalagon:REPOS/longbow/data/perf_logs/ data/perf_logs/

echo "Collecting all results..."
python3 scripts/aggregate_results.py --dir data/perf_logs --out docs/performance_matrix_v021.md

echo "Merging results into docs/performance.md..."
python3 -c "
import os
new_data = ''
if os.path.exists('docs/performance_matrix_v021.md'):
    with open('docs/performance_matrix_v021.md') as f:
        new_data = f.read()

# Strip the title from the generated report if present
if new_data.startswith('# Longbow v0.2.1 Performance Matrix'):
    new_data = new_data[len('# Longbow v0.2.1 Performance Matrix'):].strip()

old_data = ''
if os.path.exists('docs/performance.md'):
    with open('docs/performance.md') as f:
        old_data = f.read()

title = '# Longbow Performance Benchmarks\n\n'
if old_data.startswith(title):
    merged = title + '## v0.2.3-rc1 Final Performance Validation (2026-05-19)\n\n' + new_data + '\n\n' + old_data[len(title):]
else:
    merged = '## v0.2.3-rc1 Final Performance Validation (2026-05-19)\n\n' + new_data + '\n\n' + old_data

with open('docs/performance.md', 'w') as f:
    f.write(merged)
"

echo "Audit completed successfully!"
