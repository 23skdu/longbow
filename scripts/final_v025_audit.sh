#!/bin/bash
# scripts/final_v025_audit.sh

# User-requested parameters
DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
DIMS="128,384"
COUNTS="10000,25000,100000,250000"
MAX_MEM=19327352832 # 18 GB

# Ensure binaries are built and directories exist
echo "Building binaries..."
mkdir -p bin logs
go build -o bin/longbow ./cmd/longbow
go build -o bin/longbow-cli ./cmd/cli
go build -o bin/bench-tool ./cmd/bench-tool

# Function to run local benchmarks
run_local() {
    echo "Starting Local CPU Benchmark..."
    LONGBOW_MAX_MEMORY=$MAX_MEM python3 scripts/unified_benchmark.py \
        --mode cpu \
        --dtypes "$DTYPES" \
        --dims "$DIMS" \
        --counts "$COUNTS" \
        --search-modes all \
        --label local_cpu \
        --duration 3 \
        --pprof > logs/local_cpu_audit.log 2>&1

    echo "Starting Local Metal Benchmark..."
    LONGBOW_MAX_MEMORY=$MAX_MEM python3 scripts/unified_benchmark.py \
        --mode metal \
        --dtypes "$DTYPES" \
        --dims "$DIMS" \
        --counts "$COUNTS" \
        --search-modes all \
        --label local_metal \
        --duration 3 \
        --pprof > logs/local_metal_audit.log 2>&1
}

# Function to run remote benchmarks on ancalagon
run_remote() {
    echo "Starting Remote Benchmark on ancalagon..."
    ssh ancalagon "cd REPOS/longbow && \
        go build -o bin/longbow ./cmd/longbow && \
        go build -o bin/longbow-cli ./cmd/cli && \
        go build -o bin/bench-tool ./cmd/bench-tool && \
        mkdir -p logs && \
        echo 'Starting Remote CPU Benchmark...' && \
        LONGBOW_MAX_MEMORY=$MAX_MEM python3 scripts/unified_benchmark.py \
            --mode cpu \
            --dtypes \"$DTYPES\" \
            --dims \"$DIMS\" \
            --counts \"$COUNTS\" \
            --search-modes all \
            --label remote_cpu \
            --duration 3 \
            --pprof > logs/remote_cpu_audit.log 2>&1 && \
        echo 'Starting Remote CUDA Benchmark...' && \
        LONGBOW_MAX_MEMORY=$MAX_MEM python3 scripts/unified_benchmark.py \
            --mode cuda \
            --dtypes \"$DTYPES\" \
            --dims \"$DIMS\" \
            --counts \"$COUNTS\" \
            --search-modes all \
            --label remote_cuda \
            --duration 3 \
            --pprof > logs/remote_cuda_audit.log 2>&1"
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
python3 scripts/aggregate_results.py --dir data/perf_logs --out docs/performance_matrix_v025.md

echo "Merging results into docs/performance.md..."
python3 -c "
with open('docs/performance_matrix_v025.md') as f:
    new_data = f.read()
# Strip the title from the generated report if present
if new_data.startswith('# Longbow v0.2.5 Performance Matrix'):
    new_data = new_data[len('# Longbow v0.2.5 Performance Matrix'):].strip()
with open('docs/performance.md') as f:
    old_data = f.read()
title = '# Longbow Performance Benchmarks\n\n'
if old_data.startswith(title):
    merged = title + '## v0.2.5 Final Performance Validation (2026-05-16)\n\n' + new_data + '\n\n' + old_data[len(title):]
else:
    merged = '## v0.2.5 Final Performance Validation (2026-05-16)\n\n' + new_data + '\n\n' + old_data
with open('docs/performance.md', 'w') as f:
    f.write(merged)
"

