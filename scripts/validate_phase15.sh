#!/bin/bash
set -e

# Cleanup function
cleanup() {
    echo "Stopping Longbow..."
    pkill longbow || true
    pkill -f "go run cmd/longbow/main.go" || true
}
trap cleanup EXIT

# 1. Install SDK
echo "Installing SDK..."
if [ -d "venv" ]; then
    source venv/bin/activate
else
    python3 -m venv venv
    source venv/bin/activate
fi
pip install -q  ./longbowclientsdk pandas pyarrow numpy

# 2. Start Server
echo "Starting Longbow Server..."
pkill longbow || true
go run cmd/longbow/main.go > server_phase15.log 2>&1 &
SERVER_PID=$!

# Wait for healthy
echo "Waiting for server to be ready..."
for i in {1..30}; do
    if curl -s http://localhost:9090/metrics > /dev/null; then
        echo "Server is ready!"
        break
    fi
    sleep 1
done

# 3. Start Profiling (Background)
echo "Starting PProf capture (20s)..."
curl -s "http://localhost:9090/debug/pprof/profile?seconds=20" > phase15_cpu.pprof &
PPROF_PID=$!

# 4. Run Benchmark (Bulk Insert focus)
# 15,000 vectors to trigger bulk (~3 batches if threshold 1000)
# But standard bulk chunk is 2500? AddBatchBulk is called if N >= 1000.
python3 scripts/perf_test.py --dataset phase15_test --rows 15000 --dim 128 --concurrent 0 --json results_phase15.json

# Wait for pprof
wait $PPROF_PID
echo "PProf captured to phase15_cpu.pprof"

echo "Validation Complete."
