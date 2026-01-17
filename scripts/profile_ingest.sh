#!/bin/bash
set -e

# Cleanup
cleanup() {
    pkill longbow || true
}
trap cleanup EXIT

# 1. Build Binary (with symbols)
echo "Building Longbow..."
go build -o bin/longbow cmd/longbow/main.go

# 2. Start Server
echo "Starting Server..."
./bin/longbow > server_debug.log 2>&1 &
SERVER_PID=$!

# Wait for ready
for i in {1..30}; do
    if curl -s http://localhost:9090/metrics > /dev/null; then
        echo "Server Ready."
        break
    fi
    sleep 1
done

# 3. Start PProf
echo "Capturing CPU profile..."
curl -s "http://localhost:9090/debug/pprof/profile?seconds=30" > ingestion.pprof &
PPROF_PID=$!

# 4. Run Perf Test (High scale to trigger Async Limit issues)
# 2,000,000 vectors (~1GB raw)
# If worker rejects, we lose data.
echo "Running Benchmark..."
if [ -d "venv" ]; then
    source venv/bin/activate
fi
# Use specific dataset name
DS="ingest_loss_test"
python3 scripts/perf_test.py --dataset $DS --rows 2000000 --dim 128 --skip-search --skip-get

wait $PPROF_PID
echo "Profile captured to ingestion.pprof"

# 5. Analyze Log for Rejections
echo "Checking logs for rejections..."
grep "Failed to apply batch" server_debug.log | head -n 5 || echo "No rejections found."
grep "Large memory addition" server_debug.log | head -n 5 || true

# 6. Verify Count
echo "Verifying Index Count..."
# Use curl or python to check status
curl -s -X POST -d "{\"dataset\":\"$DS\"}" http://localhost:9090/check_readiness # Not a real endpoint? data server is gRPC.
# Use perf_test or simple python script to check count
python3 -c "
from longbow import LongbowClient
import sys
try:
    c = LongbowClient('grpc://localhost:3000', 'grpc://localhost:3001')
    c.connect()
    # Wait a bit for async worker
    import time
    time.sleep(10) 
    # Check count via search blank? or internal stats?
    # SDK doesn't have 'count'.
    # But search returns total? No.
    # Hack: check cluster-status or logs?
    # Just try to search by ID for the last ID
    res = c.search_by_id('$DS', '1999999')
    if 'ids' in res and len(res['ids']) > 0:
        print('Found last ID')
    else:
        print('MISSING last ID')
except Exception as e:
    print(e)
"

# 7. Analyze
echo "Analyzing Profile..."
go tool pprof -top -cum bin/longbow ingestion.pprof > profile_analysis.txt
cat profile_analysis.txt | head -n 30
