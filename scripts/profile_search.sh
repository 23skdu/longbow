#!/bin/bash
set -e

# Cleanup
cleanup() {
    pkill -9 longbow || true
    # Keep server_debug.log
}
trap cleanup EXIT

# 1. Build Binary
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

# 3. Ingest Data (Prep)
echo "Ingesting 250,000 vectors (Prep)..."
if [ -d "venv" ]; then
    source venv/bin/activate
fi
DS="search_prof"
python3 scripts/perf_test.py --dataset $DS --rows 250000 --dim 128 --skip-search --skip-get

echo "Ingestion request done. Waiting for indexing to drain..."

# Wait for Idle
for i in {1..120}; do
    METRICS=$(curl -s "http://localhost:9090/metrics")
    
    # Extract Queue Depths (Default to 0 if not present)
    # Using simple grep/awk for speed
    INGEST=$(echo "$METRICS" | grep "longbow_ingestion_queue_depth" | grep -v "#" | awk '{print $2}' | head -n1 || echo "0")
    INDEX=$(echo "$METRICS" | grep "longbow_index_queue_depth" | grep -v "#" | awk '{print $2}' | head -n1 || echo "0")
    
    # Handle scientific notation or float
    INGEST=${INGEST%.*}
    INDEX=${INDEX%.*}
    [ -z "$INGEST" ] && INGEST=0
    [ -z "$INDEX" ] && INDEX=0

    echo "Queue Status: Ingest=$INGEST Index=$INDEX"

    if [ "$INGEST" -eq 0 ] && [ "$INDEX" -eq 0 ]; then
        echo "System Idle/Ready."
        break
    fi
    sleep 2
done

# 4. Start PProf
echo "Capturing CPU profile (Run Phase)..."
curl -s "http://localhost:9090/debug/pprof/profile?seconds=20" > search.pprof &
PPROF_PID=$!

# 5. Run Search (Concurrent Load)
echo "Running Mixed Search Workload..."
# High number of queries to stress search
python3 scripts/perf_test.py --dataset $DS --skip-put --queries 10000 --concurrent 8

wait $PPROF_PID
echo "Profile captured to search.pprof"

# 6. Analyze
echo "Analyzing Profile..."
go tool pprof -top -cum bin/longbow search.pprof > profile_analysis.txt
cat profile_analysis.txt | head -n 30
