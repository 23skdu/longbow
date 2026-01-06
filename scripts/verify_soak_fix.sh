#!/bin/bash
set -e

cleanup() {
    echo "Stopping longbow..."
    pkill longbow || true
}
trap cleanup EXIT

# 1. Start Server
echo "Starting Longbow..."
export LISTEN_ADDR=:3300
export META_ADDR=:3301
export METRICS_ADDR=:9300
export GOSSIP_ENABLED=false
rm -rf verify_data
export DATA_PATH=verify_data

./bin/longbow > verify_server.log 2>&1 &
SERVER_PID=$!
sleep 5

# 2. Ingest Data
echo "Ingesting data..."
./bin/bench-tool -peers 127.0.0.1:3300 -mode ingest -duration 5s -batch-size 100 -concurrency 2 > verify_ingest.log 2>&1
echo "Ingest complete."

# 3. Search Data
echo "Searching data..."
./bin/bench-tool -peers 127.0.0.1:3300 -mode search -duration 5s -concurrency 2 > verify_search.log 2>&1
echo "Search complete."

# 4. Check results
echo "Checking search errors..."
grep "Errors:" verify_search.log
grep "Throughput:" verify_search.log

ERRORS=$(grep "Errors:" verify_search.log | awk '{print $2}')
if [ "$ERRORS" -eq "0" ]; then
    echo "SUCCESS: No search errors found."
else
    echo "FAILURE: Search errors found: $ERRORS"
    cat verify_search.log
    exit 1
fi
