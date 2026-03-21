#!/bin/bash
set -e

echo "Starting isolated server for profiling..."
rm -rf ./data
LONGBOW_MAX_MEMORY=21474836480 ./bin/longbow > server_profile.log 2>&1 &
SERVER_PID=$!

# Wait for server to bind
sleep 5

(sleep 6 && echo "Triggering profile mid-test..." && curl -s "http://127.0.0.1:9090/debug/pprof/profile?seconds=5" > cpu.prof) &

echo "Running benchmark tool for float32 dim=384 scale=15000..."
./bin/benchmark_tool -uri 127.0.0.1:3000 -dtype float32 -dim 384 -scale 15000 -queries 10 -dataset bench_profile

echo "Waiting for profile to flush..."
sleep 10

echo "Stopping server..."
kill $SERVER_PID 2>/dev/null || true

echo "Profile captured to cpu.prof"
