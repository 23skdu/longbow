#!/bin/bash
set -e

cd /Users/rsd/REPOS/longbow

echo "=== Cleaning up old processes ==="
kill -9 $(lsof -t -i :3000) 2>/dev/null || true
rm -rf ./data

echo "=== Starting Longbow Server ==="
nohup ./bin/longbow > /tmp/longbow.log 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"

cleanup() {
    echo "=== Cleaning up ==="
    kill -9 $SERVER_PID 2>/dev/null || true
    kill -9 $(lsof -t -i :3000) 2>/dev/null || true
}
trap cleanup EXIT

echo "Waiting for server to be ready..."
for i in {1..30}; do
    if lsof -i :3000 | grep -q LISTEN; then
        echo "Server ready on port 3000"
        break
    fi
    sleep 1
done

if ! lsof -i :3000 | grep -q LISTEN; then
    echo "ERROR: Server failed to start"
    cat /tmp/longbow.log
    exit 1
fi

sleep 2

echo ""
echo "=== Test 1: dim=128 scale=1000 ==="
./bin/bench-tool -mode vec -dim 128 -scale 1000 -queries 100 -search-modes dense

echo ""
echo "=== Test 2: dim=384 scale=1000 ==="
./bin/bench-tool -mode vec -dim 384 -scale 1000 -queries 100 -search-modes dense

echo ""
echo "=== Test 3: dim=768 scale=1000 ==="
./bin/bench-tool -mode vec -dim 768 -scale 1000 -queries 100 -search-modes dense

echo ""
echo "=== Test 4: Multiple search modes ==="
./bin/bench-tool -mode vec -dim 128 -scale 1000 -queries 100 -search-modes dense,hybrid,sparse,filtered,byid

echo ""
echo "=== Test 5: JSON output ==="
rm -rf ./data
./bin/bench-tool -mode vec -dim 128 -scale 1000 -queries 100 -json /tmp/bench_out.json
echo "JSON output:"
cat /tmp/bench_out.json

echo ""
echo "=== All functional tests passed ==="