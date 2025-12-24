#!/bin/bash
set -e

# Single Node Soak Test with Pprof Profiling
DURATION=${DURATION:-300}
WORKERS=${WORKERS:-4}
DIMS=${DIMS:-384}

echo "Starting Single Node Soak Test with Pprof..."
echo " - Dimensions: $DIMS"
echo " - Duration: $DURATION seconds"
echo " - Workers: $WORKERS"
echo "---------------------------------------------"

# 1. Build Binary
echo "Building longbow binary..."
CGO_ENABLED=0 go build -tags=nogpu -o bin/longbow ./cmd/longbow

# 2. Start Single Node
echo "Starting single node..."
mkdir -p data/node1

LONGBOW_LISTEN_ADDR=0.0.0.0:3000 \
LONGBOW_NODE_ID=node1 \
LONGBOW_META_ADDR=0.0.0.0:3001 \
LONGBOW_METRICS_ADDR=0.0.0.0:9090 \
LONGBOW_DATA_PATH=data/node1 \
LONGBOW_MAX_MEMORY=3221225472 \
./bin/longbow > data/node1/longbow.log 2>&1 &

NODE_PID=$!
echo "Node started (PID: $NODE_PID)"
echo "Waiting 5s for startup..."
sleep 5

# 3. Capture Initial Heap Profile
echo "Capturing initial heap profile..."
curl -s http://localhost:9090/debug/pprof/heap > data/node1/heap_initial.pprof

# 4. Run Load Test
echo "Starting load test..."
python3 scripts/perf_test.py \
  --data-uri grpc://localhost:3000 \
  --meta-uri grpc://localhost:3001 \
  --dim $DIMS \
  --rows 10000 \
  --concurrent $WORKERS \
  --duration $DURATION \
  --operation mixed &

LOAD_PID=$!

# 5. Monitor and Capture Profiles During Test
echo "Monitoring memory and capturing profiles..."
INTERVAL=30
ELAPSED=0

while [ $ELAPSED -lt $DURATION ]; do
  sleep $INTERVAL
  ELAPSED=$((ELAPSED + INTERVAL))
  
  echo "--- Time: ${ELAPSED}s ---"
  
  # Capture heap profile
  curl -s http://localhost:9090/debug/pprof/heap > data/node1/heap_${ELAPSED}s.pprof
  
  # Show memory metrics
  curl -s http://localhost:9090/metrics | grep "longbow_memory_current_bytes\|longbow_memory_utilization"
  
  # Show process RSS
  ps -o rss= -p $NODE_PID | awk '{printf "RSS: %.2f MB\n", $1/1024}'
done

# 6. Capture Final Heap Profile
echo "Capturing final heap profile..."
curl -s http://localhost:9090/debug/pprof/heap > data/node1/heap_final.pprof

# 7. Cleanup
echo "Stopping load test..."
kill $LOAD_PID 2>/dev/null || true

echo "Stopping node..."
kill $NODE_PID 2>/dev/null || true
wait $NODE_PID 2>/dev/null || true

echo ""
echo "Soak test complete!"
echo "Heap profiles saved to data/node1/"
echo ""
echo "To analyze profiles:"
echo "  go tool pprof -http=:8080 data/node1/heap_final.pprof"
echo "  go tool pprof -base=data/node1/heap_initial.pprof data/node1/heap_final.pprof"
