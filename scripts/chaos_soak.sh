#!/bin/bash

# Longbow Chaos Soak Test Script
# 1. Starts server
# 2. Starts soak test client
# 3. Periodically kills server to test WAL recovery and persistence integrity

DATA_DIR="./soak_data"
SERVER_BIN="./cmd/longbow/longbow"
CLIENT_BIN="./cmd/soak_test/soak_test"
LOG_DIR="./logs"

mkdir -p $DATA_DIR
mkdir -p $LOG_DIR

echo "🌊 Starting Chaos Soak Test"

# Build components
go build -o $SERVER_BIN ./cmd/longbow/
go build -o $CLIENT_BIN ./cmd/soak_test/

cleanup() {
    echo "Shutting down..."
    kill $SERVER_PID $CLIENT_PID 2>/dev/null
    exit
}

trap cleanup SIGINT SIGTERM

# Start Server
export LONGBOW_DATA_PATH=$DATA_DIR
export LONGBOW_LOG_LEVEL=debug
export LONGBOW_USE_DISK=1
export LONGBOW_COMPACTION_INTERVAL=10s
export LONGBOW_AUTO_SHARDING_THRESHOLD=500

start_server() {
    echo "🚀 Starting Longbow Server..."
    $SERVER_BIN > $LOG_DIR/server.log 2>&1 &
    SERVER_PID=$!
    sleep 5
}

start_server

# Start Soak Test Client
echo "🏃 Starting Soak Test Client..."
$CLIENT_BIN -duration 1h -workers 4 -batch 100 > $LOG_DIR/client.log 2>&1 &
CLIENT_PID=$!

CYCLE=1
while kill -0 $CLIENT_PID 2>/dev/null; do
    echo "Cycle $CYCLE: Load running..."
    sleep 60
    
    echo "🔥 CHAOS: Killing Server (SIGKILL)..."
    kill -9 $SERVER_PID
    sleep 2
    
    echo "♻️ Restarting Server..."
    start_server
    
    # Check if client is still alive
    if ! kill -0 $CLIENT_PID 2>/dev/null; then
        echo "⚠️ Client died, restarting..."
        $CLIENT_BIN -duration 1h -workers 4 -batch 100 >> $LOG_DIR/client.log 2>&1 &
        CLIENT_PID=$!
    fi
    
    # Trigger a manual offload to test tiered storage recovery
    echo "📦 Triggering Tiered Offload..."
    go run scripts/chaos_tools/trigger_action.go TieredOffload > /dev/null 2>&1
    
    # Trigger a manual compaction
    echo "🏗️ Triggering Compaction..."
    go run scripts/chaos_tools/trigger_action.go Compact > /dev/null 2>&1

    CYCLE=$((CYCLE+1))
done

echo "✅ Chaos Soak Test Completed"
cleanup
