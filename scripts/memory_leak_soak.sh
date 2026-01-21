#!/bin/bash

# memory_leak_soak.sh: Automated soak test for memory leak detection in Longbow.
# This script runs the memory leak integration test repeatedly for a specified duration.

DURATION=${1:-3600} # Default 1 hour
LOG_FILE="memory_soak_$(date +%Y%m%d_%H%M%S).log"

echo "Starting memory soak test for ${DURATION} seconds..."
echo "Logging to ${LOG_FILE}"

END_TIME=$(( $(date +%s) + DURATION ))

# Track baseline
BEFORE_RSS=$(ps -o rss= -p $$)

while [ $(date +%s) -lt $END_TIME ]; do
    echo "[$(date)] Running memory leak test cycle..." >> "${LOG_FILE}"
    
    # Run a subset of the test or full test
    # We use -run TestMemoryLeak_CreateDropDataset to target the leak test
    go test -v ./internal/store -run TestMemoryLeak_CreateDropDataset -timeout 10m >> "${LOG_FILE}" 2>&1
    
    if [ $? -ne 0 ]; then
        echo "Test failed! Checking logs..."
        tail -n 20 "${LOG_FILE}"
        exit 1
    fi
    
    # Log current system memory usage
    echo "[$(date)] Current RSS: $(ps -o rss= -p $$) KB" >> "${LOG_FILE}"
    echo "[$(date)] Goroutines: $(go tool pprof -text http://localhost:6060/debug/pprof/goroutine 2>/dev/null | grep total || echo 'pprof not enabled')" >> "${LOG_FILE}"
    
    sleep 5
done

echo "Soak test complete."
AFTER_RSS=$(ps -o rss= -p $$)
echo "Baseline RSS: ${BEFORE_RSS} KB"
echo "Final RSS: ${AFTER_RSS} KB"

GROWTH=$(( AFTER_RSS - BEFORE_RSS ))
echo "Total RSS growth: ${GROWTH} KB"
