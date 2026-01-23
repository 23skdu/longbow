#!/bin/bash
mkdir -p profiles
echo "Starting profiler loop..."
while true; do
    TS=$(date +%s)
    echo "Capturing heap profile at $TS..."
    curl -s -o profiles/heap_node1_${TS}.pprof http://localhost:9090/debug/pprof/heap
    curl -s -o profiles/heap_node2_${TS}.pprof http://localhost:9091/debug/pprof/heap
    curl -s -o profiles/heap_node3_${TS}.pprof http://localhost:9092/debug/pprof/heap
    sleep 30
done
