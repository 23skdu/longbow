#!/bin/bash
mkdir -p profiles
echo "Starting profiler loop..."

check_memory_pressure() {
    available_kb=$(grep MemAvailable /proc/meminfo 2>/dev/null | awk '{print $2}')
    if [ -z "$available_kb" ]; then
        available_kb=$(grep MemFree /proc/meminfo | awk '{print $2}')
    fi
    threshold_kb=524288
    if [ "$available_kb" -lt "$threshold_kb" ]; then
        echo "Memory pressure detected (${available_kb}KB available), skipping pprof collection"
        return 1
    fi
    return 0
}

while true; do
    TS=$(date +%s)
    if ! check_memory_pressure; then
        sleep 30
        continue
    fi
    echo "Capturing heap profile at $TS..."
    curl -s --fail -o profiles/heap_node1_${TS}.pprof http://localhost:9090/debug/pprof/heap 2>/dev/null || echo "Failed node1"
    curl -s --fail -o profiles/heap_node2_${TS}.pprof http://localhost:9091/debug/pprof/heap 2>/dev/null || echo "Failed node2"
    curl -s --fail -o profiles/heap_node3_${TS}.pprof http://localhost:9092/debug/pprof/heap 2>/dev/null || echo "Failed node3"
    sleep 30
done
