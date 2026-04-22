#!/bin/bash
INTERVAL=30
DURATION=3600
END_TIME=$((SECONDS + DURATION))

mkdir -p data/perf_logs/pprof

while [ $SECONDS -lt $END_TIME ]; do
    for PORT in 9000 10000; do
        if lsof -i :$PORT > /dev/null; then
            TIMESTAMP=$(date +%Y%m%d_%H%M%S)
            echo "Collecting pprof from port $PORT at $TIMESTAMP..."
            curl -s http://127.0.0.1:$PORT/debug/pprof/heap > data/perf_logs/pprof/heap_${PORT}_${TIMESTAMP}.pprof
            curl -s http://127.0.0.1:$PORT/debug/pprof/profile?seconds=10 > data/perf_logs/pprof/cpu_${PORT}_${TIMESTAMP}.pprof &
        fi
    done
    sleep $INTERVAL
done
