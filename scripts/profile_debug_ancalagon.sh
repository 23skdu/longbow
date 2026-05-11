#!/bin/bash
set -e

# Start server on ancalagon with 12GB limit
ssh ancalagon "cd longbow && 
    export LONGBOW_MAX_MEMORY=12884901888 && 
    export LONGBOW_LISTEN_ADDR=0.0.0.0:4000 && 
    export LONGBOW_METRICS_ADDR=0.0.0.0:10000 && 
    nohup ./bin/longbow > server_debug.log 2>&1 &"

sleep 10

# Ingest 10k vectors (float32)
ssh ancalagon "cd longbow && 
    ./bin/bench-tool -mode vec -uri grpc://127.0.0.1:4000 -dim 384 -dtype float32 -scale 10000 -queries 0"

# Monitor memory while searching
ssh ancalagon "cd longbow && 
    (while true; do date; free -h; ps -o rss,vsz,comm -p \$(pgrep longbow); sleep 2; done) > mem_monitor.log &"

# Run search-only profile
ssh ancalagon "cd longbow && 
    curl -o search_debug.pprof http://127.0.0.1:10000/debug/pprof/profile?seconds=20 &
    ./bin/bench-tool -mode vec -uri grpc://127.0.0.1:4000 -dim 384 -dtype float32 -scale 0 -queries 50000 -workers 2"

# Pull logs and profile
rsync ancalagon:longbow/mem_monitor.log ./profiles/ancalagon_mem.log
rsync ancalagon:longbow/search_debug.pprof ./profiles/ancalagon_search_debug.pprof

# Kill monitoring and server
ssh ancalagon "pkill -f mem_monitor.log || true"
ssh ancalagon "pkill longbow"
