#!/bin/bash
set -e

# Start server on ancalagon
ssh ancalagon "cd longbow && 
    export LONGBOW_MAX_MEMORY=12884901888 && # 12GB to be safe
    export LONGBOW_LISTEN_ADDR=0.0.0.0:4000 && 
    export LONGBOW_METRICS_ADDR=0.0.0.0:10000 && 
    nohup ./bin/longbow > server_search_restricted.log 2>&1 &"

sleep 10

# Ingest some data first (restricted scale)
ssh ancalagon "cd longbow && 
    ./bin/bench-tool -mode vec -uri grpc://127.0.0.1:4000 -dim 384 -dtype float32 -scale 5000 -queries 0"

# Now profile SEARCH only with restricted concurrency
ssh ancalagon "cd longbow && 
    curl -o search_loopback_restricted.pprof http://127.0.0.1:10000/debug/pprof/profile?seconds=30 &
    ./bin/bench-tool -mode vec -uri grpc://127.0.0.1:4000 -dim 384 -dtype float32 -scale 0 -queries 20000 -workers 4"

# Pull profile back
rsync ancalagon:longbow/search_loopback_restricted.pprof ./profiles/ancalagon_search_restricted.pprof

# Kill server
ssh ancalagon "pkill longbow"
