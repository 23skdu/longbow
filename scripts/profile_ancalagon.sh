#!/bin/bash
set -e
# Sync latest code
# rsync -avz --exclude '.git' ./ ancalagon:longbow/

# Start server on ancalagon
ssh ancalagon "cd longbow && 
    export LONGBOW_MAX_MEMORY=19327352832 && 
    export LONGBOW_LISTEN_ADDR=0.0.0.0:4000 && 
    export LONGBOW_METRICS_ADDR=0.0.0.0:10000 && 
    nohup ./bin/longbow > server.log 2>&1 &"

sleep 5

# Run benchmark on ancalagon (loopback)
ssh ancalagon "cd longbow && 
    ./bin/bench-tool -mode vec -uri grpc://127.0.0.1:4000 -dim 384 -dtype float32 -scale 25000 -queries 5000 -json loopback_prof.json"

# Collect profile while benchmark is running? 
# Better to run a separate bench and profile.
ssh ancalagon "cd longbow && 
    curl -o loopback.pprof http://127.0.0.1:10000/debug/pprof/profile?seconds=10 &
    ./bin/bench-tool -mode vec -uri grpc://127.0.0.1:4000 -dim 384 -dtype float32 -scale 25000 -queries 10000"

# Pull profile back
rsync ancalagon:longbow/loopback.pprof ./profiles/ancalagon_loopback.pprof

# Kill server
ssh ancalagon "pkill longbow"
