#!/bin/bash
# Master Sequential Benchmark Runner

set -e

MODES=("cpu" "metal" "cuda")
HOSTS=("local" "ancalagon")

# Parameters from user request
DIMS="128,384,768"
COUNTS="10000,25000,50000,100000,250000,500000"
TYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"

for host in "${HOSTS[@]}"; do
    for mode in "${MODES[@]}"; do
        # Skip invalid combinations
        if [[ "$host" == "local" && "$mode" == "cuda" ]]; then continue; fi
        if [[ "$host" == "ancalagon" && "$mode" == "metal" ]]; then continue; fi

        echo "================================================================"
        echo "Starting Sequential Run: Host=$host, Mode=$mode"
        echo "================================================================"

        if [[ "$host" == "local" ]]; then
            export AUTOSCALE_ENABLED=false
            ./scripts/bench_tool_runner.sh --mode $mode --dims "$DIMS" --counts "$COUNTS" --types "$TYPES"
        else
            # Remote run
            ssh ancalagon "cd ~/REPOS/longbow && export AUTOSCALE_ENABLED=false; ./scripts/bench_tool_runner.sh --mode $mode --dims \"$DIMS\" --counts \"$COUNTS\" --types \"$TYPES\""
        fi
        
        echo "Finished Sequential Run: Host=$host, Mode=$mode"
        echo "Cleaning up before next run..."
        killall longbow bench-tool 2>/dev/null || true
        ssh ancalagon "killall -9 longbow bench-tool 2>/dev/null || true" 2>/dev/null || true
        sleep 5
    done
done
