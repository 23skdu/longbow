#!/bin/bash
# cleanup_nodes.sh - System-wide cleanup for Longbow Benchmark Cluster

NODES=("localhost" "ancalagon")
WORKSPACE="~/REPOS/longbow"

for NODE in "${NODES[@]}"; do
    echo "Cleaning up node: $NODE"
    if [ "$NODE" == "localhost" ]; then
        killall longbow-server bench-tool 2>/dev/null || true
        rm -rf /tmp/longbow_bench_*
        rm -f longbow.log
    else
        ssh "$NODE" "killall longbow-server bench-tool 2>/dev/null || true; rm -rf /tmp/longbow_bench_*; rm -f $WORKSPACE/longbow.log"
    fi
done

echo "Cleanup complete."
