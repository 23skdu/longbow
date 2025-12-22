#!/bin/bash
# Longbow Partition Tolerance Test
# Simulates network partitions using iptables to verify cluster recovery.
# NOTE: Requires sudo privileges for iptables.

set -e

# Configuration
CLUSTER_PORTS=(7946 7947 7948) # Default SWIM ports for 3 nodes
DURATION=30
LOG_DIR="/tmp/longbow_partition_test"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

mkdir -p "$LOG_DIR"

log() {
    echo -e "$(date +'%H:%M:%S') $1"
}

check_health() {
    local port=$1
    if curl -s "http://localhost:$port/health" > /dev/null; then
        return 0
    else
        return 1
    fi
}

simulate_partition() {
    log "${RED}Network Partition: Isolating Node 3 (Port ${CLUSTER_PORTS[2]})...${NC}"
    
    # Drop traffic to/from Port 7948 (Node 3 Gossip Port)
    # Note: This affects localhost traffic. Be careful.
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        sudo iptables -A INPUT -p tcp --dport ${CLUSTER_PORTS[2]} -j DROP
        sudo iptables -A OUTPUT -p tcp --sport ${CLUSTER_PORTS[2]} -j DROP
        sudo iptables -A INPUT -p udp --dport ${CLUSTER_PORTS[2]} -j DROP
        sudo iptables -A OUTPUT -p udp --sport ${CLUSTER_PORTS[2]} -j DROP
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        # MacOS uses pfctl usually, blocking localhost port is tricky and can disrupt everything.
        # Fallback: SIGSTOP the process to simulate unresponsiveness
        log "${RED}MacOS detected: Using SIGSTOP on Node 3 process instead of iptables${NC}"
        pids=$(lsof -t -i :${CLUSTER_PORTS[2]})
        for pid in $pids; do
            kill -STOP $pid
            echo $pid > "$LOG_DIR/frozen_node.pid"
        done
    fi
}

heal_partition() {
    log "${GREEN}Healing Network Partition...${NC}"
    
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        sudo iptables -D INPUT -p tcp --dport ${CLUSTER_PORTS[2]} -j DROP || true
        sudo iptables -D OUTPUT -p tcp --sport ${CLUSTER_PORTS[2]} -j DROP || true
        sudo iptables -D INPUT -p udp --dport ${CLUSTER_PORTS[2]} -j DROP || true
        sudo iptables -D OUTPUT -p udp --sport ${CLUSTER_PORTS[2]} -j DROP || true
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        if [ -f "$LOG_DIR/frozen_node.pid" ]; then
            pid=$(cat "$LOG_DIR/frozen_node.pid")
            kill -CONT $pid
            rm "$LOG_DIR/frozen_node.pid"
        fi
    fi
}

cleanup() {
    heal_partition
    log "Test cleanup complete."
}

trap cleanup EXIT

# Main Execution
echo "=================================================="
echo " Longbow Partition Tolerance Test"
echo "=================================================="

# 1. Start Cluster (Assume 3 nodes running locally)
# check_health ...

# 2. Verify Initial State
log "Verifying 3-node cluster state..."
# TODO: Use CLI or API to check member count

# 3. Inject Failure
simulate_partition

log "Partition active for $DURATION seconds..."
sleep $DURATION

# 4. Verify Split-Brain Detection
# Check that Node 1 & 2 view Node 3 as "Suspect" or "Dead"
# Check that Node 3 views itself as isolated (or keeps functioning if partition logic is weak)

# 5. Heal
heal_partition
sleep 10

# 6. Verify Recovery
log "Verifying cluster converged back to 3 nodes..."
# TODO: Check member list

log "Test Complete."
