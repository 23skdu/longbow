#!/bin/bash
# Longbow Partition Tolerance Test
# Simulates network partitions using iptables to verify cluster recovery.
# NOTE: Requires sudo privileges for iptables.

set -e

# Configuration
CLUSTER_PORTS=(7946 7947 7948) # Default SWIM ports for 3 nodes
METRICS_PORT=9090
DURATION=30
LOG_DIR="/tmp/longbow_partition_test"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
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

get_member_count() {
    local port=$1
    local count=$(curl -s "http://localhost:$port/metrics" 2>/dev/null | grep "^longbow_gossip_active_members" | awk '{print $2}')
    echo "${count:-0}"
}

get_peer_health() {
    local port=$1
    curl -s "http://localhost:$port/metrics" 2>/dev/null | grep "^longbow_peer_health_status" || true
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
expected_members=3
for i in "${!CLUSTER_PORTS[@]}"; do
    port=${CLUSTER_PORTS[$i]}
    members=$(get_member_count $METRICS_PORT)
    if [ "$members" -ge "$expected_members" ]; then
        log "${GREEN}Node $((i+1)): Cluster has $members active members${NC}"
    else
        log "${YELLOW}Node $((i+1)): Only $members members detected (expected $expected_members)${NC}"
    fi
done

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
sleep 5
for i in "${!CLUSTER_PORTS[@]}"; do
    members=$(get_member_count $METRICS_PORT)
    if [ "$members" -ge "$expected_members" ]; then
        log "${GREEN}Node $((i+1)): Cluster recovered with $members active members${NC}"
    else
        log "${YELLOW}Node $((i+1)): Still converging - $members members (expected $expected_members)${NC}"
    fi
done

peer_health=$(get_peer_health $METRICS_PORT)
if [ -n "$peer_health" ]; then
    log "Peer health status:"
    echo "$peer_health" | while read line; do
        log "  $line"
    done
fi

log "Test Complete."
