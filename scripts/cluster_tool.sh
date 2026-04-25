#!/bin/bash
# Longbow Cluster Management Tool
# Consolidated script for Kind/Minikube setup and distributed testing.

set -e

# Defaults
CLUSTER_NAME="longbow-cluster"
IMAGE_NAME="ghcr.io/23skdu/longbow:latest"
NAMESPACE="default"

function usage() {
    echo "Usage: $0 [command] [options]"
    echo ""
    echo "Commands:"
    echo "  up              Provision a Kind cluster"
    echo "  down            Delete the cluster"
    echo "  deploy          Build and load Docker image into cluster"
    echo "  test-dist       Run distributed Kubernetes tests (node1, node2, node3)"
    echo "  test-soak [dur] Run a local soak test with memory profiling"
    echo "  chaos-partition Simulate a network partition (isolates Node 3)"
    echo "  chaos-heal      Heal active partitions"
    echo "  clean           Remove temporary K8S resources"
    echo ""
    echo "Options:"
    echo "  --name NAME    Override cluster name (default: longbow-cluster)"
    echo "  --image IMAGE  Override image name"
}

function check_kind() {
    if ! command -v kind &> /dev/null; then
        echo "Error: 'kind' is not installed."
        exit 1
    fi
}

function cluster_up() {
    check_kind
    if ! kind get clusters | grep -q "$CLUSTER_NAME"; then
        echo "Creating Kind cluster $CLUSTER_NAME..."
        kind create cluster --name "$CLUSTER_NAME"
    else
        echo "Cluster $CLUSTER_NAME already exists."
    fi
}

function build_and_load() {
    echo "Building Docker image $IMAGE_NAME..."
    DOCKER_BUILDKIT=1 docker build -t "$IMAGE_NAME" .
    
    echo "Loading image into Kind ($CLUSTER_NAME)..."
    kind load docker-image "$IMAGE_NAME" --name "$CLUSTER_NAME"
}

function run_dist_test() {
    echo "🚀 Starting Distributed Testing Scenario..."
    
    # Namespace Setup
    echo "Creating namespaces node1, node2, node3..."
    for i in 1 2 3; do
        kubectl create ns node$i --dry-run=client -o yaml | kubectl apply -f -
    done

    # Logic from distributed_test_k8s.sh...
    # (Helm installs, service checks, etc)
    echo "Scenario deployment logic placeholder..."
}

function run_soak_test() {
    DURATION=${1:-1200}
    INTERVAL=120
    echo "Starting soak test for ${DURATION}s..."
    
    # Capture profiles in background
    (
        elapsed=0
        count=0
        mkdir -p profiles
        while [ $elapsed -lt $DURATION ]; do
            TS=$(date +%s)
            echo "Capturing profile ${count}..."
            curl -s -o profiles/heap_${count}_${TS}.pprof http://localhost:9090/debug/pprof/heap || true
            count=$((count + 1))
            sleep $INTERVAL
            elapsed=$((elapsed + INTERVAL))
        done
    ) &
    
    # Trigger load using consolidated benchmark tool
    python3 scripts/benchmark_tool.py run --counts 10000 --duration $DURATION
    echo "Soak test finished."
}

function chaos_partition() {
    echo "Network Partition: Isolating Node 3..."
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        sudo iptables -A INPUT -p tcp --dport 7948 -j DROP
        sudo iptables -A OUTPUT -p tcp --sport 7948 -j DROP
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        pids=$(lsof -t -i :7948)
        for pid in $pids; do kill -STOP $pid; done
        echo "Suspended PIDs: $pids"
    fi
}

function chaos_heal() {
    echo "Healing Network Partition..."
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        sudo iptables -D INPUT -p tcp --dport 7948 -j DROP || true
    elif [[ "$OSTYPE" == "darwin"* ]]; then
         pids=$(lsof -t -i :7948)
         for pid in $pids; do kill -CONT $pid; done
    fi
}

# Simple dispatcher
COMMAND=$1
shift

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --name) CLUSTER_NAME="$2"; shift ;;
        --image) IMAGE_NAME="$2"; shift ;;
        *) echo "Unknown option: $1"; usage; exit 1 ;;
    esac
    shift
done

case $COMMAND in
    up)              cluster_up ;;
    deploy)          build_and_load ;;
    test-dist)       run_dist_test ;;
    test-soak)       run_soak_test "$1" ;;
    chaos-partition) chaos_partition ;;
    chaos-heal)      chaos_heal ;;
    down)            kind delete cluster --name "$CLUSTER_NAME" ;;
    *)         usage; exit 1 ;;
esac
