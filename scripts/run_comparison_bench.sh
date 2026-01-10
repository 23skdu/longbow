#!/bin/bash
set -e

# Configuration
MEMORY_BYTES="6442450944"
REPLICAS=3

function setup_port_forwards() {
    echo "Setting up port forwards..."
    pkill -f "kubectl port-forward" || true
    sleep 2
    
    PODS=($(kubectl get pods -l app.kubernetes.io/name=longbow -o jsonpath="{.items[*].metadata.name}"))
    if [ ${#PODS[@]} -lt 3 ]; then
        echo "Error: Need 3 pods, found ${#PODS[@]}"
        exit 1
    fi
    
    # Pod 0: gRPC=3000, Meta=3001, Metrics=9090
    kubectl port-forward ${PODS[0]} 3000:3000 3001:3001 9090:9090 >/dev/null 2>&1 &
    
    # Pod 1: gRPC=3010, Meta=3011, Metrics=9091
    kubectl port-forward ${PODS[1]} 3010:3000 3011:3001 9091:9090 >/dev/null 2>&1 &
    
    # Pod 2: gRPC=3020, Meta=3021, Metrics=9092
    kubectl port-forward ${PODS[2]} 3020:3000 3021:3001 9092:9090 >/dev/null 2>&1 &
    
    echo "Port forwards started."
    sleep 5
}

function deploy() {
    local float16=$1
    echo "Deploying Longbow with Float16=${float16}..."
    
    helm upgrade --install longbow ./helm/longbow \
        --set replicaCount=${REPLICAS} \
        --set config.maxMemory="${MEMORY_BYTES}" \
        --set hnsw.float16Enabled=${float16} \
        --set resources.limits.memory="6Gi" \
        --set resources.requests.memory="6Gi" \
        --wait
        
    kubectl rollout status deployment/longbow
    echo "Deployment ready."
    sleep 30
}

function run_tests() {
    local label=$1
    
    setup_port_forwards
    
    # 1. Benchmark (Throughput/QPS)
    echo "Running Benchmark (15k)..."
    python3 scripts/benchmark_12gb.py
    mv benchmark_12gb_final.json "results_${label}.json"
    
    # 2. Soak Test (10 mins, PProf)
    echo "Running Soak Test (10m)..."
    # Ensure profile dir exists
    mkdir -p "profiles_${label}"
    
    python3 scripts/soak_test.py \
        --uris grpc://localhost:3000,grpc://localhost:3010,grpc://localhost:3020 \
        --duration 600 \
        --concurrency 6 \
        --pprof-urls http://localhost:9090,http://localhost:9091,http://localhost:9092 \
        --output-dir "profiles_${label}"
        
    pkill -f "kubectl port-forward"
}

# Run 1: FP16 Enabled
deploy "true"
run_tests "fp16_enabled"

# Run 2: FP16 Disabled
deploy "false"
run_tests "fp16_disabled"

echo "Benchmark comparison complete."
