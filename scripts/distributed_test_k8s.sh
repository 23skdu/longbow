#!/bin/bash
set -e

echo "🚀 Starting Distributed Testing on Kubernetes..."

# 1. Image Build
echo "📦 Building Docker image..."
eval $(minikube docker-env)
docker build -t ghcr.io/23skdu/longbow:latest -t ghcr.io/23skdu/longbow:0.1.1 .
# Also tag locally just in case
docker tag ghcr.io/23skdu/longbow:latest longbow:latest

# 2. Namespace Setup
echo "Creating namespaces..."
kubectl create ns node1 --dry-run=client -o yaml | kubectl apply -f -
kubectl create ns node2 --dry-run=client -o yaml | kubectl apply -f -
kubectl create ns node3 --dry-run=client -o yaml | kubectl apply -f -

# 3. Helm Deploy
echo "🚢 Deploying Node 1..."
helm upgrade --install longbow ./helm/longbow --namespace node1 \
    --set replicaCount=1 \
    --set nodeSelector."kubernetes\.io/hostname"=minikube \
    --set gossip.enabled=true \
    --set gossip.discovery.provider=static \
    --set "gossip.discovery.staticPeers=longbow.node2.svc.cluster.local:7946\,longbow.node3.svc.cluster.local:7946" \
    --wait

echo "🚢 Deploying Node 2..."
helm upgrade --install longbow ./helm/longbow --namespace node2 \
    --set replicaCount=1 \
    --set gossip.enabled=true \
    --set gossip.discovery.provider=static \
    --set "gossip.discovery.staticPeers=longbow.node1.svc.cluster.local:7946\,longbow.node3.svc.cluster.local:7946" \
    --wait

echo "🚢 Deploying Node 3..."
helm upgrade --install longbow ./helm/longbow --namespace node3 \
    --set replicaCount=1 \
    --set gossip.enabled=true \
    --set gossip.discovery.provider=static \
    --set "gossip.discovery.staticPeers=longbow.node1.svc.cluster.local:7946\,longbow.node2.svc.cluster.local:7946" \
    --wait

echo "✅ Cluster deployed. Waiting for stability..."
sleep 10

# 4. Port Forwarding
echo "🔌 Setting up port forwarding..."
# Cleanup previous forwards
pkill -f "kubectl port-forward" || true

kubectl port-forward svc/longbow-data -n node1 9001:3000 &
PID1=$!
kubectl port-forward svc/longbow-data -n node2 9002:3000 &
PID2=$!
kubectl port-forward svc/longbow-data -n node3 9003:3000 &
PID3=$!

echo "Waiting for ports..."
sleep 5

# 5. Run Tests
echo "🧪 Running Integration Tests..."
export NODE1_ADDR="localhost:9001"
export NODE2_ADDR="localhost:9002"
export NODE3_ADDR="localhost:9003"

go test -v -tags=integration ./test/distributed/...

# 6. Failure Scenario
echo "💀 Killing Node 2..."
kubectl delete pod -l app.kubernetes.io/name=longbow -n node2 --wait=false

echo "Running Partial Failure Tests..."
# In real test, we would check if Node 1/3 are still responsive.
# The previous go test handles simple connectivity. 
# We run it again.
go test -v -tags=integration -run TestClusterConnectivity/Node1 ./test/distributed/...
go test -v -tags=integration -run TestClusterConnectivity/Node3 ./test/distributed/...

echo "✅ Distributed Testing Complete!"
kill $PID1 $PID2 $PID3
