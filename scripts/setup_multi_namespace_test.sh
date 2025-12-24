#!/bin/bash
set -e

CLUSTER_NAME="longbow-multi"
IMAGE_NAME="ghcr.io/23skdu/longbow:latest"

echo "Check for Kind cluster..."
if ! kind get clusters | grep -q "$CLUSTER_NAME"; then
    echo "Creating Kind cluster $CLUSTER_NAME..."
    kind create cluster --name "$CLUSTER_NAME"
else
    echo "Cluster $CLUSTER_NAME already exists."
fi

echo "Building Docker image..."
DOCKER_BUILDKIT=1 docker build -t "$IMAGE_NAME" .

echo "Loading image into Kind..."
kind load docker-image "$IMAGE_NAME" --name "$CLUSTER_NAME"

NAMESPACES=("ns-a" "ns-b" "ns-c")

for NS in "${NAMESPACES[@]}"; do
    echo "Setting up namespace $NS..."
    kubectl create ns "$NS" --context "kind-$CLUSTER_NAME" || true
    
    echo "Installing Longbow in $NS..."
    # Enable Gossip and K8s discovery. 
    # Use replicaCount=2 to prove they find each other.
    helm upgrade --install longbow ./helm/longbow \
        --namespace "$NS" \
        --set image.repository="ghcr.io/23skdu/longbow" \
        --set image.tag="latest" \
        --set image.pullPolicy=IfNotPresent \
        --set replicaCount=2 \
        --set gossip.enabled=true \
        --set gossip.discovery.provider=k8s \
        --set gossip.discovery.labelSelector="app.kubernetes.io/name=longbow" \
        --kube-context "kind-$CLUSTER_NAME"
done

echo "Waiting for pods to be ready..."
for NS in "${NAMESPACES[@]}"; do
    echo "Waiting for deployment in $NS..."
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=longbow --timeout=120s --namespace "$NS" --context "kind-$CLUSTER_NAME"
done

echo "Verifying discovery in logs..."
for NS in "${NAMESPACES[@]}"; do
    echo "Checking logs in $NS..."
    # Get the *first* pod name
    POD_NAME=$(kubectl get pods -n "$NS" --context "kind-$CLUSTER_NAME" -l app.kubernetes.io/name=longbow -o jsonpath="{.items[0].metadata.name}")
    
    # Check for "Node joined ring" messages. In a 2-node cluster, we should see at least one other node joining.
    if kubectl logs "$POD_NAME" -n "$NS" --context "kind-$CLUSTER_NAME" | grep -q "Node joined ring"; then
        echo "✅ Discovery success in $NS"
        kubectl logs "$POD_NAME" -n "$NS" --context "kind-$CLUSTER_NAME" | grep "Node joined ring" | head -n 5
    else
        echo "❌ Discovery FAILED in $NS"
        kubectl logs "$POD_NAME" -n "$NS" --context "kind-$CLUSTER_NAME"
        exit 1
    fi
done

echo "Multi-namespace discovery test complete!"
echo "To clean up: kind delete cluster --name $CLUSTER_NAME"
