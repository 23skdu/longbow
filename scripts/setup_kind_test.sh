#!/bin/bash
set -e

CLUSTER_NAME="longbow-test"
IMAGE_NAME="ghcr.io/23skdu/longbow:latest"

echo "Check for Kind cluster..."
if ! kind get clusters | grep -q "$CLUSTER_NAME"; then
    echo "Creating Kind cluster $CLUSTER_NAME..."
    kind create cluster --name "$CLUSTER_NAME"
else
    echo "Cluster $CLUSTER_NAME already exists."
fi

echo "Building Docker image..."
# DOCKER_BUILDKIT=1 docker build -t "$IMAGE_NAME" .
# Assuming standard build from root
docker build -t "$IMAGE_NAME" .

echo "Loading image into Kind..."
kind load docker-image "$IMAGE_NAME" --name "$CLUSTER_NAME"

echo "Installing/Upgrading Helm chart..."
helm upgrade --install longbow ./helm/longbow \
    --set image.repository="ghcr.io/23skdu/longbow" \
    --set image.tag="latest" \
    --set image.pullPolicy=IfNotPresent \
    --kube-context "kind-$CLUSTER_NAME"

echo "Waiting for pods to be ready..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=longbow --timeout=120s --context "kind-$CLUSTER_NAME"

echo "Test complete! Pods are ready."
echo "To clean up: kind delete cluster --name $CLUSTER_NAME"
