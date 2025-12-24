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

# Namespaces: ns-a, ns-b
# Headless service DNS: <pod-name>.<service-name>.<namespace>.svc.cluster.local
# With replicaCount=1 and fullnameOverride=longbow, pod is longbow-0 (statefulset) or random hash (deployment)
# Current chart uses Deployment. Random hash makes static peering hard BEFORE deployment.
# BUT, the Service (headless) points to Pods. 
# "longbow-headless" is the service.
# For Deployment, distinct pods do NOT have stable DNS if not StatefulSet.
# However, I can use the Service DNS if I only have 1 replica per namespace?
# Service DNS: longbow-headless.ns-a.svc.cluster.local -> resolves to IPs of all pods in ns-a.
# If I use this as peer, it might resolve to one IP.
# If I have 1 replica, it resolves to that pod.

PEER_A="longbow-headless.ns-a.svc.cluster.local:7946"
PEER_B="longbow-headless.ns-b.svc.cluster.local:7946"

echo "Deploying to ns-a with peer ns-b..."
kubectl create ns ns-a --context "kind-$CLUSTER_NAME" || true
helm upgrade --install longbow ./helm/longbow \
    --namespace ns-a \
    --set image.repository="ghcr.io/23skdu/longbow" \
    --set image.tag="latest" \
    --set image.pullPolicy=IfNotPresent \
    --set replicaCount=1 \
    --set gossip.enabled=true \
    --set gossip.discovery.provider=static \
    --set gossip.discovery.staticPeers="$PEER_B" \
    --kube-context "kind-$CLUSTER_NAME"

echo "Deploying to ns-b with peer ns-a..."
kubectl create ns ns-b --context "kind-$CLUSTER_NAME" || true
helm upgrade --install longbow ./helm/longbow \
    --namespace ns-b \
    --set image.repository="ghcr.io/23skdu/longbow" \
    --set image.tag="latest" \
    --set image.pullPolicy=IfNotPresent \
    --set replicaCount=1 \
    --set gossip.enabled=true \
    --set gossip.discovery.provider=static \
    --set gossip.discovery.staticPeers="$PEER_A" \
    --kube-context "kind-$CLUSTER_NAME"

echo "Waiting for pods..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=longbow --timeout=120s --namespace ns-a --context "kind-$CLUSTER_NAME"
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=longbow --timeout=120s --namespace ns-b --context "kind-$CLUSTER_NAME"

echo "Verifying cross-namespace mesh..."
# Check logs in ns-a to see if it discovered member from ns-b
echo "--- Logs from ns-a ---"
POD_A=$(kubectl get pods -n ns-a --context "kind-$CLUSTER_NAME" -l app.kubernetes.io/name=longbow -o jsonpath="{.items[0].metadata.name}")
kubectl logs "$POD_A" -n ns-a --context "kind-$CLUSTER_NAME" | grep "Node joined ring" | head -n 5

# Check logs in ns-b
echo "--- Logs from ns-b ---"
POD_B=$(kubectl get pods -n ns-b --context "kind-$CLUSTER_NAME" -l app.kubernetes.io/name=longbow -o jsonpath="{.items[0].metadata.name}")
kubectl logs "$POD_B" -n ns-b --context "kind-$CLUSTER_NAME" | grep "Node joined ring" | head -n 5

if kubectl logs "$POD_A" -n ns-a --context "kind-$CLUSTER_NAME" | grep -q "Node joined ring"; then
    echo "SUCCESS: Mesh formed across namespaces!"
else
    echo "FAILURE: No mesh formed."
    exit 1
fi
