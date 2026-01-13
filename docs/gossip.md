# Gossip Protocol - SWIM-Based Mesh Networking

## Overview

Longbow uses a **SWIM (Scalable Weakly-consistent Infection-style Process Group Membership)** gossip
protocol for distributed mesh networking. This enables:

- **Failure Detection**: Automatically detect and handle node failures
- **Membership Management**: Maintain consistent view of cluster members
- **Metadata Propagation**: Efficiently spread updates across the cluster
- **Decentralized Coordination**: No single point of failure
- **Topology Driver**: Drives the Consistent Hash Ring via `EventDelegate`

## Architecture

### Protocol Components

```text
┌─────────────────────────────────────────────────────────────┐
│                    Gossip Protocol (UDP)                     │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Probe Loop   │  │ Suspicion    │  │ Listen Loop  │      │
│  │ (Direct Ping)│  │ Management   │  │ (Handle Msgs)│      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
├─────────────────────────────────────────────────────────────┤
│              Member State (Alive/Suspect/Dead)               │
├─────────────────────────────────────────────────────────────┤
│           Event Delegate (Ring Manager Notification)         │
├─────────────────────────────────────────────────────────────┤
│           Discovery (K8s / DNS / Static Seeds)               │
└─────────────────────────────────────────────────────────────┘
```

### SWIM Failure Detection

1. **Direct Ping**: Probe random member every `protocolPeriod` (default: 200ms)
2. **Indirect Ping**: If no ACK within `ackTimeout` (100ms), ask K neighbors to ping
3. **Suspicion**: If indirect ping fails, mark member as **Suspect**
4. **Confirmation**: After `suspicionTimeout` (5s), mark as **Dead**

This provides:

- **Fast failure detection** (< 1 second typical)
- **Low false positive rate** (indirect pings reduce network partition issues)
- **Scalable** (O(log N) message complexity)

## Configuration

### Helm Values

```yaml
gossip:
  enabled: true
  port: 7946                    # UDP port for gossip
  protocolPeriod: "200ms"       # Probe frequency
  ackTimeout: "100ms"           # Direct ping timeout
  suspicionTimeout: "5s"        # Suspect → Dead timeout
  discovery:
    provider: "k8s"             # k8s, dns, or static
    namespace: ""               # K8s namespace (empty = same as pod)
    labelSelector: "app.kubernetes.io/name=longbow"
    staticPeers: ""             # For static: "host1:7946,host2:7946"
    dnsRecord: "_gossip._udp.longbow.default.svc.cluster.local"
```

### Environment Variables

The Helm chart configures gossip via environment variables:

```bash
LONGBOW_GOSSIP_ENABLED=true
LONGBOW_GOSSIP_PORT=7946
LONGBOW_GOSSIP_PROTOCOL_PERIOD=200ms
LONGBOW_GOSSIP_ACK_TIMEOUT=100ms
LONGBOW_GOSSIP_SUSPICION_TIMEOUT=5s
LONGBOW_GOSSIP_DISCOVERY_PROVIDER=k8s
```

## Discovery Providers

### Kubernetes (Recommended)

Uses Kubernetes API to discover peers via label selector:

```yaml
gossip:
  discovery:
    provider: "k8s"
    labelSelector: "app.kubernetes.io/name=longbow"
```

**Pros:**

- Automatic peer discovery
- Works with StatefulSets and Deployments
- No manual configuration needed

**Cons:**

- Requires RBAC permissions (ServiceAccount with pod list access)

### DNS (SRV Records)

Queries DNS SRV records for peer list:

```yaml
gossip:
  discovery:
    provider: "dns"
    dnsRecord: "_gossip._udp.longbow.default.svc.cluster.local"
```

**Pros:**

- Standard DNS-based discovery
- Works with headless services

**Cons:**

- Requires SRV record configuration
- DNS propagation delays

### Static Seeds

Manual peer list configuration:

```yaml
gossip:
  discovery:
    provider: "static"
    staticPeers: "longbow-0.longbow:7946,longbow-1.longbow:7946,longbow-2.longbow:7946"
```

**Pros:**

- Simple, predictable
- No external dependencies

**Cons:**

- Manual configuration required
- Doesn't adapt to scaling

## Networking Requirements

### Ports

- **UDP 7946**: Gossip protocol communication (configurable)

### Firewall Rules

Ensure UDP traffic is allowed between all Longbow pods:

```yaml
# Example NetworkPolicy
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: longbow-gossip
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: longbow
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: longbow
      ports:
        - protocol: UDP
          port: 7946
```

## Monitoring

### Metrics

Gossip protocol exposes Prometheus metrics:

```text
# Active cluster members
gossip_active_members{} 

# Ping statistics
gossip_pings_total{direction="sent"}
gossip_pings_total{direction="received"}

# Member state transitions
gossip_member_state_changes_total{from="alive",to="suspect"}
gossip_member_state_changes_total{from="suspect",to="dead"}
```

### Health Checks

Query mesh status via Flight API:

```bash
# Get cluster membership
grpcurl -plaintext \
  -d '{"type":"MeshStatus"}' \
  localhost:3001 \
  arrow.flight.protocol.FlightService/DoAction

# Get node identity
grpcurl -plaintext \
  -d '{"type":"MeshIdentity"}' \
  localhost:3001 \
  arrow.flight.protocol.FlightService/DoAction
```

## Performance Characteristics

### Message Batching

The gossip protocol batches updates to reduce syscalls:

- **Batch Size**: Up to 5 member updates per UDP packet
- **Piggybacking**: Updates ride on ping/ack messages
- **MTU Awareness**: Respects 1400-byte packet limit

### Scalability

| Cluster Size | Probe Rate | Network Overhead |
|--------------|------------|------------------|
| 10 nodes     | 5 probes/s | ~5 KB/s per node |
| 100 nodes    | 50 probes/s| ~50 KB/s per node|
| 1000 nodes   | 500 probes/s| ~500 KB/s per node|

**Note**: Overhead scales linearly with cluster size due to SWIM's efficient design.

## Troubleshooting

### Nodes Not Discovering Each Other

**Symptoms**: `gossip_active_members` stays at 1

**Solutions**:

1. Check UDP port 7946 is accessible between pods
2. Verify discovery provider configuration
3. For K8s: Ensure ServiceAccount has pod list permissions
4. Check logs for discovery errors:

   ```bash
   kubectl logs -l app.kubernetes.io/name=longbow | grep -i discovery
   ```

### High False Positive Rate

**Symptoms**: Nodes frequently marked suspect/dead

**Solutions**:

1. Increase `ackTimeout` (e.g., 200ms for high-latency networks)
2. Increase `suspicionTimeout` (e.g., 10s for unstable networks)
3. Check network latency between pods
4. Verify CPU isn't saturated (delays ACK processing)

### Split Brain

**Symptoms**: Cluster partitions into multiple groups

**Solutions**:

1. Ensure all pods can reach each other (check NetworkPolicies)
2. Use K8s discovery instead of static peers
3. Verify DNS resolution for headless services
4. Check for network partitions in infrastructure

## Example Deployment

### StatefulSet with Gossip

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: longbow
spec:
  serviceName: longbow
  replicas: 3
  selector:
    matchLabels:
      app.kubernetes.io/name: longbow
  template:
    metadata:
      labels:
        app.kubernetes.io/name: longbow
    spec:
      containers:
      - name: longbow
        image: ghcr.io/23skdu/longbow:latest
        env:
        - name: LONGBOW_GOSSIP_ENABLED
          value: "true"
        - name: LONGBOW_GOSSIP_PORT
          value: "7946"
        - name: LONGBOW_GOSSIP_DISCOVERY_PROVIDER
          value: "k8s"
        ports:
        - name: gossip
          containerPort: 7946
          protocol: UDP
---
apiVersion: v1
kind: Service
metadata:
  name: longbow
spec:
  clusterIP: None  # Headless service for StatefulSet
  selector:
    app.kubernetes.io/name: longbow
  ports:
  - name: gossip
    port: 7946
    protocol: UDP
```

## Best Practices

1. **Use K8s Discovery**: Simplest and most reliable in Kubernetes
2. **Tune for Network**: Adjust timeouts based on network latency
3. **Monitor Metrics**: Watch `gossip_active_members` and state transitions
4. **Test Failure Scenarios**: Verify detection works during pod restarts
5. **Network Policies**: Ensure UDP 7946 is allowed between pods
6. **Resource Limits**: Gossip is lightweight but needs CPU for timely ACKs

## References

- [SWIM Paper](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf) - Original SWIM protocol
- [Metrics Documentation](metrics.md) - Full list of gossip metrics
- [Helm Chart](helm.md) - Deployment configuration
