package store

import (
"sync"
"sync/atomic"
"time"

"github.com/23skdu/longbow/internal/metrics"
)

// SplitBrainConfig configures split-brain detection behavior.
type SplitBrainConfig struct {
HeartbeatInterval time.Duration
HeartbeatTimeout  time.Duration
MinQuorum         int
FenceOnPartition  bool
}

// peerState tracks the health state of a peer.
type peerState struct {
lastHeartbeat time.Time
}

// SplitBrainDetector monitors cluster health and detects network partitions.
type SplitBrainDetector struct {
config          SplitBrainConfig
peers           map[string]*peerState
fenced          atomic.Bool
heartbeatCount  atomic.Uint64
partitionCount  atomic.Uint64
mu              sync.RWMutex
}

// NewSplitBrainDetector creates a new split-brain detector.
func NewSplitBrainDetector(cfg SplitBrainConfig) *SplitBrainDetector {
return &SplitBrainDetector{
config: cfg,
peers:  make(map[string]*peerState),
}
}

// RegisterPeer adds a peer to monitor.
func (d *SplitBrainDetector) RegisterPeer(id string) {
d.mu.Lock()
defer d.mu.Unlock()
d.peers[id] = &peerState{}
}

// UnregisterPeer removes a peer from monitoring.
func (d *SplitBrainDetector) UnregisterPeer(id string) {
d.mu.Lock()
defer d.mu.Unlock()
delete(d.peers, id)
}

// GetPeers returns all registered peer IDs.
func (d *SplitBrainDetector) GetPeers() []string {
d.mu.RLock()
defer d.mu.RUnlock()
peers := make([]string, 0, len(d.peers))
for id := range d.peers {
peers = append(peers, id)
}
return peers
}

// RecordHeartbeat updates the last heartbeat time for a peer.
func (d *SplitBrainDetector) RecordHeartbeat(id string) {
d.mu.Lock()
defer d.mu.Unlock()
if peer, ok := d.peers[id]; ok {
peer.lastHeartbeat = time.Now()
d.heartbeatCount.Add(1)
metrics.SplitBrainHeartbeatsTotal.Inc()
}
}

// IsPeerHealthy returns true if peer has sent heartbeat within timeout.
func (d *SplitBrainDetector) IsPeerHealthy(id string) bool {
d.mu.RLock()
defer d.mu.RUnlock()
peer, ok := d.peers[id]
if !ok {
return false
}
return time.Since(peer.lastHeartbeat) < d.config.HeartbeatTimeout
}

// GetHealthyPeerCount returns the number of healthy peers.
func (d *SplitBrainDetector) GetHealthyPeerCount() int {
d.mu.RLock()
defer d.mu.RUnlock()
count := 0
for _, peer := range d.peers {
if time.Since(peer.lastHeartbeat) < d.config.HeartbeatTimeout {
count++
}
}
return count
}

// CheckQuorum evaluates if quorum is maintained and updates fencing state.
func (d *SplitBrainDetector) CheckQuorum() bool {
healthyCount := d.GetHealthyPeerCount()
hasQuorum := healthyCount >= d.config.MinQuorum

metrics.SplitBrainHealthyPeers.Set(float64(healthyCount))

if !hasQuorum && d.config.FenceOnPartition {
if !d.fenced.Load() {
d.fenced.Store(true)
d.partitionCount.Add(1)
metrics.SplitBrainPartitionsTotal.Inc()
metrics.SplitBrainFenced.Set(1)
}
} else if hasQuorum {
if d.fenced.Load() {
d.fenced.Store(false)
metrics.SplitBrainFenced.Set(0)
}
}

return hasQuorum
}

// IsFenced returns true if the node is fenced due to partition.
func (d *SplitBrainDetector) IsFenced() bool {
return d.fenced.Load()
}

// GetHeartbeatCount returns the total heartbeat count.
func (d *SplitBrainDetector) GetHeartbeatCount() uint64 {
return d.heartbeatCount.Load()
}

// GetPartitionCount returns the number of detected partitions.
func (d *SplitBrainDetector) GetPartitionCount() uint64 {
return d.partitionCount.Load()
}
