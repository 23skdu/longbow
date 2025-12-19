package store

import (
"context"
"sync"
"sync/atomic"
"time"

"github.com/23skdu/longbow/internal/metrics"
)

// CheckpointConfig configures coordinated checkpointing behavior.
type CheckpointConfig struct {
Interval       time.Duration
Timeout        time.Duration
MinWALSize     int64
QuorumRequired int
}

// CheckpointCoordinator manages distributed checkpoint synchronization.
type CheckpointCoordinator struct {
config          CheckpointConfig
epoch           atomic.Uint64
checkpointCount atomic.Uint64
participants    map[string]bool
barrierCounts   map[uint64]int
barrierCond     *sync.Cond
mu              sync.Mutex
inProgress      atomic.Bool
}

// NewCheckpointCoordinator creates a new checkpoint coordinator.
func NewCheckpointCoordinator(cfg CheckpointConfig) *CheckpointCoordinator {
c := &CheckpointCoordinator{
config:        cfg,
participants:  make(map[string]bool),
barrierCounts: make(map[uint64]int),
}
c.barrierCond = sync.NewCond(&c.mu)
return c
}

// GetEpoch returns the current checkpoint epoch.
func (c *CheckpointCoordinator) GetEpoch() uint64 {
return c.epoch.Load()
}

// GetCheckpointCount returns the total number of checkpoints completed.
func (c *CheckpointCoordinator) GetCheckpointCount() uint64 {
return c.checkpointCount.Load()
}

// RegisterParticipant adds a participant to the checkpoint barrier.
func (c *CheckpointCoordinator) RegisterParticipant(id string) {
c.mu.Lock()
defer c.mu.Unlock()
c.participants[id] = true
}

// UnregisterParticipant removes a participant from the checkpoint barrier.
func (c *CheckpointCoordinator) UnregisterParticipant(id string) {
c.mu.Lock()
defer c.mu.Unlock()
delete(c.participants, id)
}

// InitiateCheckpoint starts a new checkpoint epoch.
func (c *CheckpointCoordinator) InitiateCheckpoint(ctx context.Context) error {
if !c.inProgress.CompareAndSwap(false, true) {
return nil // Already in progress
}
defer c.inProgress.Store(false)

newEpoch := c.epoch.Add(1)
c.checkpointCount.Add(1)

metrics.CheckpointEpoch.Set(float64(newEpoch))
metrics.CheckpointsTotal.Inc()

// Broadcast to all waiting goroutines
c.mu.Lock()
c.barrierCond.Broadcast()
c.mu.Unlock()

return nil
}

// WaitForBarrier waits until quorum participants reach the barrier.
func (c *CheckpointCoordinator) WaitForBarrier(ctx context.Context, participantID string, epoch uint64) error {
c.mu.Lock()
defer c.mu.Unlock()

// Increment barrier count for this epoch
c.barrierCounts[epoch]++
currentCount := c.barrierCounts[epoch]

// Check if quorum is reached
if currentCount >= c.config.QuorumRequired {
c.barrierCond.Broadcast()
metrics.CheckpointBarrierReached.Inc()
return nil
}

// Wait for quorum or timeout
done := make(chan struct{})
go func() {
c.mu.Lock()
for c.barrierCounts[epoch] < c.config.QuorumRequired {
c.barrierCond.Wait()
}
c.mu.Unlock()
close(done)
}()

c.mu.Unlock()
select {
case <-done:
c.mu.Lock()
return nil
case <-ctx.Done():
c.mu.Lock()
metrics.CheckpointTimeoutsTotal.Inc()
return ctx.Err()
}
}

// ShouldTruncateWAL returns true if WAL can be truncated after checkpoint.
func (c *CheckpointCoordinator) ShouldTruncateWAL(epoch uint64) bool {
return epoch <= c.GetEpoch()
}

// RecoverFromEpoch sets the epoch during recovery.
func (c *CheckpointCoordinator) RecoverFromEpoch(epoch uint64) {
c.epoch.Store(epoch)
}
