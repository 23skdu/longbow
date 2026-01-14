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
	epochChannels   map[uint64]chan struct{}
	mu              sync.Mutex
	inProgress      atomic.Bool
}

// NewCheckpointCoordinator creates a new checkpoint coordinator.
func NewCheckpointCoordinator(cfg CheckpointConfig) *CheckpointCoordinator {
	return &CheckpointCoordinator{
		config:        cfg,
		participants:  make(map[string]bool),
		barrierCounts: make(map[uint64]int),
		epochChannels: make(map[uint64]chan struct{}),
	}
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

	return nil
}

// WaitForBarrier waits until quorum participants reach the barrier.
func (c *CheckpointCoordinator) WaitForBarrier(ctx context.Context, participantID string, epoch uint64) error {
	c.mu.Lock()

	// Get or create channel for this epoch
	ch, ok := c.epochChannels[epoch]
	if !ok {
		ch = make(chan struct{})
		c.epochChannels[epoch] = ch
	}

	// Increment barrier count for this epoch
	c.barrierCounts[epoch]++
	currentCount := c.barrierCounts[epoch]

	// Check if quorum is reached
	// Only close if not already closed
	select {
	case <-ch:
		// already closed
	default:
		if currentCount >= c.config.QuorumRequired {
			close(ch)
			metrics.CheckpointBarrierReached.Inc()
		}
	}
	c.mu.Unlock()

	// Wait for quorum or timeout
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
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
