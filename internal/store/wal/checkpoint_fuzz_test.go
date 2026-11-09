package wal

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"
)

// FuzzCheckpointEpochLifecycle verifies the WAL truncation decision around
// recovery and checkpointing: after recovering to a fuzzed epoch, only epochs
// up to the current one may be truncated, and each completed checkpoint moves
// the truncation window forward by exactly one epoch.
func FuzzCheckpointEpochLifecycle(f *testing.F) {
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(42))
	f.Add(uint64(1_000_000))
	f.Add(uint64(math.MaxUint64 - 2))

	f.Fuzz(func(t *testing.T, epoch uint64) {
		// epoch+2 must not overflow (out of range for this target).
		if epoch > math.MaxUint64-2 {
			return
		}

		c := NewCheckpointCoordinator(CheckpointConfig{
			Interval:       time.Second,
			Timeout:        time.Second,
			QuorumRequired: 1,
		})

		c.RecoverFromEpoch(epoch)
		if got := c.GetEpoch(); got != epoch {
			t.Fatalf("GetEpoch() = %d after RecoverFromEpoch(%d)", got, epoch)
		}
		if !c.ShouldTruncateWAL(epoch) {
			t.Fatalf("must truncate at recovered epoch %d", epoch)
		}
		if c.ShouldTruncateWAL(epoch + 1) {
			t.Fatalf("must not truncate epoch %d beyond current %d", epoch+1, epoch)
		}
		if epoch > 0 && !c.ShouldTruncateWAL(epoch-1) {
			t.Fatalf("must truncate past epoch %d (current %d)", epoch-1, epoch)
		}

		if err := c.InitiateCheckpoint(context.Background()); err != nil {
			t.Fatalf("InitiateCheckpoint: %v", err)
		}
		if got := c.GetEpoch(); got != epoch+1 {
			t.Fatalf("GetEpoch() = %d after checkpoint, want %d", got, epoch+1)
		}
		if got := c.GetCheckpointCount(); got != 1 {
			t.Fatalf("GetCheckpointCount() = %d, want 1", got)
		}
		if !c.ShouldTruncateWAL(epoch + 1) {
			t.Fatalf("epoch %d must be truncatable after checkpoint", epoch+1)
		}
		if c.ShouldTruncateWAL(epoch + 2) {
			t.Fatalf("epoch %d must not be truncatable after one checkpoint", epoch+2)
		}
	})
}

// FuzzCheckpointBarrierQuorum verifies barrier semantics: participants below
// quorum block until their context ends (returning context.Canceled), the
// participant that reaches quorum unblocks immediately with nil, and later
// participants pass through without blocking.
func FuzzCheckpointBarrierQuorum(f *testing.F) {
	f.Add(1, uint64(0))
	f.Add(2, uint64(1))
	f.Add(3, uint64(42))
	f.Add(8, uint64(1_000_000))
	f.Add(0, uint64(0)) // invalid quorum: out of range
	f.Add(9, uint64(0)) // invalid quorum: out of range

	f.Fuzz(func(t *testing.T, quorum int, epoch uint64) {
		if quorum < 1 || quorum > 8 {
			return
		}

		c := NewCheckpointCoordinator(CheckpointConfig{
			Interval:       time.Second,
			Timeout:        time.Second,
			QuorumRequired: quorum,
		})

		// Participants below the quorum barrier must observe their canceled
		// context (their channel can never become ready).
		for i := 0; i < quorum-1; i++ {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := c.WaitForBarrier(ctx, fmt.Sprintf("p-%d", i), epoch)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("participant %d below quorum: err = %v, want context.Canceled", i, err)
			}
		}

		// The participant reaching quorum unblocks immediately with nil.
		if err := c.WaitForBarrier(context.Background(), "p-quorum", epoch); err != nil {
			t.Fatalf("quorum participant blocked: %v", err)
		}

		// Once closed, later participants pass through without blocking.
		if err := c.WaitForBarrier(context.Background(), "p-late", epoch); err != nil {
			t.Fatalf("late participant after quorum: %v", err)
		}
	})
}
