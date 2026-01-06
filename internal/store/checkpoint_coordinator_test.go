package store


import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestCheckpointCoordinator_New(t *testing.T) {
	cfg := CheckpointConfig{
		Interval:       1 * time.Minute,
		Timeout:        30 * time.Second,
		MinWALSize:     1024 * 1024,
		QuorumRequired: 2,
	}
	coord := NewCheckpointCoordinator(cfg)
	if coord == nil {
		t.Fatal("Coordinator should not be nil")
	}
	if coord.GetEpoch() != 0 {
		t.Error("Initial epoch should be 0")
	}
}

func TestCheckpointCoordinator_InitiateCheckpoint(t *testing.T) {
	cfg := CheckpointConfig{
		Interval:       1 * time.Minute,
		Timeout:        100 * time.Millisecond,
		QuorumRequired: 1,
	}
	coord := NewCheckpointCoordinator(cfg)
	ctx := context.Background()
	err := coord.InitiateCheckpoint(ctx)
	if err != nil {
		t.Fatalf("InitiateCheckpoint failed: %v", err)
	}
	if coord.GetEpoch() != 1 {
		t.Errorf("Epoch should be 1 after checkpoint, got %d", coord.GetEpoch())
	}
}

func TestCheckpointCoordinator_BarrierSync(t *testing.T) {
	cfg := CheckpointConfig{
		Interval:       1 * time.Minute,
		Timeout:        100 * time.Millisecond,
		QuorumRequired: 3,
	}
	coord := NewCheckpointCoordinator(cfg)
	coord.RegisterParticipant("peer1")
	coord.RegisterParticipant("peer2")
	coord.RegisterParticipant("local")

	done := make(chan bool, 3)
	for _, peer := range []string{"peer1", "peer2", "local"} {
		go func(p string) {
			ctx := context.Background()
			err := coord.WaitForBarrier(ctx, p, 1)
			if err != nil {
				t.Errorf("Barrier wait failed for %s: %v", p, err)
			}
			done <- true
		}(peer)
	}

	for i := 0; i < 3; i++ {
		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
			t.Error("Barrier sync timed out")
		}
	}
}

func TestCheckpointCoordinator_Timeout(t *testing.T) {
	cfg := CheckpointConfig{
		Interval:       1 * time.Minute,
		Timeout:        50 * time.Millisecond,
		QuorumRequired: 2,
	}
	coord := NewCheckpointCoordinator(cfg)
	coord.RegisterParticipant("peer1")
	coord.RegisterParticipant("peer2")

	// Only one participant reaches barrier - should timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := coord.WaitForBarrier(ctx, "peer1", 1)
	if err == nil {
		t.Error("Expected timeout error when quorum not reached")
	}
}

func TestCheckpointCoordinator_Metrics(t *testing.T) {
	cfg := CheckpointConfig{
		Interval:       1 * time.Minute,
		Timeout:        100 * time.Millisecond,
		QuorumRequired: 1,
	}
	coord := NewCheckpointCoordinator(cfg)
	ctx := context.Background()
	_ = coord.InitiateCheckpoint(ctx)
	if coord.GetCheckpointCount() == 0 {
		t.Error("Checkpoint count should be incremented")
	}
}

func TestCheckpointCoordinator_ConcurrentCheckpoints(t *testing.T) {
	cfg := CheckpointConfig{
		Interval:       1 * time.Minute,
		Timeout:        100 * time.Millisecond,
		QuorumRequired: 1,
	}
	coord := NewCheckpointCoordinator(cfg)
	var successCount atomic.Int64
	done := make(chan bool, 5)

	for i := 0; i < 5; i++ {
		go func() {
			ctx := context.Background()
			if err := coord.InitiateCheckpoint(ctx); err == nil {
				successCount.Add(1)
			}
			done <- true
		}()
	}

	for i := 0; i < 5; i++ {
		<-done
	}
	// At least one should succeed
	if successCount.Load() == 0 {
		t.Error("At least one checkpoint should succeed")
	}
}

func TestCheckpointCoordinator_WALTruncation(t *testing.T) {
	cfg := CheckpointConfig{
		Interval:       1 * time.Minute,
		Timeout:        100 * time.Millisecond,
		QuorumRequired: 1,
		MinWALSize:     0, // Allow immediate truncation
	}
	coord := NewCheckpointCoordinator(cfg)
	ctx := context.Background()
	_ = coord.InitiateCheckpoint(ctx)
	truncated := coord.ShouldTruncateWAL(1)
	if !truncated {
		t.Error("WAL should be truncatable after checkpoint")
	}
}

func TestCheckpointCoordinator_Recovery(t *testing.T) {
	cfg := CheckpointConfig{
		Interval:       1 * time.Minute,
		Timeout:        100 * time.Millisecond,
		QuorumRequired: 1,
	}
	coord := NewCheckpointCoordinator(cfg)
	ctx := context.Background()
	_ = coord.InitiateCheckpoint(ctx)
	epoch := coord.GetEpoch()

	// Simulate recovery by creating new coordinator and setting epoch
	coord2 := NewCheckpointCoordinator(cfg)
	coord2.RecoverFromEpoch(epoch)
	if coord2.GetEpoch() != epoch {
		t.Errorf("Recovered epoch mismatch: got %d, want %d", coord2.GetEpoch(), epoch)
	}
}
