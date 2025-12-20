package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.uber.org/zap"
)

// =============================================================================
// UpdatePeerState Edge Cases
// =============================================================================

func TestUpdatePeerState_NonExistent(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         []string{"peer1:8816"},
		ReplicaFactor: 1,
	}
	mgr := NewReplicationManager(cfg, nil)

	// Should not panic for non-existent peer
	mgr.UpdatePeerState("nonexistent:8816", PeerStateConnected)

	// Verify original peer unchanged
	info, _ := mgr.GetPeerInfo("peer1:8816")
	if info.State != PeerStateDisconnected {
		t.Error("original peer state should be unchanged")
	}
}

func TestUpdatePeerState_LastSeenUpdate(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         []string{"peer1:8816"},
		ReplicaFactor: 1,
	}
	mgr := NewReplicationManager(cfg, nil)

	before := time.Now()
	mgr.UpdatePeerState("peer1:8816", PeerStateConnected)
	after := time.Now()

	info, _ := mgr.GetPeerInfo("peer1:8816")
	if info.LastSeen.Before(before) || info.LastSeen.After(after) {
		t.Error("LastSeen should be set when connected")
	}
}

func TestUpdatePeerState_NoLastSeenOnDisconnect(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         []string{"peer1:8816"},
		ReplicaFactor: 1,
	}
	mgr := NewReplicationManager(cfg, nil)

	// First connect
	mgr.UpdatePeerState("peer1:8816", PeerStateConnected)
	info1, _ := mgr.GetPeerInfo("peer1:8816")
	lastSeen1 := info1.LastSeen

	// Then disconnect - LastSeen should not update
	time.Sleep(10 * time.Millisecond)
	mgr.UpdatePeerState("peer1:8816", PeerStateDisconnected)
	info2, _ := mgr.GetPeerInfo("peer1:8816")

	if !info2.LastSeen.Equal(lastSeen1) {
		t.Error("LastSeen should not update on disconnect")
	}
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

func TestReplicationManager_ConcurrentAccess(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         []string{"peer1:8816", "peer2:8816"},
		ReplicaFactor: 1,
	}
	mgr := NewReplicationManager(cfg, nil)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(4)

		go func() {
			defer wg.Done()
			_ = mgr.GetPeers()
		}()

		go func() {
			defer wg.Done()
			_ = mgr.GetHealthyPeers()
		}()

		go func() {
			defer wg.Done()
			_ = mgr.GetStatus()
		}()

		go func() {
			defer wg.Done()
			mgr.UpdatePeerState("peer1:8816", PeerStateConnected)
		}()
	}
	wg.Wait()
	// Test passes if no race detected
}

func TestReplicationManager_ConcurrentAddRemove(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         []string{"peer1:8816"},
		ReplicaFactor: 1,
	}
	mgr := NewReplicationManager(cfg, nil)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			addr := "dynamic" + string(rune('0'+idx%10)) + ":8816"
			_ = mgr.AddPeer(addr)
			_ = mgr.RemovePeer(addr)
		}()
	}
	wg.Wait()
}

// =============================================================================
// ReplicateDataset Tests
// =============================================================================

func TestReplicateDataset_WithRecords(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         []string{"peer1:8816"},
		ReplicaFactor: 1,
	}
	logger := zap.NewNop()
	mgr := NewReplicationManager(cfg, logger)

	// Create test records
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}},
		nil,
	)
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int64Builder).Append(1)
	rec := bldr.NewRecord() //nolint:staticcheck
	defer rec.Release()

	// NOTE: This is a deprecated type but our codebase uses it
	// nolint:staticcheck
	records := []arrow.RecordBatch{rec}

	err := mgr.ReplicateDataset(context.Background(), "test", records)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestReplicateDataset_ContextCancellation(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         []string{"peer1:8816"},
		ReplicaFactor: 1,
	}
	mgr := NewReplicationManager(cfg, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := mgr.ReplicateDataset(ctx, "test", nil)
	// Current implementation ignores context, just testing it doesn't panic
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// =============================================================================
// GetPeers Edge Cases
// =============================================================================

func TestGetPeers_EmptyManager(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled: false,
		Peers:   []string{},
	}
	mgr := NewReplicationManager(cfg, nil)

	peers := mgr.GetPeers()
	if len(peers) != 0 {
		t.Errorf("expected empty peers, got %d", len(peers))
	}
}

func TestGetHealthyPeers_AllDisconnected(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         []string{"peer1:8816", "peer2:8816"},
		ReplicaFactor: 1,
	}
	mgr := NewReplicationManager(cfg, nil)

	// All peers start disconnected
	healthy := mgr.GetHealthyPeers()
	if len(healthy) != 0 {
		t.Errorf("expected no healthy peers, got %d", len(healthy))
	}
}

func TestGetHealthyPeers_AllConnected(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         []string{"peer1:8816", "peer2:8816"},
		ReplicaFactor: 1,
	}
	mgr := NewReplicationManager(cfg, nil)

	mgr.UpdatePeerState("peer1:8816", PeerStateConnected)
	mgr.UpdatePeerState("peer2:8816", PeerStateConnected)

	healthy := mgr.GetHealthyPeers()
	if len(healthy) != 2 {
		t.Errorf("expected 2 healthy peers, got %d", len(healthy))
	}
}

// =============================================================================
// PeerInfo Edge Cases
// =============================================================================

func TestGetPeerInfo_ReturnsCopy(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         []string{"peer1:8816"},
		ReplicaFactor: 1,
	}
	mgr := NewReplicationManager(cfg, nil)

	info1, _ := mgr.GetPeerInfo("peer1:8816")
	info1.State = PeerStateConnected // Modify the copy

	info2, _ := mgr.GetPeerInfo("peer1:8816")
	if info2.State == PeerStateConnected {
		t.Error("GetPeerInfo should return a copy, not modify original")
	}
}

// =============================================================================
// Status Edge Cases
// =============================================================================

func TestGetStatus_AllStates(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         []string{"p1:8816", "p2:8816", "p3:8816", "p4:8816"},
		ReplicaFactor: 2,
	}
	mgr := NewReplicationManager(cfg, nil)

	mgr.UpdatePeerState("p1:8816", PeerStateConnected)
	mgr.UpdatePeerState("p2:8816", PeerStateConnecting)
	mgr.UpdatePeerState("p3:8816", PeerStateError)
	mgr.UpdatePeerState("p4:8816", PeerStateDisconnected)

	status := mgr.GetStatus()
	if status.ConnectedPeers != 1 {
		t.Errorf("expected 1 connected peer, got %d", status.ConnectedPeers)
	}
	if status.TotalPeers != 4 {
		t.Errorf("expected 4 total peers, got %d", status.TotalPeers)
	}
}

// =============================================================================
// Config Edge Cases
// =============================================================================

func TestReplicationConfig_NegativeReplicaFactor(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         []string{"peer1:8816"},
		ReplicaFactor: -1,
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("expected error for negative replica factor")
	}
}

func TestReplicationConfig_ZeroSyncInterval(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         []string{"peer1:8816"},
		ReplicaFactor: 1,
		SyncInterval:  0, // Zero is valid
	}

	err := cfg.Validate()
	if err != nil {
		t.Errorf("zero sync interval should be valid, got: %v", err)
	}
}

// =============================================================================
// Manager with Logger
// =============================================================================

func TestNewReplicationManager_WithLogger(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         []string{"peer1:8816"},
		ReplicaFactor: 1,
	}
	logger := zap.NewNop()

	mgr := NewReplicationManager(cfg, logger)
	if mgr == nil {
		t.Fatal("expected non-nil manager")
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkGetPeers(b *testing.B) {
	peers := make([]string, 100)
	for i := range peers {
		peers[i] = "peer" + string(rune('0'+i%10)) + ":8816"
	}
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         peers,
		ReplicaFactor: 1,
	}
	mgr := NewReplicationManager(cfg, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mgr.GetPeers()
	}
}

func BenchmarkGetHealthyPeers(b *testing.B) {
	peers := make([]string, 100)
	for i := range peers {
		peers[i] = "peer" + string(rune('0'+i%10)) + ":8816"
	}
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         peers,
		ReplicaFactor: 1,
	}
	mgr := NewReplicationManager(cfg, nil)

	// Mark half as connected
	for i := 0; i < 50; i++ {
		mgr.UpdatePeerState(peers[i], PeerStateConnected)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mgr.GetHealthyPeers()
	}
}

func BenchmarkUpdatePeerState(b *testing.B) {
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         []string{"peer1:8816"},
		ReplicaFactor: 1,
	}
	mgr := NewReplicationManager(cfg, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mgr.UpdatePeerState("peer1:8816", PeerStateConnected)
	}
}
