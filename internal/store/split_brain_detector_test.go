package store

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestSplitBrainDetector_New(t *testing.T) {
	cfg := SplitBrainConfig{
		HeartbeatInterval: 100 * time.Millisecond,
		HeartbeatTimeout:  300 * time.Millisecond,
		MinQuorum:         2,
		FenceOnPartition:  true,
	}
	detector := NewSplitBrainDetector(cfg)
	if detector == nil {
		t.Fatal("Detector should not be nil")
	}
	if detector.IsFenced() {
		t.Error("Should not be fenced initially")
	}
}

func TestSplitBrainDetector_RegisterPeer(t *testing.T) {
	cfg := SplitBrainConfig{
		HeartbeatInterval: 100 * time.Millisecond,
		HeartbeatTimeout:  300 * time.Millisecond,
		MinQuorum:         2,
	}
	detector := NewSplitBrainDetector(cfg)
	detector.RegisterPeer("peer1")
	detector.RegisterPeer("peer2")
	peers := detector.GetPeers()
	if len(peers) != 2 {
		t.Errorf("Expected 2 peers, got %d", len(peers))
	}
}

func TestSplitBrainDetector_Heartbeat(t *testing.T) {
	cfg := SplitBrainConfig{
		HeartbeatInterval: 50 * time.Millisecond,
		HeartbeatTimeout:  150 * time.Millisecond,
		MinQuorum:         1,
	}
	detector := NewSplitBrainDetector(cfg)
	detector.RegisterPeer("peer1")
	detector.RecordHeartbeat("peer1")
	if !detector.IsPeerHealthy("peer1") {
		t.Error("Peer should be healthy after heartbeat")
	}
}

func TestSplitBrainDetector_HeartbeatTimeout(t *testing.T) {
	cfg := SplitBrainConfig{
		HeartbeatInterval: 10 * time.Millisecond,
		HeartbeatTimeout:  30 * time.Millisecond,
		MinQuorum:         1,
	}
	detector := NewSplitBrainDetector(cfg)
	detector.RegisterPeer("peer1")
	detector.RecordHeartbeat("peer1")
	time.Sleep(50 * time.Millisecond)
	if detector.IsPeerHealthy("peer1") {
		t.Error("Peer should be unhealthy after timeout")
	}
}

func TestSplitBrainDetector_QuorumLoss(t *testing.T) {
	cfg := SplitBrainConfig{
		HeartbeatInterval: 10 * time.Millisecond,
		HeartbeatTimeout:  30 * time.Millisecond,
		MinQuorum:         2,
		FenceOnPartition:  true,
	}
	detector := NewSplitBrainDetector(cfg)
	detector.RegisterPeer("peer1")
	detector.RegisterPeer("peer2")
	detector.RecordHeartbeat("peer1")
	// peer2 never sends heartbeat
	time.Sleep(50 * time.Millisecond)
	detector.CheckQuorum()
	if !detector.IsFenced() {
		t.Error("Should be fenced when quorum lost")
	}
}

func TestSplitBrainDetector_QuorumRecovery(t *testing.T) {
	cfg := SplitBrainConfig{
		HeartbeatInterval: 10 * time.Millisecond,
		HeartbeatTimeout:  50 * time.Millisecond,
		MinQuorum:         2,
		FenceOnPartition:  true,
	}
	detector := NewSplitBrainDetector(cfg)
	detector.RegisterPeer("peer1")
	detector.RegisterPeer("peer2")
	// Initially fenced due to no heartbeats
	detector.CheckQuorum()
	if !detector.IsFenced() {
		t.Error("Should be fenced initially")
	}
	// Recover both peers
	detector.RecordHeartbeat("peer1")
	detector.RecordHeartbeat("peer2")
	detector.CheckQuorum()
	if detector.IsFenced() {
		t.Error("Should unfence when quorum recovered")
	}
}

func TestSplitBrainDetector_Metrics(t *testing.T) {
	cfg := SplitBrainConfig{
		HeartbeatInterval: 10 * time.Millisecond,
		HeartbeatTimeout:  30 * time.Millisecond,
		MinQuorum:         1,
	}
	detector := NewSplitBrainDetector(cfg)
	detector.RegisterPeer("peer1")
	detector.RecordHeartbeat("peer1")
	if detector.GetHeartbeatCount() == 0 {
		t.Error("Heartbeat count should be incremented")
	}
}

func TestSplitBrainDetector_ConcurrentHeartbeats(t *testing.T) {
	cfg := SplitBrainConfig{
		HeartbeatInterval: 10 * time.Millisecond,
		HeartbeatTimeout:  100 * time.Millisecond,
		MinQuorum:         1,
	}
	detector := NewSplitBrainDetector(cfg)
	for i := 0; i < 10; i++ {
		detector.RegisterPeer(string(rune('a' + i)))
	}
	var count atomic.Int64
	done := make(chan bool, 100)
	for i := 0; i < 100; i++ {
		go func(idx int) {
			peer := string(rune('a' + (idx % 10)))
			detector.RecordHeartbeat(peer)
			count.Add(1)
			done <- true
		}(i)
	}
	for i := 0; i < 100; i++ {
		<-done
	}
	if count.Load() != 100 {
		t.Errorf("Expected 100 heartbeats, got %d", count.Load())
	}
}

func TestSplitBrainDetector_PartitionCount(t *testing.T) {
	cfg := SplitBrainConfig{
		HeartbeatInterval: 10 * time.Millisecond,
		HeartbeatTimeout:  30 * time.Millisecond,
		MinQuorum:         2,
		FenceOnPartition:  true,
	}
	detector := NewSplitBrainDetector(cfg)
	detector.RegisterPeer("peer1")
	detector.RegisterPeer("peer2")
	detector.CheckQuorum() // Triggers partition
	if detector.GetPartitionCount() == 0 {
		t.Error("Partition count should be incremented")
	}
}
