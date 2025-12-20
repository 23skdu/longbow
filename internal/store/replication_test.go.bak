package store

import (
	"context"
	"testing"
	"time"
)

// TestReplicationConfigValidation tests configuration validation
func TestReplicationConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  ReplicationConfig
		wantErr bool
	}{
		{
			name: "valid config with peers",
			config: ReplicationConfig{
				Enabled:        true,
				Peers:          []string{"localhost:8816", "localhost:8817"},
				ReplicaFactor:  2,
				SyncInterval:   10 * time.Second,
				ConnectTimeout: 5 * time.Second,
				MaxRetries:     3,
			},
			wantErr: false,
		},
		{
			name: "disabled config skips validation",
			config: ReplicationConfig{
				Enabled: false,
			},
			wantErr: false,
		},
		{
			name: "enabled but no peers",
			config: ReplicationConfig{
				Enabled:       true,
				Peers:         []string{},
				ReplicaFactor: 2,
			},
			wantErr: true,
		},
		{
			name: "replica factor exceeds peers",
			config: ReplicationConfig{
				Enabled:       true,
				Peers:         []string{"localhost:8816"},
				ReplicaFactor: 3,
			},
			wantErr: true,
		},
		{
			name: "zero replica factor",
			config: ReplicationConfig{
				Enabled:       true,
				Peers:         []string{"localhost:8816"},
				ReplicaFactor: 0,
			},
			wantErr: true,
		},
		{
			name: "negative sync interval",
			config: ReplicationConfig{
				Enabled:       true,
				Peers:         []string{"localhost:8816"},
				ReplicaFactor: 1,
				SyncInterval:  -1 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "empty peer address",
			config: ReplicationConfig{
				Enabled:       true,
				Peers:         []string{"localhost:8816", ""},
				ReplicaFactor: 1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestNewReplicationConfigDefaults tests default configuration
func TestNewReplicationConfigDefaults(t *testing.T) {
	cfg := NewReplicationConfig()

	if cfg.Enabled {
		t.Error("Expected Enabled to be false by default")
	}
	if cfg.ReplicaFactor != 1 {
		t.Errorf("Expected ReplicaFactor=1, got %d", cfg.ReplicaFactor)
	}
	if cfg.SyncInterval != 30*time.Second {
		t.Errorf("Expected SyncInterval=30s, got %v", cfg.SyncInterval)
	}
	if cfg.ConnectTimeout != 10*time.Second {
		t.Errorf("Expected ConnectTimeout=10s, got %v", cfg.ConnectTimeout)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("Expected MaxRetries=3, got %d", cfg.MaxRetries)
	}
}

// TestNewReplicationManager tests manager creation
func TestNewReplicationManager(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled:        true,
		Peers:          []string{"localhost:8816", "localhost:8817"},
		ReplicaFactor:  2,
		SyncInterval:   10 * time.Second,
		ConnectTimeout: 5 * time.Second,
	}

	mgr := NewReplicationManager(cfg, nil)
	if mgr == nil {
		t.Fatal("Expected non-nil ReplicationManager")
	}

	if !mgr.IsEnabled() {
		t.Error("Expected manager to be enabled")
	}

	peers := mgr.GetPeers()
	if len(peers) != 2 {
		t.Errorf("Expected 2 peers, got %d", len(peers))
	}
}

// TestReplicationManagerDisabled tests disabled manager behavior
func TestReplicationManagerDisabled(t *testing.T) {
	cfg := ReplicationConfig{Enabled: false}
	mgr := NewReplicationManager(cfg, nil)

	if mgr.IsEnabled() {
		t.Error("Expected manager to be disabled")
	}

	// Operations should be no-ops when disabled
	err := mgr.ReplicateDataset(context.Background(), "test", nil)
	if err != nil {
		t.Errorf("Expected no error when disabled, got %v", err)
	}
}

// TestPeerStateTransitions tests peer connection state tracking
func TestPeerStateTransitions(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         []string{"peer1:8816", "peer2:8816"},
		ReplicaFactor: 1,
		SyncInterval:  10 * time.Second,
	}

	mgr := NewReplicationManager(cfg, nil)

	// Initially all peers should be disconnected
	status := mgr.GetStatus()
	if status.ConnectedPeers != 0 {
		t.Errorf("Expected 0 connected peers initially, got %d", status.ConnectedPeers)
	}
	if status.TotalPeers != 2 {
		t.Errorf("Expected 2 total peers, got %d", status.TotalPeers)
	}

	// Test state update
	mgr.UpdatePeerState("peer1:8816", PeerStateConnected)
	status = mgr.GetStatus()
	if status.ConnectedPeers != 1 {
		t.Errorf("Expected 1 connected peer, got %d", status.ConnectedPeers)
	}

	// Test disconnect
	mgr.UpdatePeerState("peer1:8816", PeerStateDisconnected)
	status = mgr.GetStatus()
	if status.ConnectedPeers != 0 {
		t.Errorf("Expected 0 connected peers after disconnect, got %d", status.ConnectedPeers)
	}
}

// TestPeerState constants
func TestPeerStateConstants(t *testing.T) {
	if PeerStateDisconnected != 0 {
		t.Error("PeerStateDisconnected should be 0")
	}
	if PeerStateConnecting != 1 {
		t.Error("PeerStateConnecting should be 1")
	}
	if PeerStateConnected != 2 {
		t.Error("PeerStateConnected should be 2")
	}
	if PeerStateError != 3 {
		t.Error("PeerStateError should be 3")
	}
}

// TestReplicationStatus tests status reporting
func TestReplicationStatus(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         []string{"peer1:8816", "peer2:8816", "peer3:8816"},
		ReplicaFactor: 2,
		SyncInterval:  10 * time.Second,
	}

	mgr := NewReplicationManager(cfg, nil)

	status := mgr.GetStatus()
	if !status.Enabled {
		t.Error("Expected status.Enabled to be true")
	}
	if status.TotalPeers != 3 {
		t.Errorf("Expected TotalPeers=3, got %d", status.TotalPeers)
	}
	if status.ReplicaFactor != 2 {
		t.Errorf("Expected ReplicaFactor=2, got %d", status.ReplicaFactor)
	}
}

// TestAddRemovePeer tests dynamic peer management
func TestAddRemovePeer(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         []string{"peer1:8816"},
		ReplicaFactor: 1,
		SyncInterval:  10 * time.Second,
	}

	mgr := NewReplicationManager(cfg, nil)

	// Add peer
	err := mgr.AddPeer("peer2:8816")
	if err != nil {
		t.Errorf("AddPeer failed: %v", err)
	}

	peers := mgr.GetPeers()
	if len(peers) != 2 {
		t.Errorf("Expected 2 peers after add, got %d", len(peers))
	}

	// Add duplicate should fail
	err = mgr.AddPeer("peer1:8816")
	if err == nil {
		t.Error("Expected error when adding duplicate peer")
	}

	// Remove peer
	err = mgr.RemovePeer("peer1:8816")
	if err != nil {
		t.Errorf("RemovePeer failed: %v", err)
	}

	peers = mgr.GetPeers()
	if len(peers) != 1 {
		t.Errorf("Expected 1 peer after remove, got %d", len(peers))
	}

	// Remove non-existent should fail
	err = mgr.RemovePeer("nonexistent:8816")
	if err == nil {
		t.Error("Expected error when removing non-existent peer")
	}
}

// TestReplicationModeConstants tests replication mode values
func TestReplicationModeConstants(t *testing.T) {
	if ReplicationModeAsync != 0 {
		t.Error("ReplicationModeAsync should be 0")
	}
	if ReplicationModeSync != 1 {
		t.Error("ReplicationModeSync should be 1")
	}
}

// TestHealthyPeerSelection tests selecting healthy peers for replication
func TestHealthyPeerSelection(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         []string{"peer1:8816", "peer2:8816", "peer3:8816"},
		ReplicaFactor: 2,
		SyncInterval:  10 * time.Second,
	}

	mgr := NewReplicationManager(cfg, nil)

	// Mark some peers as connected
	mgr.UpdatePeerState("peer1:8816", PeerStateConnected)
	mgr.UpdatePeerState("peer2:8816", PeerStateConnected)
	mgr.UpdatePeerState("peer3:8816", PeerStateError)

	healthy := mgr.GetHealthyPeers()
	if len(healthy) != 2 {
		t.Errorf("Expected 2 healthy peers, got %d", len(healthy))
	}

	// Verify peer3 is not in healthy list
	for _, p := range healthy {
		if p == "peer3:8816" {
			t.Error("peer3 should not be in healthy list")
		}
	}
}

// TestGetPeerInfo tests getting detailed peer information
func TestGetPeerInfo(t *testing.T) {
	cfg := ReplicationConfig{
		Enabled:       true,
		Peers:         []string{"peer1:8816"},
		ReplicaFactor: 1,
		SyncInterval:  10 * time.Second,
	}

	mgr := NewReplicationManager(cfg, nil)

	info, err := mgr.GetPeerInfo("peer1:8816")
	if err != nil {
		t.Fatalf("GetPeerInfo failed: %v", err)
	}

	if info.Address != "peer1:8816" {
		t.Errorf("Expected address peer1:8816, got %s", info.Address)
	}

	// Non-existent peer
	_, err = mgr.GetPeerInfo("nonexistent:8816")
	if err == nil {
		t.Error("Expected error for non-existent peer")
	}
}
