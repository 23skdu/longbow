package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/mesh"
	"github.com/apache/arrow-go/v18/arrow/flight"
)

// ReplicatorPeerPool defines the interface for peer-to-peer operations used by the replicator.
type ReplicatorPeerPool interface {
	DoAction(ctx context.Context, host string, action *flight.Action) error
}

// MeshProvider defines the interface for membership discovery.
type MeshProvider interface {
	GetMembers() []mesh.Member
}

// FlightWALReplicator implements storage.WALReplicator using the FlightClientPool
type FlightWALReplicator struct {
	pool    ReplicatorPeerPool
	mesh    MeshProvider
	timeout time.Duration
}

// NewFlightWALReplicator creates a new WAL replicator that uses Arrow Flight
func NewFlightWALReplicator(pool ReplicatorPeerPool, mesh MeshProvider) *FlightWALReplicator {
	return &FlightWALReplicator{
		pool:    pool,
		mesh:    mesh,
		timeout: 2 * time.Second, // Fast timeout for synchronous replication
	}
}

// Replicate synchronously replicates the WAL batch to N/2 + 1 nodes (quorum)
func (r *FlightWALReplicator) Replicate(ctx context.Context, data []byte) error {
	if r.mesh == nil {
		return fmt.Errorf("mesh provider not configured")
	}

	members := r.mesh.GetMembers()
	if len(members) == 0 {
		return nil // Standalone mode, no replication needed
	}

	// Calculate quorum (N/2) excluding self, because self already writes locally
	// So we need (TotalNodes / 2 + 1) acks including self.
	// That means we need (TotalNodes / 2) successful remote acks.
	totalNodes := len(members) + 1
	requiredAcks := (totalNodes / 2) // Remote acks needed
	
	if requiredAcks == 0 {
		return nil
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(members))
	successCh := make(chan struct{}, len(members))

	for _, member := range members {
		wg.Add(1)
		go func(m mesh.Member) {
			defer wg.Done()

			reqCtx, cancel := context.WithTimeout(ctx, r.timeout)
			defer cancel()

			action := &flight.Action{
				Type: "ReplicateWAL",
				Body: data,
			}

			err := r.pool.DoAction(reqCtx, m.GRPCAddr, action)
			if err != nil {
				errCh <- fmt.Errorf("failed to replicate to %s: %w", m.GRPCAddr, err)
				return
			}

			successCh <- struct{}{}
		}(member)
	}

	// Wait for quorum
	successes := 0
	var lastErr error

	// We can use a timer or just loop until quorum or all routines finish
	for i := 0; i < len(members); i++ {
		select {
		case <-successCh:
			successes++
			if successes >= requiredAcks {
				// Quorum met! We don't need to wait for the rest to finish, 
				// but they will continue in background.
				return nil
			}
		case err := <-errCh:
			lastErr = err
		}
	}

	return fmt.Errorf("failed to reach WAL replication quorum (%d/%d): %v", successes, requiredAcks, lastErr)
}
