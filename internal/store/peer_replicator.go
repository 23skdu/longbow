package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ReplicatorConfig holds configuration for peer replication
type ReplicatorConfig struct {
	Timeout              time.Duration
	MaxRetries           int
	RetryBackoff         time.Duration
	AsyncReplication     bool
	AsyncBufferSize      int
	QuorumSize           int
	CircuitBreakerConfig CircuitBreakerConfig
}

// DefaultReplicatorConfig returns sensible defaults
func DefaultReplicatorConfig() ReplicatorConfig {
	return ReplicatorConfig{
		Timeout:              5 * time.Second,
		MaxRetries:           3,
		RetryBackoff:         100 * time.Millisecond,
		AsyncReplication:     false,
		AsyncBufferSize:      1000,
		QuorumSize:           1,
		CircuitBreakerConfig: DefaultCircuitBreakerConfig(),
	}
}

// ReplicatorPeerInfo holds information about a peer for replicator
type ReplicatorPeerInfo struct {
	ID       string
	Address  string
	Healthy  bool
	LastSeen time.Time
}

// PeerReplicationResult holds the result of a single peer replication attempt
type PeerReplicationResult struct {
	PeerID   string
	Success  bool
	Error    error
	Attempts int
	Duration time.Duration
}

// ReplicationTask represents an async replication task
type ReplicationTask struct {
	Dataset string
	Record  arrow.Record //nolint:staticcheck
	Ctx     context.Context
}

// ReplicatorStats holds operational statistics
type ReplicatorStats struct {
	SuccessTotal int64
	FailureTotal int64
	RetryTotal   int64
	QueuedTotal  int64
	CircuitOpens int64
}

// PeerReplicator handles replication to peer nodes
type PeerReplicator struct {
	config   ReplicatorConfig
	peers    sync.Map // map[string]*ReplicatorPeerInfo
	circuits *CircuitBreakerRegistry

	// Stats
	successTotal atomic.Int64
	failureTotal atomic.Int64
	retryTotal   atomic.Int64
	queuedTotal  atomic.Int64

	// Async replication
	taskQueue chan ReplicationTask
	stopChan  chan struct{}
	wg        sync.WaitGroup
	running   atomic.Bool
}

// NewPeerReplicator creates a new peer replicator
func NewPeerReplicator(cfg ReplicatorConfig) *PeerReplicator {
	return &PeerReplicator{
		config:    cfg,
		circuits:  NewCircuitBreakerRegistry(cfg.CircuitBreakerConfig),
		taskQueue: make(chan ReplicationTask, cfg.AsyncBufferSize),
		stopChan:  make(chan struct{}),
	}
}

// AddPeer adds a peer to the replicator
func (r *PeerReplicator) AddPeer(id, address string) error {
	info := &ReplicatorPeerInfo{
		ID:       id,
		Address:  address,
		Healthy:  true,
		LastSeen: time.Now(),
	}
	r.peers.Store(id, info)
	metrics.ReplicationPeersTotal.Inc()
	return nil
}

// RemovePeer removes a peer from the replicator
func (r *PeerReplicator) RemovePeer(id string) {
	r.peers.Delete(id)
	r.circuits.Reset(id)
	metrics.ReplicationPeersTotal.Dec()
}

// GetPeers returns all registered peers
func (r *PeerReplicator) GetPeers() []*ReplicatorPeerInfo {
	var result []*ReplicatorPeerInfo
	r.peers.Range(func(_, value any) bool {
		result = append(result, value.(*ReplicatorPeerInfo))
		return true
	})
	return result
}

// GetCircuitBreaker returns the circuit breaker for a peer
func (r *PeerReplicator) GetCircuitBreaker(peerID string) *CircuitBreaker {
	return r.circuits.GetOrCreate(peerID)
}

// ReplicateRecord replicates a record to all peers synchronously
//
//nolint:staticcheck
func (r *PeerReplicator) ReplicateRecord(ctx context.Context, dataset string, record arrow.Record) []PeerReplicationResult {
	peers := r.GetPeers()
	results := make([]PeerReplicationResult, 0, len(peers))

	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, peer := range peers {
		wg.Add(1)
		go func(p *ReplicatorPeerInfo) {
			defer wg.Done()
			result := r.replicateToPeer(ctx, p, dataset, record)
			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}(peer)
	}

	wg.Wait()
	return results
}

// replicateToPeer handles replication to a single peer with retry and circuit breaker
//
//nolint:staticcheck
func (r *PeerReplicator) replicateToPeer(ctx context.Context, peer *ReplicatorPeerInfo, dataset string, record arrow.Record) PeerReplicationResult {
	start := time.Now()
	result := PeerReplicationResult{
		PeerID: peer.ID,
	}

	cb := r.circuits.GetOrCreate(peer.ID)

	for attempt := 0; attempt <= r.config.MaxRetries; attempt++ {
		result.Attempts = attempt + 1

		// Check circuit breaker
		if !cb.Allow() {
			result.Error = ErrCircuitOpen
			result.Duration = time.Since(start)
			r.failureTotal.Add(1)
			metrics.ReplicationFailuresTotal.Inc()
			return result
		}

		// Attempt replication
		err := r.sendToPeer(ctx, peer, dataset, record)
		if err == nil {
			cb.RecordSuccess()
			result.Success = true
			result.Duration = time.Since(start)
			r.successTotal.Add(1)
			metrics.ReplicationSuccessTotal.Inc()
			return result
		}

		cb.RecordFailure()
		result.Error = err

		// Retry with backoff
		if attempt < r.config.MaxRetries {
			r.retryTotal.Add(1)
			metrics.ReplicationRetriesTotal.Inc()
			select {
			case <-ctx.Done():
				result.Duration = time.Since(start)
				r.failureTotal.Add(1)
				metrics.ReplicationFailuresTotal.Inc()
				return result
			case <-time.After(r.config.RetryBackoff * time.Duration(attempt+1)):
				// Continue to next attempt
			}
		}
	}

	result.Duration = time.Since(start)
	r.failureTotal.Add(1)
	metrics.ReplicationFailuresTotal.Inc()
	return result
}

// sendToPeer performs the actual Arrow Flight DoPut to a peer
//
//nolint:staticcheck
func (r *PeerReplicator) sendToPeer(ctx context.Context, peer *ReplicatorPeerInfo, dataset string, record arrow.Record) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, r.config.Timeout)
	defer cancel()

	// Connect to peer
	conn, err := grpc.DialContext(timeoutCtx, peer.Address, //nolint:staticcheck
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), //nolint:staticcheck
	)
	if err != nil {
		return fmt.Errorf("failed to connect to peer %s: %w", peer.ID, err)
	}
	defer conn.Close() //nolint:errcheck

	client := flight.NewFlightServiceClient(conn)

	// Create DoPut stream
	stream, err := client.DoPut(timeoutCtx)
	if err != nil {
		return fmt.Errorf("failed to create DoPut stream: %w", err)
	}

	// Create writer
	wr := flight.NewRecordWriter(stream, ipc.WithSchema(record.Schema()))
	wr.SetFlightDescriptor(&flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{dataset},
	})

	// Write record
	if err := wr.Write(record); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	if err := wr.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	return nil
}

// ReplicateWithQuorum replicates to peers and waits for quorum
//
//nolint:staticcheck
func (r *PeerReplicator) ReplicateWithQuorum(ctx context.Context, dataset string, record arrow.Record) []PeerReplicationResult {
	peers := r.GetPeers()
	results := make([]PeerReplicationResult, 0, len(peers))

	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := atomic.Int32{}

	quorumCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, peer := range peers {
		wg.Add(1)
		go func(p *ReplicatorPeerInfo) {
			defer wg.Done()
			result := r.replicateToPeer(quorumCtx, p, dataset, record)
			mu.Lock()
			results = append(results, result)
			mu.Unlock()

			if result.Success {
				if int(successCount.Add(1)) >= r.config.QuorumSize {
					cancel() // Quorum reached, cancel remaining
				}
			}
		}(peer)
	}

	wg.Wait()
	return results
}

// =============================================================================
// Async Replication
// =============================================================================

// Start starts the async replication worker
func (r *PeerReplicator) Start() error {
	if r.running.Swap(true) {
		return fmt.Errorf("replicator already running")
	}

	r.wg.Add(1)
	go r.asyncWorker()

	return nil
}

// Stop stops the async replication worker
func (r *PeerReplicator) Stop() {
	if !r.running.Swap(false) {
		return
	}

	close(r.stopChan)
	r.wg.Wait()
}

// asyncWorker processes async replication tasks
func (r *PeerReplicator) asyncWorker() {
	defer r.wg.Done()

	for {
		select {
		case <-r.stopChan:
			return
		case task := <-r.taskQueue:
			r.ReplicateRecord(task.Ctx, task.Dataset, task.Record)
		}
	}
}

// ReplicateAsync queues a record for async replication
func (r *PeerReplicator) ReplicateAsync(ctx context.Context, dataset string, record arrow.Record) bool { //nolint:staticcheck
	task := ReplicationTask{
		Dataset: dataset,
		Record:  record,
		Ctx:     ctx,
	}

	select {
	case r.taskQueue <- task:
		r.queuedTotal.Add(1)
		metrics.ReplicationQueuedTotal.Inc()
		return true
	default:
		// Queue full
		metrics.ReplicationQueueDropped.Inc()
		return false
	}
}

// Stats returns current statistics
func (r *PeerReplicator) Stats() ReplicatorStats {
	return ReplicatorStats{
		SuccessTotal: r.successTotal.Load(),
		FailureTotal: r.failureTotal.Load(),
		RetryTotal:   r.retryTotal.Load(),
		QueuedTotal:  r.queuedTotal.Load(),
	}
}
