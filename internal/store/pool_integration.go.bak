package store

import (
	"context"
	"math"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.uber.org/zap"
)

// =============================================================================
// VectorStore FlightClientPool Integration
// =============================================================================

// NewVectorStoreWithReplication creates a VectorStore with replication enabled.
func NewVectorStoreWithReplication(
	mem memory.Allocator,
	logger *zap.Logger,
	maxMemory, maxWALSize int64,
	ttl time.Duration,
	repCfg ReplicationConfig,
) *VectorStore {
	// Create base store
	s := NewVectorStore(mem, logger, maxMemory, maxWALSize, ttl)

	// Set replication config
	s.replicationConfig = repCfg

	if repCfg.Enabled {
		// Create flight client pool
		poolCfg := DefaultFlightClientPoolConfig()
		s.flightClientPool = NewFlightClientPool(poolCfg)

		// Pre-register peers
		for _, peer := range repCfg.Peers {
			_ = s.flightClientPool.AddHost(peer)
		}
	}

	return s
}

// GetFlightClientPool returns the flight client pool for peer replication.
func (s *VectorStore) GetFlightClientPool() *FlightClientPool {
	return s.flightClientPool
}

// IsReplicationEnabled returns whether replication is enabled.
func (s *VectorStore) IsReplicationEnabled() bool {
	return s.replicationConfig.Enabled
}

// GetReplicationPeers returns the configured replication peers.
func (s *VectorStore) GetReplicationPeers() []string {
	return s.replicationConfig.Peers
}

// SetReplicationHook sets a hook function for testing replication.
func (s *VectorStore) SetReplicationHook(hook func(ctx context.Context, dataset string, records []arrow.RecordBatch)) {
	s.replicationHook = hook
}

// StoreRecordBatch stores a record batch and triggers replication if enabled.
// This is a convenience method for programmatic use (non-gRPC).
func (s *VectorStore) StoreRecordBatch(ctx context.Context, dataset string, rec arrow.RecordBatch) error {
	size := CachedRecordSize(rec)

	// Memory limit check
	maxMem := s.maxMemory.Load()
	if maxMem > 0 {
		for {
			current := s.currentMemory.Load()
			if current+size > maxMem {
				return NewResourceExhaustedError("memory", "limit exceeded")
			}
			if s.currentMemory.CompareAndSwap(current, current+size) {
				break
			}
		}
	} else {
		s.currentMemory.Add(size)
	}

	// Store the record
	rec.Retain()
	ds := s.vectors.GetOrCreate(dataset, func() *Dataset {
		newDs := &Dataset{Records: []arrow.RecordBatch{}, lastAccess: time.Now().UnixNano()}
		newDs.Index = NewHNSWIndex(newDs)
		return newDs
	})

	ds.mu.Lock()
	batchIdx := len(ds.Records)

	// Calculate baseRowID for BM25 indexing based on existing rows
	var totalRows int64
	for _, existingRec := range ds.Records {
		totalRows += existingRec.NumRows()
	}
	var baseRowID uint32
	if totalRows > math.MaxUint32 {
		s.logger.Warn("Dataset row count exceeds MaxUint32, indexing may be incorrect", zap.String("dataset", dataset))
		baseRowID = math.MaxUint32
	} else {
		baseRowID = uint32(totalRows) //nolint:gosec // G115 - checked above
	}

	ds.Records = append(ds.Records, rec)
	ds.mu.Unlock()

	// Auto-index text columns for BM25 hybrid search
	s.indexTextColumnsForHybridSearch(rec, baseRowID)

	// Auto-trigger compaction if batch count exceeds threshold
	if s.compactionWorker != nil && batchIdx+1 > s.compactionConfig.MinBatchesToCompact {
		_ = s.compactionWorker.TriggerCompaction(dataset)
	}

	// Update metadata
	if _, exists := s.metadata.Get(dataset); !exists {
		s.metadata.Set(dataset, DatasetMetadata{
			Name:       dataset,
			Schema:     rec.Schema(),
			TotalRows:  rec.NumRows(),
			BatchCount: 1,
		})
	} else {
		s.metadata.IncrementStats(dataset, rec.NumRows(), 1)
	}

	// Trigger replication
	s.triggerReplication(ctx, dataset, []arrow.RecordBatch{rec})

	return nil
}

// triggerReplication sends data to peer nodes if replication is enabled.
func (s *VectorStore) triggerReplication(ctx context.Context, dataset string, records []arrow.RecordBatch) {
	if !s.replicationConfig.Enabled {
		return
	}

	// If hook is set (for testing), call it instead of real replication
	if s.replicationHook != nil {
		s.replicationHook(ctx, dataset, records)
		return
	}

	// Real replication via pool
	if s.flightClientPool == nil || len(s.replicationConfig.Peers) == 0 {
		return
	}

	// Async replication
	if s.replicationConfig.Mode == ReplicationModeAsync {
		go func() {
			timeout := s.replicationConfig.ConnectTimeout
			if timeout == 0 {
				timeout = 10 * time.Second
			}
			replicationCtx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			errors := s.flightClientPool.ReplicateToPeers(replicationCtx, s.replicationConfig.Peers, dataset, records)
			if len(errors) > 0 {
				s.logger.Warn("Async replication errors", zap.Int("error_count", len(errors)))
			}
		}()
	} else {
		// Sync replication
		timeout := s.replicationConfig.ConnectTimeout
		if timeout == 0 {
			timeout = 10 * time.Second
		}
		replicationCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		errors := s.flightClientPool.ReplicateToPeers(replicationCtx, s.replicationConfig.Peers, dataset, records)
		if len(errors) > 0 {
			s.logger.Warn("Sync replication errors", zap.Int("error_count", len(errors)))
		}
	}
}
