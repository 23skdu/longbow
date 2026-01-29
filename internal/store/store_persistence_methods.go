package store

import (
	"context"
	"fmt"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
)

// ApplyDelta applies an Arrow record batch as a delta to the specified dataset.
func (s *VectorStore) ApplyDelta(name string, rec arrow.RecordBatch, seq uint64, ts int64) error {
	if s.engine == nil {
		return fmt.Errorf("persistence not initialized")
	}

	// 1. Write to WAL
	if err := s.engine.WriteWAL(name, rec, seq, ts); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// 2. Apply to memory (Reusing re-play logic for consistency)
	return s.applyReplayBatch(name, rec, seq, ts)
}

// Snapshot triggers a snapshot of all state.
func (s *VectorStore) Snapshot(ctx context.Context) error {
	if s.engine == nil {
		return fmt.Errorf("persistence not initialized")
	}

	s.configMu.RLock()
	datasets := s.datasets.Load()
	s.configMu.RUnlock()

	if datasets == nil {
		return nil
	}

	for name, ds := range *datasets {
		ds.dataMu.RLock()
		records := make([]arrow.RecordBatch, len(ds.Records))
		copy(records, ds.Records)
		ds.dataMu.RUnlock()

		item := storage.SnapshotItem{
			Name:    name,
			Records: records,
		}

		if err := s.engine.CreateSnapshot(&item); err != nil {
			return fmt.Errorf("failed to snapshot dataset %s: %w", name, err)
		}
	}

	return nil
}

// FlushWAL flushes any pending WAL writes.
func (s *VectorStore) FlushWAL() error {
	if s.engine == nil {
		return nil
	}
	return s.engine.FlushWAL()
}

// TruncateWAL truncates the WAL up to the given sequence.
func (s *VectorStore) TruncateWAL(seq uint64) error {
	if s.engine == nil {
		return nil
	}
	return s.engine.TruncateWAL(seq)
}

// writeToWAL is an internal helper for tests that expect it.
func (s *VectorStore) writeToWAL(name string, rec arrow.RecordBatch, seq uint64, ts int64) error {
	return s.ApplyDelta(name, rec, seq, ts)
}
