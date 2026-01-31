package store

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
)

// HNSWGraphSync manages HNSW index synchronization between peers.
type HNSWGraphSync struct {
	index       VectorIndex
	mu          sync.RWMutex
	version     atomic.Uint64
	exportCount atomic.Uint64
	importCount atomic.Uint64
}

// NewHNSWGraphSync creates a new sync manager for the given index.
func NewHNSWGraphSync(index VectorIndex) *HNSWGraphSync {
	return &HNSWGraphSync{
		index: index,
	}
}

// ExportState serializes the complete HNSW index state.
func (s *HNSWGraphSync) ExportState() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, err := s.index.ExportState()
	if err != nil {
		return nil, err
	}

	s.exportCount.Add(1)
	metrics.HNSWGraphSyncExportsTotal.Inc()
	return data, nil
}

// ImportState deserializes and applies HNSW index state.
func (s *HNSWGraphSync) ImportState(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.index.ImportState(data); err != nil {
		return err
	}

	// We might need to recover the version from the imported state if possible,
	// but currently SyncState.Version is not used by HNSWIndex.ImportState.
	// For now, we assume the caller handles versioning outside if needed,
	// or we just trust the index state.
	// Legacy behavior: s.version.Store(state.Version)

	s.importCount.Add(1)
	metrics.HNSWGraphSyncImportsTotal.Inc()
	return nil
}

// ExportGraph exports just the HNSW graph structure.
func (s *HNSWGraphSync) ExportGraph(w io.Writer) error {
	return s.index.ExportGraph(w)
}

// ImportGraph imports just the HNSW graph structure.
func (s *HNSWGraphSync) ImportGraph(r io.Reader) error {
	return s.index.ImportGraph(r)
}

// GetVersion returns the current sync version.
func (s *HNSWGraphSync) GetVersion() uint64 {
	return s.version.Load()
}

// IncrementVersion increments the sync version.
func (s *HNSWGraphSync) IncrementVersion() {
	s.version.Add(1)
}

// ExportDelta exports changes since the given version.
func (s *HNSWGraphSync) ExportDelta(fromVersion uint64) (*types.DeltaSync, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	delta, err := s.index.ExportDelta(fromVersion)
	if err != nil {
		return nil, err
	}

	metrics.HNSWGraphSyncDeltasTotal.Inc()
	return delta, nil
}

// ApplyDelta applies incremental changes from a delta.
func (s *HNSWGraphSync) ApplyDelta(delta *types.DeltaSync) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.index.ApplyDelta(delta); err != nil {
		return err
	}

	s.version.Store(delta.ToVersion)
	metrics.HNSWGraphSyncDeltaAppliesTotal.Inc()
	return nil
}

// GetExportCount returns the number of exports performed.
func (s *HNSWGraphSync) GetExportCount() uint64 {
	return s.exportCount.Load()
}

// GetImportCount returns the number of imports performed.
func (s *HNSWGraphSync) GetImportCount() uint64 {
	return s.importCount.Load()
}
