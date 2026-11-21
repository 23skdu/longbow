package index

import (
	"fmt"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
)

// =============================================================================
// Index Type Constants
// =============================================================================

// IndexType represents the type of vector index algorithm
type IndexType = types.IndexType

const (
	// IndexTypeHNSW is Hierarchical Navigable Small World graph
	IndexTypeHNSW = types.IndexTypeHNSW
	// IndexTypeIVFFlat is Inverted File with Flat quantization
	IndexTypeIVFFlat = types.IndexTypeIVFFlat
	// IndexTypeDiskANN is Microsoft DiskANN algorithm
	IndexTypeDiskANN = types.IndexTypeDiskANN
	// IndexTypeIVFHNSW is Billion-scale IVF-HNSW composite index
	IndexTypeIVFHNSW = types.IndexTypeIVFHNSW
)

// =============================================================================
// Search Result
// =============================================================================

// IndexSearchResult represents a single search result from any index type
type IndexSearchResult = types.IndexSearchResult

// =============================================================================
// Pluggable Vector Index Interface
// =============================================================================

// PluggableVectorIndex is the abstract interface for all vector index implementations
type PluggableVectorIndex = types.PluggableVectorIndex

// =============================================================================
// Index Configuration
// =============================================================================

// IndexConfig holds configuration for creating an index
type IndexConfig = types.IndexConfig

// IVFFlatConfig holds IVF-Flat-specific configuration
type IVFFlatConfig = types.IVFFlatConfig

// DiskANNConfig holds DiskANN-specific configuration
type DiskANNConfig = types.DiskANNConfig

// =============================================================================
// Index Factory
// =============================================================================

// IndexConstructor is a function that creates a PluggableVectorIndex
type IndexConstructor func(cfg IndexConfig) (PluggableVectorIndex, error)

// IndexFactory creates indexes by type using a registry pattern
type IndexFactory struct {
	mu       sync.RWMutex
	registry map[IndexType]IndexConstructor
}

// NewIndexFactory creates a new factory with default index types registered
func NewIndexFactory() *IndexFactory {
	f := &IndexFactory{
		registry: make(map[IndexType]IndexConstructor),
	}

	// Register built-in index types
	f.Register(IndexTypeHNSW, createHNSWIndex)
	f.Register(IndexTypeIVFFlat, createIVFFlatIndex)
	f.Register(IndexTypeDiskANN, createDiskANNIndex)
	f.Register(IndexTypeIVFHNSW, createIVFHNSWIndex)

	return f
}

// Register adds a new index type to the factory
func (f *IndexFactory) Register(t IndexType, ctor IndexConstructor) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registry[t] = ctor
	metrics.IndexTypesRegistered.Inc()
}

// Create creates an index of the specified type
func (f *IndexFactory) Create(cfg IndexConfig) (PluggableVectorIndex, error) {
	start := time.Now()
	defer func() {
		metrics.IndexCreationDuration.WithLabelValues(string(cfg.Type)).Observe(time.Since(start).Seconds())
	}()

	f.mu.RLock()
	ctor, ok := f.registry[cfg.Type]
	f.mu.RUnlock()

	if !ok {
		metrics.IndexCreationsTotal.WithLabelValues(string(cfg.Type), "error").Inc()
		return nil, fmt.Errorf("unknown index type: %s", cfg.Type)
	}

	idx, err := ctor(cfg)
	if err != nil {
		metrics.IndexCreationsTotal.WithLabelValues(string(cfg.Type), "error").Inc()
		return nil, err
	}

	metrics.IndexCreationsTotal.WithLabelValues(string(cfg.Type), "success").Inc()
	return idx, nil
}

// ListTypes returns all registered index types
func (f *IndexFactory) ListTypes() []IndexType {
	f.mu.RLock()
	defer f.mu.RUnlock()

	types := make([]IndexType, 0, len(f.registry))
	for t := range f.registry {
		types = append(types, t)
	}
	return types
}

// =============================================================================
// Default Index Constructors
// =============================================================================

// createHNSWIndex creates an HNSW index adapter
func createHNSWIndex(cfg IndexConfig) (PluggableVectorIndex, error) {
	config := types.ArrowHNSWConfig{}
	if cfg.HNSWConfig != nil {
		config = *cfg.HNSWConfig
	}
	return &HNSWPluggableAdapter{
		dimension:  cfg.Dimension,
		vectors:    make(map[uint64][]float32),
		hnswConfig: config,
	}, nil
}

// createIVFFlatIndex creates an IVF-Flat index
func createIVFFlatIndex(cfg IndexConfig) (PluggableVectorIndex, error) {
	return NewIVFFlatIndex(cfg)
}

// createDiskANNIndex creates a DiskANN index
func createDiskANNIndex(cfg IndexConfig) (PluggableVectorIndex, error) {
	return NewDiskANNIndex(cfg)
}

// createIVFHNSWIndex creates an IVF-HNSW composite index
func createIVFHNSWIndex(cfg IndexConfig) (PluggableVectorIndex, error) {
	var c IVFHNSWConfig
	if cfg.IVFHNSWConfig != nil {
		c = *cfg.IVFHNSWConfig
	}
	return NewIVFHNSWCompositeIndex(cfg.Dimension, c)
}
