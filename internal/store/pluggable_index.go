package store

import (
	"fmt"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// =============================================================================
// Index Type Constants
// =============================================================================

// IndexType represents the type of vector index algorithm
type IndexType string

const (
	// IndexTypeHNSW is Hierarchical Navigable Small World graph
	IndexTypeHNSW IndexType = "hnsw"
	// IndexTypeIVFFlat is Inverted File with Flat quantization
	IndexTypeIVFFlat IndexType = "ivf_flat"
	// IndexTypeDiskANN is Microsoft DiskANN algorithm
	IndexTypeDiskANN IndexType = "diskann"
)

// =============================================================================
// Search Result
// =============================================================================

// IndexSearchResult represents a single search result from any index type
type IndexSearchResult struct {
	ID       uint64
	Distance float32
}

// =============================================================================
// Pluggable Vector Index Interface
// =============================================================================

// PluggableVectorIndex is the abstract interface for all vector index implementations
type PluggableVectorIndex interface {
	// Type returns the index type identifier
	Type() IndexType

	// Dimension returns the vector dimension
	Dimension() int

	// Size returns the number of vectors in the index
	Size() int

	// NeedsBuild returns true if index requires explicit Build() call
	NeedsBuild() bool

	// Add adds a single vector to the index
	Add(id uint64, vector []float32) error

	// AddBatch adds multiple vectors to the index
	AddBatch(ids []uint64, vectors [][]float32) error

	// Search finds k nearest neighbors for the query vector
	Search(query []float32, k int) ([]IndexSearchResult, error)

	// SearchBatch performs batch search for multiple queries
	SearchBatch(queries [][]float32, k int) ([][]IndexSearchResult, error)

	// Build builds the index (for algorithms requiring training)
	Build() error

	// Save persists the index to disk
	Save(path string) error

	// Load loads the index from disk
	Load(path string) error

	// Close releases resources
	Close() error

	// Legacy interface compatibility
	AddByLocation(batchIdx, rowIdx int) error
	SearchVectors(query []float32, k int) []SearchResult
	Len() int
}

// =============================================================================
// Index Configuration
// =============================================================================

// IndexConfig holds configuration for creating an index
type IndexConfig struct {
	Type      IndexType
	Dimension int

	// Type-specific configurations
	HNSWConfig    *HNSWIndexConfig
	IVFFlatConfig *IVFFlatConfig
	DiskANNConfig *DiskANNConfig
}

// HNSWIndexConfig holds HNSW-specific configuration
type HNSWIndexConfig struct {
	M              int
	EfConstruction int
	EfSearch       int
}

// IVFFlatConfig holds IVF-Flat-specific configuration
type IVFFlatConfig struct {
	NClusters int
	NProbe    int
}

// DiskANNConfig holds DiskANN-specific configuration
type DiskANNConfig struct {
	MaxDegree    int
	BeamWidth    int
	BuildThreads int
}

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
	return &HNSWPluggableAdapter{
		dimension: cfg.Dimension,
		vectors:   make(map[uint64][]float32),
		config:    cfg.HNSWConfig,
	}, nil
}

// createIVFFlatIndex creates an IVF-Flat index (stub implementation)
func createIVFFlatIndex(cfg IndexConfig) (PluggableVectorIndex, error) {
	return &IVFFlatIndex{
		dimension: cfg.Dimension,
		vectors:   make(map[uint64][]float32),
		config:    cfg.IVFFlatConfig,
	}, nil
}

// createDiskANNIndex creates a DiskANN index (stub implementation)
func createDiskANNIndex(cfg IndexConfig) (PluggableVectorIndex, error) {
	return &DiskANNIndex{
		dimension: cfg.Dimension,
		vectors:   make(map[uint64][]float32),
		config:    cfg.DiskANNConfig,
	}, nil
}
