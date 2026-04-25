package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
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
	// IndexTypeIVFHNSW is Billion-scale IVF-HNSW composite index
	IndexTypeIVFHNSW IndexType = "ivf_hnsw"
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

	// GetNeighbors returns the k nearest neighbors for a given vector ID
	GetNeighbors(ctx context.Context, id lbtypes.VectorID, k int) ([]lbtypes.SearchResult, error)

	// Build builds the index (for algorithms requiring training)
	Build() error

	// Save persists the index to disk
	Save(path string) error

	// Load loads the index from disk
	Load(path string) error

	// ExportState returns the index state as a byte slice
	ExportState() ([]byte, error)

	// ImportState restores the index state from a byte slice
	ImportState(data []byte) error

	// Close releases resources
	Close() error

	// Legacy interface compatibility
	AddByLocation(batchIdx, rowIdx int) error
	GetVectorID(loc Location) (uint64, bool)
	SearchVectors(query []float32, k int, options SearchOptions) []lbtypes.SearchResult
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
	HNSWConfig    *ArrowHNSWConfig
	IVFFlatConfig *IVFFlatConfig
	DiskANNConfig *DiskANNConfig
	IVFHNSWConfig *IVFHNSWConfig
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
	return &HNSWPluggableAdapter{
		dimension: cfg.Dimension,
		vectors:   make(map[uint64][]float32),
		config:    cfg.HNSWConfig,
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
