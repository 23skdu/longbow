package types

import (
	"context"

	gputypes "github.com/23skdu/longbow/internal/gpu/types"
)

// IndexType represents the type of vector index algorithm.
type IndexType string

const (
	// IndexTypeHNSW is Hierarchical Navigable Small World graph.
	IndexTypeHNSW IndexType = "hnsw"
	// IndexTypeIVFFlat is Inverted File with Flat quantization.
	IndexTypeIVFFlat IndexType = "ivf_flat"
	// IndexTypeDiskANN is Microsoft DiskANN algorithm.
	IndexTypeDiskANN IndexType = "diskann"
	// IndexTypeIVFHNSW is Billion-scale IVF-HNSW composite index.
	IndexTypeIVFHNSW IndexType = "ivf_hnsw"
)

// IndexSearchResult represents a single search result from any index type.
type IndexSearchResult struct {
	ID       uint64
	Distance float32
}

// PluggableVectorIndex is the abstract interface for all vector index implementations.
type PluggableVectorIndex interface {
	Type() IndexType
	Dimension() int
	Size() int
	NeedsBuild() bool
	Add(id uint64, vector []float32) error
	AddBatchRaw(ids []uint64, vectors [][]float32) error
	Search(query []float32, k int) ([]IndexSearchResult, error)
	SearchBatch(queries [][]float32, k int) ([][]IndexSearchResult, error)
	GetNeighbors(ctx context.Context, id VectorID, k int) ([]SearchResult, error)
	Build() error
	Save(path string) error
	Load(path string) error
	ExportState() ([]byte, error)
	ImportState(data []byte) error
	Close() error
	AddByLocation(batchIdx, rowIdx int) error
	GetVectorID(loc Location) (uint64, bool)
	SearchVectors(query []float32, k int, options SearchOptions) []SearchResult
	Len() int
}

// IndexConfig holds configuration for creating an index.
type IndexConfig struct {
	Type      IndexType
	Dimension int

	HNSWConfig    *ArrowHNSWConfig
	IVFFlatConfig *IVFFlatConfig
	DiskANNConfig *DiskANNConfig
	IVFHNSWConfig *IVFHNSWConfig
}

// IVFFlatConfig holds IVF-Flat-specific configuration.
type IVFFlatConfig struct {
	NClusters int
	NProbe    int
}

// DiskANNConfig holds DiskANN-specific configuration.
type DiskANNConfig struct {
	MaxDegree    int
	BeamWidth    int
	BuildThreads int
}

// IVFHNSWConfig holds configuration for the IVF-HNSW composite index.
type IVFHNSWConfig struct {
	Nlist              int
	M                  int
	K                  int
	Nprobe             int
	HNSWM              int
	HNSWEfConstruction int
	HNSWEfSearch       int
	GPUEnabled         bool
	GPUConfig          *gputypes.GPUConfig
}
