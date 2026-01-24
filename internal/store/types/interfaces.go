package types

import (
	"context"

	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
)

// VectorIndexer defines the interface for vector indexing operations
type VectorIndexer interface {
	// Core indexing operations
	AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error)
	AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error)
	Search(ctx context.Context, query any, k int, filter any) ([]Candidate, error)
	SearchVectors(ctx context.Context, q any, k int, filters []query.Filter, options any) ([]SearchResult, error)
	SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *roaring.Bitmap, options any) ([]SearchResult, error)

	// Metadata operations
	Size() int
	Len() int
	GetEntryPoint() uint32
	GetLocation(id uint32) (any, bool) // Using any to avoid cycle with store.Location? No, types can define Location alias or use any.
	GetVectorID(loc any) (uint32, bool)
	GetDimension() uint32
	SetIndexedColumns(cols []string)

	// Diagnostic/Repair
	GetNeighbors(id uint32) ([]uint32, error)
	PreWarm(targetSize int)

	// Maintenance
	Warmup() int
	EstimateMemory() int64

	// PQ
	TrainPQ(vectors [][]float32) error
	GetPQEncoder() *pq.PQEncoder

	// Lifecycle operations
	Close() error

	// Batch operations
	AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error)
	DeleteBatch(ctx context.Context, ids []uint32) error
}

// GraphDataInterface defines the interface for graph data operations
type GraphDataInterface interface {
	// Data access operations
	GetVector(id uint32) (any, error)
	SetVector(id uint32, vec any) error
	GetNeighbors(layer int, id uint32, buffer []uint32) []uint32

	// Metadata operations
	Capacity() int
	Dims() int
	Type() VectorDataType

	// Lifecycle operations
	Close() error
}

// HNSWGraphInterface defines the interface for HNSW graph operations
type HNSWGraphInterface interface {
	// Graph construction
	Insert(id uint32, vec any, level int) error
	Delete(id uint32) error

	// Search operations
	SearchLayer(ctx context.Context, entryPoint uint32, ef, layer int, results []Candidate) ([]Candidate, error)
	SelectNeighbors(candidates []Candidate, m int) []Candidate

	// Graph maintenance
	NeedsCompaction() bool
	Compact() error

	// Metadata
	NodeCount() int
	MaxLevel() int
}

// CompactionWorkerInterface defines the interface for background compaction
type CompactionWorkerInterface interface {
	Start()
	Stop()
	IsRunning() bool
	Trigger(dataset string)
}

// StorageInterface defines the interface for persistent storage operations
type StorageInterface interface {
	// Persistence operations
	Save(data any) error
	Load() (any, error)
	Flush() error

	// Metadata
	Path() string
	Size() int64
}
