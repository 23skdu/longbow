package types

import (
	"context"
	"io"

	"github.com/23skdu/longbow/internal/core"
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
	SearchVectors(ctx context.Context, q any, k int, filters []core.Filter, options any) ([]SearchResult, error)
	SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *roaring.Bitmap, options any) ([]SearchResult, error)

	// RangeSearch returns all vectors within a similarity threshold.
	// This is useful for clustering, duplicate detection, and radius-based queries.
	// Returns all vectors where distance <= threshold (or score >= minScore for similarity metrics).
	SearchVectorsInRange(ctx context.Context, q any, threshold float32, filters []core.Filter, options any) ([]SearchResult, error)
	IsSharded() bool

	// Metadata operations
	Size() int
	Len() int
	GetEntryPoint() uint32
	GetLocation(id uint32) (any, bool) // Using any to avoid cycle with store.Location? No, types can define Location alias or use any.
	GetVectorID(loc any) (uint32, bool)
	GetDimension() uint32
	SetIndexedColumns(cols []string)

	// Diagnostic/Repair
	// GetRawNeighbors returns internal neighbor IDs for diagnostics
	GetRawNeighbors(id uint32) ([]uint32, error)

	// GetNeighbors returns the k nearest neighbors for a given vector ID
	GetNeighbors(ctx context.Context, id uint32, k int) ([]SearchResult, error)
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

	// Sync/Serialization operations
	ExportState() ([]byte, error)
	ImportState(data []byte) error
	ExportGraph(w io.Writer) error
	ImportGraph(r io.Reader) error
	ExportDelta(fromVersion uint64) (*DeltaSync, error)
	ApplyDelta(delta *DeltaSync) error

	// Parallel Search
	SetParallelSearchConfig(cfg ParallelSearchConfig)
	GetParallelSearchConfig() ParallelSearchConfig

	// Maintenance
	RemapLocations(ctx context.Context, mapping map[uint32]any) error
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

// IndexDataProvider defines the interface for data access required by vector indexes
type IndexDataProvider interface {
	GetName() string
	GetRecords() []arrow.RecordBatch
	GetSchema() *arrow.Schema
	GetTombstones() map[int]*query.Bitset
	GetPQEncoder() *pq.PQEncoder
	RLockData()
	RUnlockData()
	GenerateFilterBitset(filters []core.Filter, expr FilterExpr) (*query.Bitset, error)
	ResetTombstones()
	GetIndex() any
}
