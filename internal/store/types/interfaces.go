package types

import "context"

// VectorIndexer defines the interface for vector indexing operations
type VectorIndexer interface {
	// Core indexing operations
	AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error)
	Search(ctx context.Context, query any, k int, filter any) ([]Candidate, error)

	// Metadata operations
	Size() int
	GetEntryPoint() uint32

	// Lifecycle operations
	Close() error
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
