package store

import "github.com/apache/arrow-go/v18/arrow"

// VectorIndex defines the interface for vector index implementations.
// This allows for both single-threaded and sharded index implementations.

type SearchResult struct {
	ID    VectorID
	Score float32
}

type VectorIndex interface {
	// AddByLocation adds a vector from the dataset using batch and row indices.
	// Returns the assigned internal Vector ID.
	AddByLocation(batchIdx, rowIdx int) (uint32, error)

	// AddByRecord adds a vector directly from a record batch.
	// Returns the assigned internal Vector ID.
	AddByRecord(rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error)

	// SearchVectors returns the k nearest neighbors for the query vector with scores and optional filtering.
	SearchVectors(query []float32, k int, filters []Filter) []SearchResult

	// SearchVectorsWithBitmap returns k nearest neighbors filtered by a bitset.
	SearchVectorsWithBitmap(query []float32, k int, filter *Bitset) []SearchResult

	// Len returns the number of vectors in the index.
	Len() int

	// GetDimension returns the vector dimension.
	GetDimension() uint32

	// Warmup pre-loads index data into memory
	Warmup() int

	// SetIndexedColumns updates columns that should be indexed for fast equality lookups
	SetIndexedColumns(cols []string)

	// Close releases index resources.
	Close() error
}

// VectorMetric defines the distance metric type.
type VectorMetric int

const (
	MetricEuclidean VectorMetric = iota
	MetricCosine
	MetricDotProduct
)
