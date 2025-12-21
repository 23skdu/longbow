package store

import "github.com/apache/arrow-go/v18/arrow"

// VectorIndex defines the interface for vector index implementations.
// This allows for both single-threaded and sharded index implementations.
type VectorIndex interface {
	// AddByLocation adds a vector from the dataset using batch and row indices.
	AddByLocation(batchIdx, rowIdx int) error

	// AddByRecord adds a vector directly from a record batch.
	AddByRecord(rec arrow.RecordBatch, rowIdx, batchIdx int) error

	// SearchVectors returns the k nearest neighbors for the query vector with scores and optional filtering.
	SearchVectors(query []float32, k int, filters []Filter) []SearchResult

	// Len returns the number of vectors in the index.
	Len() int

	// GetDimension returns the vector dimension.
	GetDimension() uint32

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
