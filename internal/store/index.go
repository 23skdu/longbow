package store

import (
	"io"

	"github.com/apache/arrow-go/v18/arrow"
)

// VectorMetric defines the distance metric used for vector search.
type VectorMetric int

const (
	MetricEuclidean VectorMetric = iota
	MetricCosine
	MetricDotProduct
)

// Index defines the interface for vector index implementations.
// It abstracts the underlying algorithm (HNSW, IVF, etc.) from the dataset.
type Index interface {
	// Add inserts a vector location into the index.
	// The vector data is retrieved from the Dataset via the callback or internal reference.
	Add(batchIdx, rowIdx int) error

	// AddByRecord adds a vector directly from a record batch.
	AddByRecord(rec arrow.RecordBatch, rowIdx, batchIdx int) error

	// Search performs k-NN search using a query vector.
	// Returns just IDs.
	Search(query []float32, k int) []VectorID

	// SearchVectors performs a k-NN search on the index.
	// filtering is applied if filters serve as a predicate.
	SearchVectors(query []float32, k int, filters []Filter) []SearchResult

	// AddByLocation adds a vector from the dataset using batch and row indices.
	// This matches the existing VectorIndex interface.
	AddByLocation(batchIdx, rowIdx int) error

	// Warmup performs cache warming for the index.
	// Returns the number of items/nodes touched.
	Warmup() int

	// Len returns the number of vectors in the index.
	Len() int

	// GetDimension returns the vector dimension.
	GetDimension() uint32

	// Close releases any resources held by the index.
	io.Closer
}
