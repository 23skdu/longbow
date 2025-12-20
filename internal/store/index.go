package store

import (
	"io"
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

	// Search performs k-NN search using a query vector.
	// Returns just IDs.
	Search(query []float32, k int) []VectorID

	// SearchVectors performs k-NN search returning full results with scores.
	// This matches the existing VectorIndex interface.
	SearchVectors(query []float32, k int) []SearchResult

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
