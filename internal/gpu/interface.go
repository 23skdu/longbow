package gpu

// Index defines the interface for a GPU-accelerated vector index.
type Index interface {
	// Add adds vectors to the index.
	Add(ids []int64, vectors []float32) error

	// Search queries the index for the k-nearest neighbors.
	Search(vector []float32, k int) (ids []int64, distances []float32, err error)

	// Close releases GPU resources.
	Close() error
}
