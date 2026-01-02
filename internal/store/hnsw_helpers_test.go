package store

// NewHNSWIndexWithMetric creates a new index with a specific distance metric (for testing)
func NewHNSWIndexWithMetric(ds *Dataset, metric VectorMetric) *HNSWIndex {
	idx := NewHNSWIndex(ds)
	idx.Metric = metric
	idx.Graph.Distance = idx.GetDistanceFunc()
	return idx
}

// NewHNSWIndexWithCapacity creates a new index (capacity ignored as implementation is dynamic)
func NewHNSWIndexWithCapacity(ds *Dataset, cap int) *HNSWIndex {
	return NewHNSWIndex(ds)
}
