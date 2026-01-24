package store

// NewHNSWIndexWithMetric creates a new index with a specific distance metric (for testing)
func NewHNSWIndexWithMetric(ds *Dataset, metric DistanceMetric) *HNSWIndex {
	config := DefaultConfig()
	config.Metric = metric
	return NewHNSWIndex(ds, config)
}

// NewHNSWIndexWithCapacity creates a new index (capacity ignored as implementation is dynamic)
func NewHNSWIndexWithCapacity(ds *Dataset, capacity int) *HNSWIndex {
	return NewHNSWIndex(ds)
}
