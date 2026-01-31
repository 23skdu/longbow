package store

// NewHNSWIndexWithMetric creates a new index with a specific distance metric (for testing)
func NewHNSWIndexWithMetric(ds *Dataset, metric DistanceMetric) *ArrowHNSW {
	config := DefaultArrowHNSWConfig()
	config.Metric = metric
	return NewArrowHNSW(ds, &config)
}

// NewHNSWIndexWithCapacity creates a new index (capacity ignored as implementation is dynamic)
func NewHNSWIndexWithCapacity(ds *Dataset, capacity int) *ArrowHNSW {
	return NewTestHNSWIndex(ds)
}
