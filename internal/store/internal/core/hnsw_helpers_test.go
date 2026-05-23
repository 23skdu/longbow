package core

import (
	basecore "github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/store/types"
)

// NewHNSWIndexWithMetric creates a new index with a specific distance metric (for testing)
func NewHNSWIndexWithMetric(ds types.IndexDataProvider, metric basecore.DistanceMetric) *ArrowHNSW {
	config := types.DefaultArrowHNSWConfig()
	config.Metric = metric
	return NewArrowHNSW(ds, &config, nil)
}

// NewHNSWIndexWithCapacity creates a new index (capacity ignored as implementation is dynamic)
func NewHNSWIndexWithCapacity(ds types.IndexDataProvider, capacity int) *ArrowHNSW {
	config := types.DefaultArrowHNSWConfig()
	return NewArrowHNSW(ds, &config, nil)
}
