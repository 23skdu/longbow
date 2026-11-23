package hnsw

import "github.com/23skdu/longbow/internal/store/types"

// CreateHNSWIndex is the exported construction entry point used by the root
// package's index factory and by the IVF factory wiring.
func CreateHNSWIndex(cfg types.IndexConfig) (types.PluggableVectorIndex, error) {
	var config types.ArrowHNSWConfig
	if cfg.HNSWConfig != nil {
		config = *cfg.HNSWConfig
	}
	return NewHNSWPluggableAdapter(cfg.Dimension, config), nil
}

// NewHNSWPluggableAdapter is the factory constructor used by the root index
// package and by IVF quantizer wiring. It keeps the adapter's internal state
// private while exposing the construction seam required by the modularized
// package layout.
func NewHNSWPluggableAdapter(dimension int, config types.ArrowHNSWConfig) *HNSWPluggableAdapter {
	return &HNSWPluggableAdapter{
		dimension:  dimension,
		vectors:    make(map[uint64][]float32),
		hnswConfig: config,
	}
}
