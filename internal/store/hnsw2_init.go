package store

import (
	"github.com/23skdu/longbow/internal/store/hnsw2"
	"go.uber.org/zap"
)

// initHNSW2IfEnabled initializes hnsw2 for a dataset if the feature flag is enabled.
// This function is in a separate file to avoid import cycles in store.go.
func initHNSW2IfEnabled(ds *Dataset, logger *zap.Logger) {
	if !ds.useHNSW2 {
		return
	}
	
	config := hnsw2.DefaultConfig()
	ds.hnsw2Index = hnsw2.NewArrowHNSW(ds, config)
	
	if logger != nil {
		logger.Info("hnsw2 enabled for dataset", zap.String("dataset", ds.Name))
	}
}
