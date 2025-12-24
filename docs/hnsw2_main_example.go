// Example initialization code for cmd/longbow/main.go
// This shows how to set up the hnsw2 initialization hook

package main

import (
	"github.com/23skdu/longbow/internal/store"
	"github.com/23skdu/longbow/internal/store/hnsw2"
	"go.uber.org/zap"
)

// initializeHNSW2 is the hook function that initializes hnsw2 for datasets.
// This is called after dataset creation and can import both store and hnsw2
// packages without creating an import cycle.
func initializeHNSW2(ds *store.Dataset, logger *zap.Logger) {
	// Check if hnsw2 is enabled for this dataset
	if !ds.UseHNSW2() {
		return
	}
	
	// Create hnsw2 index with default configuration
	config := hnsw2.DefaultConfig()
	hnswIndex := hnsw2.NewArrowHNSW(ds, config)
	
	// Set the index on the dataset
	ds.SetHNSW2Index(hnswIndex)
	
	// Log initialization
	if logger != nil {
		logger.Info("hnsw2 initialized for dataset",
			zap.String("dataset", ds.Name),
			zap.Int("M", config.M),
			zap.Int("efConstruction", config.EfConstruction))
	}
}

// setupVectorStore creates and configures the VectorStore with hnsw2 support
func setupVectorStore(logger *zap.Logger) *store.VectorStore {
	// Create VectorStore (existing code)
	vs := store.NewVectorStore(
		/* mem */ nil,
		logger,
		/* maxMemory */ 10*1024*1024*1024, // 10GB
		/* maxWALSize */ 100*1024*1024,     // 100MB
		/* ttl */ 0,
	)
	
	// Register hnsw2 initialization hook
	vs.SetDatasetInitHook(func(ds *store.Dataset) {
		initializeHNSW2(ds, logger)
	})
	
	return vs
}

// Usage in main():
//
// func main() {
//     logger, _ := zap.NewProduction()
//     defer logger.Sync()
//     
//     // Create VectorStore with hnsw2 hook
//     vs := setupVectorStore(logger)
//     
//     // Now when datasets are created (via DoPut), hnsw2 will be
//     // automatically initialized if LONGBOW_USE_HNSW2=true
//     
//     // Start server...
// }
