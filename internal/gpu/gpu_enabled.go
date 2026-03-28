//go:build gpu && !darwin

package gpu

import "fmt"


// NewIndexWithConfig creates a GPU index with custom configuration
// This function is implemented in faiss_gpu.go for actual FAISS GPU binding
func NewIndexWithConfig(cfg GPUConfig) (Index, error) {
	fmt.Printf("Initializing FAISS GPU Index (device=%d, dim=%d)...\n", cfg.DeviceID, cfg.Dimension)
	// Call the actual FAISS GPU implementation
	return NewFaissGPUIndex(cfg)
}
