//go:build gpu && !darwin

package gpu

import "fmt"

// NewIndex creates a new GPU-accelerated index with default configuration
func NewIndex() (Index, error) {
	return NewIndexWithConfig(GPUConfig{
		DeviceID:  0,
		Dimension: 128,
	})
}

// NewIndexWithConfig creates a GPU index with custom configuration
// This function is implemented in faiss_gpu.go for actual FAISS GPU binding
func NewIndexWithConfig(cfg GPUConfig) (Index, error) {
	fmt.Printf("Initializing FAISS GPU Index (device=%d, dim=%d)...\n", cfg.DeviceID, cfg.Dimension)
	// Call the actual FAISS GPU implementation
	return NewFaissGPUIndex(cfg)
}
