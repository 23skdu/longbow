//go:build gpu && darwin && arm64

package gpu

import "fmt"

// NewIndex creates a new GPU-accelerated index with default configuration
func NewIndex() (Index, error) {
	return NewIndexWithConfig(GPUConfig{
		DeviceID:  0,
		Dimension: 128,
	})
}

// NewIndexWithConfig creates a GPU index with custom configuration for Metal
func NewIndexWithConfig(cfg GPUConfig) (Index, error) {
	fmt.Printf("Initializing Metal GPU Index (device=%d, dim=%d)...\n", cfg.DeviceID, cfg.Dimension)
	return NewMetalIndex(cfg)
}
