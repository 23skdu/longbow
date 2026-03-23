//go:build gpu && darwin && arm64

package gpu

import "fmt"

func NewIndexWithConfig(cfg GPUConfig) (Index, error) {
	fmt.Printf("Initializing Metal GPU Index (device=%d, dim=%d)...\n", cfg.DeviceID, cfg.Dimension)
	return NewMetalIndex(cfg)
}
