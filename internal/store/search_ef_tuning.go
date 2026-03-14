package store

import (
	"runtime"

	"github.com/23skdu/longbow/internal/gpu"
)

type SearchEfConfig struct {
	BaseEf int

	DimensionEf128  int
	DimensionEf384  int
	DimensionEf768  int
	DimensionEf1536 int

	GPUefMultiplier float32

	detectedGPU   bool
	detectedCores int
}

func NewSearchEfConfig() SearchEfConfig {
	gpus := gpu.DetectAvailableGPUs()
	hasGPU := len(gpus) > 0
	numCPU := runtime.NumCPU()

	return SearchEfConfig{
		BaseEf:          50,
		DimensionEf128:  64,
		DimensionEf384:  128,
		DimensionEf768:  256,
		DimensionEf1536: 512,
		GPUefMultiplier: 2.0,
		detectedGPU:     hasGPU,
		detectedCores:   numCPU,
	}
}

func (cfg *SearchEfConfig) GetEfForDimension(dim int) int {
	switch {
	case dim <= 128:
		return cfg.DimensionEf128
	case dim <= 384:
		return cfg.DimensionEf384
	case dim <= 768:
		return cfg.DimensionEf768
	case dim <= 1536:
		return cfg.DimensionEf1536
	default:
		return cfg.BaseEf * 4
	}
}

func (cfg *SearchEfConfig) GetEf(isGPU bool, dimension int) int {
	ef := cfg.GetEfForDimension(dimension)

	if isGPU || cfg.detectedGPU {
		ef = int(float32(ef) * cfg.GPUefMultiplier)
	}

	return ef
}
