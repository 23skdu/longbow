//go:build gpu

package store

import (
	"github.com/rs/zerolog"
)

// initGPUIfEnabled attempts to initialize GPU for an HNSW index if GPU is enabled
func (vs *VectorStore) initGPUIfEnabled(idx *HNSWIndex) {
	// Check if GPU is enabled via environment or config
	// For now, we'll add a field to VectorStore
	if !vs.gpuEnabled {
		return
	}

	err := idx.InitGPU(vs.gpuDeviceID, vs.logger)
	if err != nil {
		// GPU init failed, but we continue with CPU-only
		// Error already logged in InitGPU
		return
	}

	// GPU successfully initialized
	vs.logger.Info().
		Int("device", vs.gpuDeviceID).
		Int("dimensions", idx.dims).
		Msg("GPU acceleration enabled for index")
}
