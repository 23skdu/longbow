//go:build gpu

package store

import (
	"github.com/23skdu/longbow/internal/gpu"
)

// initGPUIfEnabled attempts to initialize GPU for an HNSW index if GPU is enabled
func (vs *VectorStore) initGPUIfEnabled(idx VectorIndex) {
	if !vs.gpuEnabled {
		return
	}

	if hnswIdx, ok := idx.(*ArrowHNSW); ok {
		cfg := gpu.GPUConfig{
			Backend:   vs.gpuBackend,
			DeviceID:  vs.gpuDeviceID,
			Dimension: hnswIdx.GetDimension(),
			Enabled:   true,
		}

		err := hnswIdx.InitGPU(vs.gpuDeviceID, vs.logger)
		if err != nil {
			vs.logger.Warn().
				Err(err).
				Int("device", vs.gpuDeviceID).
				Msg("GPU initialization failed, using CPU-only")
			return
		}

		vs.logger.Info().
			Str("backend", vs.gpuBackend.String()).
			Int("device", vs.gpuDeviceID).
			Uint32("dimensions", hnswIdx.GetDimension()).
			Msg("GPU acceleration enabled for index")
	}
}
