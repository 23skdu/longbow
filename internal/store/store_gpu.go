//go:build gpu

package store

// initGPUIfEnabled attempts to initialize GPU for an HNSW index if GPU is enabled
func (vs *VectorStore) initGPUIfEnabled(idx VectorIndex) {
	if hnswIdx, ok := idx.(*ArrowHNSW); ok {
		// Check if GPU is enabled via environment or config
		// For now, we'll add a field to VectorStore
		if !vs.gpuEnabled {
			return
		}

		err := hnswIdx.InitGPU(vs.gpuDeviceID, vs.logger)
		if err != nil {
			// GPU init failed, but we continue with CPU-only
			// Error already logged in InitGPU
			return
		}

		// GPU successfully initialized
		vs.logger.Info().
			Int("device", vs.gpuDeviceID).
			Uint32("dimensions", hnswIdx.GetDimension()).
			Msg("GPU acceleration enabled for index")
	}
}
