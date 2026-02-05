//go:build gpu

package store

import (
	"time"

	"github.com/23skdu/longbow/internal/gpu"
	"github.com/23skdu/longbow/internal/metrics"
)

// InitGPUBackend initializes GPU acceleration for the vector store
func (s *VectorStore) InitGPUBackend(backend gpu.GPUBackend, deviceID int) error {
	s.configMu.Lock()
	defer s.configMu.Unlock()

	if s.gpuEnabled {
		return nil // Already initialized
	}

	s.gpuBackend = backend
	s.gpuDeviceID = deviceID

	// Validate backend availability
	available, reason, err := gpu.GetGPURequirements(backend)
	if err != nil {
		metrics.GPUFallbackTotal.WithLabelValues("initialization_error").Inc()
		return err
	}
	if !available {
		metrics.GPUFallbackTotal.WithLabelValues(reason).Inc()
		s.logger.Warn().
			Str("backend", backend.String()).
			Str("reason", reason).
			Msg("GPU backend not available, falling back to CPU")
		s.gpuBackend = gpu.BackendCPU
		s.gpuEnabled = true
		return nil
	}

	// Initialize GPU memory pool
	if backend != gpu.BackendCPU {
		pool, err := gpu.NewGPUMemPool(backend, deviceID)
		if err != nil {
			metrics.GPUFallbackTotal.WithLabelValues("memory_pool_error").Inc()
			s.logger.Error().
				Err(err).
				Str("backend", backend.String()).
				Int("device", deviceID).
				Msg("Failed to create GPU memory pool, falling back to CPU")
			s.gpuBackend = gpu.BackendCPU
			s.gpuEnabled = true
			return nil
		}
		s.gpuMemPool = pool

		// Initialize memory metrics
		total := pool.GetTotalMemory()
		metrics.GPUMemoryBytes.WithLabelValues(string(rune(deviceID)), "total").Set(float64(total))

		// Update free memory
		if s.gpuMemPool != nil {
			availableMem := s.gpuMemPool.GetAvailableMemory()
			metrics.GPUMemoryBytes.WithLabelValues(string(rune(deviceID)), "free").Set(float64(availableMem))
			used := total - availableMem
			metrics.GPUMemoryBytes.WithLabelValues(string(rune(deviceID)), "used").Set(float64(used))
		}
	}

	s.gpuEnabled = true
	s.logger.Info().
		Str("backend", backend.String()).
		Int("device", deviceID).
		Msg("GPU backend initialized successfully")

	return nil
}

// initGPUIfEnabled attempts to initialize GPU for an HNSW index if GPU is enabled
func (s *VectorStore) initGPUIfEnabled(idx VectorIndex) {
	if !s.gpuEnabled {
		return
	}

	if hnswIdx, ok := idx.(*ArrowHNSW); ok {
		start := time.Now()

		err := hnswIdx.InitGPU(s.gpuDeviceID, s.logger)
		if err != nil {
			metrics.GPUFallbackTotal.WithLabelValues("hnsw_init_error").Inc()
			s.logger.Warn().
				Err(err).
				Int("device", s.gpuDeviceID).
				Msg("GPU initialization failed for HNSW index, using CPU-only")
			return
		}

		duration := time.Since(start).Seconds()
		metrics.GPUSearchDurationSeconds.WithLabelValues(s.gpuBackend.String()).Observe(duration)

		s.logger.Info().
			Str("backend", s.gpuBackend.String()).
			Int("device", s.gpuDeviceID).
			Uint32("dimensions", hnswIdx.GetDimension()).
			Dur("duration", time.Since(start)).
			Msg("GPU acceleration enabled for index")
	}
}

// GetGPUMemoryStats returns current GPU memory statistics
func (s *VectorStore) GetGPUMemoryStats() (total, used, free int64) {
	if !s.gpuEnabled || s.gpuMemPool == nil {
		return 0, 0, 0
	}

	total = s.gpuMemPool.GetTotalMemory()
	used = s.gpuMemPool.GetUsedMemory()
	free = total - used
	return
}

// UpdateGPUMemoryMetrics updates GPU memory metrics for monitoring
func (s *VectorStore) UpdateGPUMemoryMetrics() {
	if !s.gpuEnabled || s.gpuMemPool == nil {
		return
	}

	deviceID := string(rune(s.gpuDeviceID))
	total := s.gpuMemPool.GetTotalMemory()
	used := s.gpuMemPool.GetUsedMemory()
	free := total - used

	metrics.GPUMemoryBytes.WithLabelValues(deviceID, "total").Set(float64(total))
	metrics.GPUMemoryBytes.WithLabelValues(deviceID, "used").Set(float64(used))
	metrics.GPUMemoryBytes.WithLabelValues(deviceID, "free").Set(float64(free))
}

// IsGPUEnabled returns whether GPU acceleration is enabled
func (s *VectorStore) IsGPUEnabled() bool {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return s.gpuEnabled
}

// GetGPUBackend returns the current GPU backend
func (s *VectorStore) GetGPUBackend() gpu.GPUBackend {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return s.gpuBackend
}
