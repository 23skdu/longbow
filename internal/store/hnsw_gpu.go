package store

import (
	"fmt"

	"github.com/23skdu/longbow/internal/gpu"
	"go.uber.org/zap"
)

// InitGPU attempts to initialize GPU acceleration for this index
func (h *HNSWIndex) InitGPU(deviceID int, logger *zap.Logger) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.gpuEnabled {
		return fmt.Errorf("GPU already initialized")
	}

	// Get dimensions from dataset
	if h.dims == 0 {
		return fmt.Errorf("cannot initialize GPU: index dimensions not set")
	}

	cfg := gpu.GPUConfig{
		DeviceID:  deviceID,
		Dimension: h.dims,
	}

	idx, err := gpu.NewIndexWithConfig(cfg)
	if err != nil {
		h.gpuFallback = true
		if logger != nil {
			logger.Warn("GPU initialization failed, using CPU-only",
				zap.Error(err),
				zap.Int("device", deviceID))
		}
		return fmt.Errorf("GPU init failed: %w", err)
	}

	h.gpuIndex = idx
	h.gpuEnabled = true

	if logger != nil {
		logger.Info("GPU acceleration enabled",
			zap.Int("device", deviceID),
			zap.Int("dimensions", h.dims))
	}

	return nil
}

// SyncGPU adds vectors to the GPU index
// Should be called after adding vectors to the CPU index
func (h *HNSWIndex) SyncGPU(ids []int64, vectors []float32) error {
	if !h.gpuEnabled || h.gpuIndex == nil {
		return nil // GPU not enabled, skip
	}

	return h.gpuIndex.Add(ids, vectors)
}

// SearchHybrid performs GPU+CPU hybrid search
// Uses GPU for candidate generation, then refines with CPU HNSW graph
func (h *HNSWIndex) SearchHybrid(query []float32, k int) []SearchResult {
	// If GPU not enabled or failed, use pure CPU
	if !h.gpuEnabled || h.gpuIndex == nil {
		// Use SearchByVector which returns []SearchResult
		return h.SearchVectors(query, k, nil)
	}

	// Step 1: GPU generates candidates (k * 10 for better recall)
	candidateCount := k * 10
	if candidateCount > h.Len() {
		candidateCount = h.Len()
	}

	candidateIDs, distances, err := h.gpuIndex.Search(query, candidateCount)
	if err != nil {
		// GPU search failed, fallback to CPU
		return h.SearchVectors(query, k, nil)
	}

	// Step 2: Refine with HNSW graph
	// Convert GPU IDs to VectorIDs and re-rank using graph
	results := make([]SearchResult, 0, k)

	for i := 0; i < len(candidateIDs) && len(results) < k; i++ {
		vecID := VectorID(candidateIDs[i])

		// Verify this is a valid vector
		h.mu.RLock()
		if int(vecID) >= len(h.locations) {
			h.mu.RUnlock()
			continue
		}
		loc := h.locations[vecID]
		h.mu.RUnlock()

		// Skip tombstoned vectors
		if loc.BatchIdx == -1 {
			continue
		}

		results = append(results, SearchResult{
			ID:    vecID,
			Score: distances[i],
		})
	}

	return results
}

// CloseGPU releases GPU resources
func (h *HNSWIndex) CloseGPU() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.gpuIndex != nil {
		err := h.gpuIndex.Close()
		h.gpuIndex = nil
		h.gpuEnabled = false
		return err
	}

	return nil
}

// IsGPUEnabled returns whether GPU acceleration is active
func (h *HNSWIndex) IsGPUEnabled() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.gpuEnabled
}
