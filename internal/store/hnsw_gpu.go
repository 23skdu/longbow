package store

import (
	"context"
	"fmt"

	"github.com/23skdu/longbow/internal/gpu"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/rs/zerolog"
)

// InitGPU attempts to initialize GPU acceleration for this index
//
// InitGPU attempts to initialize GPU acceleration for this index
//
//nolint:gocritic // Logger passed by value for simplicity
func (h *ArrowHNSW) InitGPU(deviceID int, logger zerolog.Logger) error {
	h.gpuMu.Lock()
	defer h.gpuMu.Unlock()

	if h.gpuEnabled {
		return fmt.Errorf("GPU already initialized")
	}

	// Get dimensions from ArrowHNSW
	dims := int(h.GetDimension())
	if dims == 0 {
		return fmt.Errorf("cannot initialize GPU: index dimensions not set")
	}

	cfg := gpu.GPUConfig{
		DeviceID:  deviceID,
		Dimension: dims,
	}

	idx, err := gpu.NewIndexWithConfig(cfg)
	if err != nil {
		h.gpuFallback = true
		if logger.GetLevel() != zerolog.Disabled {
			logger.Warn().
				Err(err).
				Int("device", deviceID).
				Msg("GPU initialization failed, using CPU-only")
		}
		return fmt.Errorf("GPU init failed: %w", err)
	}

	h.gpuIndex = idx
	h.gpuEnabled = true

	if logger.GetLevel() != zerolog.Disabled {
		logger.Info().
			Int("device", deviceID).
			Int("dimensions", dims).
			Msg("GPU acceleration enabled")
	}

	return nil
}

// SyncGPU adds vectors to the GPU index
// Should be called after adding vectors to the CPU index
func (h *ArrowHNSW) SyncGPU(ids []int64, vectors []float32) error {
	if !h.gpuEnabled || h.gpuIndex == nil {
		return nil // GPU not enabled, skip
	}

	return h.gpuIndex.Add(ids, vectors)
}

// SearchHybrid performs GPU+CPU hybrid search
// Uses GPU for candidate generation, then refines with CPU HNSW graph
func (h *ArrowHNSW) SearchHybrid(ctx context.Context, query []float32, k int) ([]SearchResult, error) {
	// If GPU not enabled or failed, use pure CPU
	if !h.gpuEnabled || h.gpuIndex == nil {
		// Use SearchVectors which returns []SearchResult
		res, err := h.SearchVectors(ctx, query, k, nil, any(nil))
		if err != nil {
			return nil, err
		}
		return res, nil
	}

	// Step 1: GPU generates candidates (k * 10 for better recall)
	candidateCount := k * 10
	if candidateCount > h.Len() {
		candidateCount = h.Len()
	}

	candidateIDs, distances, err := h.gpuIndex.Search(query, candidateCount)
	if err != nil {
		// GPU search failed, fallback to CPU
		res, err := h.SearchVectors(ctx, query, k, nil, any(nil))
		if err != nil {
			return nil, err
		}
		return res, nil
	}

	// Step 2: Refine with HNSW graph
	// Convert GPU IDs to VectorIDs and re-rank using graph
	results := make([]SearchResult, 0, k)

	for i := 0; i < len(candidateIDs) && len(results) < k; i++ {
		vecID := VectorID(candidateIDs[i])

		// Verify this is a valid vector
		locAny, ok := h.GetLocation(uint32(vecID))
		if !ok {
			continue
		}
		loc, _ := locAny.(Location)

		// Skip tombstoned vectors
		if loc.BatchIdx == -1 {
			continue
		}

		results = append(results, SearchResult{
			ID:    lbtypes.VectorID(vecID),
			Score: distances[i],
		})
	}

	return results, nil
}

// CloseGPU releases GPU resources
func (h *ArrowHNSW) CloseGPU() error {
	h.gpuMu.Lock()
	defer h.gpuMu.Unlock()

	if h.gpuIndex != nil {
		err := h.gpuIndex.Close()
		h.gpuIndex = nil
		h.gpuEnabled = false
		return err
	}

	return nil
}

// IsGPUEnabled returns whether GPU acceleration is active
func (h *ArrowHNSW) IsGPUEnabled() bool {
	h.gpuMu.RLock()
	defer h.gpuMu.RUnlock()
	return h.gpuEnabled
}
