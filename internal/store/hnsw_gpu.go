package store

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/23skdu/longbow/internal/gpu"
	"github.com/23skdu/longbow/internal/metrics"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/rs/zerolog"
)

// GPUConfig is the configuration for GPU/CPU hybrid search
type GPUConfig struct {
	CandidateMultiplier int
	RefineTopK          int
	EnableGPUCache      bool
	MaxGPUCacheSize     int
}

// DefaultGPUConfig returns the default GPU hybrid search configuration
func DefaultGPUConfig() GPUConfig {
	return GPUConfig{
		CandidateMultiplier: 10,
		RefineTopK:          0,
		EnableGPUCache:      false,
		MaxGPUCacheSize:     1000,
	}
}

// InitGPU attempts to initialize GPU acceleration for this index
//
// InitGPU attempts to initialize GPU acceleration for this index
//
//nolint:gocritic // Logger passed by value for simplicity
func (h *ArrowHNSW) InitGPU(deviceID int, logger zerolog.Logger) error {
	return h.InitGPUWithConfig(deviceID, logger, DefaultGPUConfig())
}

// InitGPUWithConfig initializes GPU with custom configuration
func (h *ArrowHNSW) InitGPUWithConfig(deviceID int, logger zerolog.Logger, config GPUConfig) error {
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
	h.gpuConfig = config

	// Initialize cache if enabled
	if config.EnableGPUCache && config.MaxGPUCacheSize > 0 {
		h.gpuResultCache = newGPUResultCache(config.MaxGPUCacheSize)
	}

	if logger.GetLevel() != zerolog.Disabled {
		logger.Info().
			Int("device", deviceID).
			Int("dimensions", dims).
			Bool("cache_enabled", config.EnableGPUCache).
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

	start := time.Now()
	err := h.gpuIndex.Add(ids, vectors)
	duration := time.Since(start).Seconds()

	// Record sync duration
	metrics.GPUSyncDurationSeconds.Observe(duration)

	// Update GPU index size metric if successful
	if err == nil {
		deviceID := "0" // Default device
		if h.gpuIndex != nil {
			// Try to get device info if available
			if info, err := h.gpuIndex.GetDeviceInfo(); err == nil {
				deviceID = string(rune(info.DeviceID))
			}
		}
		metrics.GPUIndexSize.WithLabelValues(deviceID).Add(float64(len(ids)))
	}

	return err
}

// SearchHybrid performs GPU+CPU hybrid search
// Uses GPU for candidate generation, then refines with CPU HNSW graph
func (h *ArrowHNSW) SearchHybrid(ctx context.Context, query []float32, k int) ([]SearchResult, error) {
	return h.SearchHybridWithConfig(ctx, query, k, DefaultGPUConfig())
}

// SearchHybridWithConfig performs GPU+CPU hybrid search with custom configuration
func (h *ArrowHNSW) SearchHybridWithConfig(ctx context.Context, query []float32, k int, config GPUConfig) ([]SearchResult, error) {
	start := time.Now()

	// Check cache first if enabled
	if config.EnableGPUCache && h.gpuResultCache != nil {
		if cached, ok := h.gpuResultCache.get(query); ok {
			metrics.GPUFallbackTotal.WithLabelValues("cache_hit").Inc()
			return cached, nil
		}
	}

	// If GPU not enabled or failed, use pure CPU
	if !h.gpuEnabled || h.gpuIndex == nil {
		metrics.GPUFallbackTotal.WithLabelValues("not_enabled").Inc()
		return h.searchCPUOnly(ctx, query, k)
	}

	// Check if we have enough vectors to warrant GPU usage
	vectorCount := h.Len()
	if vectorCount < 1000 { // Minimum threshold for GPU
		return h.searchCPUOnly(ctx, query, k)
	}

	// Step 1: GPU generates candidates
	candidateCount := k * config.CandidateMultiplier
	if candidateCount > vectorCount {
		candidateCount = vectorCount
	}

	candidateIDs, distances, err := h.gpuIndex.Search(query, candidateCount)
	if err != nil {
		metrics.GPUFallbackTotal.WithLabelValues("gpu_search_error").Inc()
		return h.searchCPUOnly(ctx, query, k)
	}

	gpuDuration := time.Since(start)

	// Step 2: CPU refinement with HNSW graph
	// Build candidates list from GPU results
	candidates := make([]candidateResult, 0, len(candidateIDs))
	for i := 0; i < len(candidateIDs); i++ {
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

		candidates = append(candidates, candidateResult{
			id:       vecID,
			distance: distances[i],
			index:    i,
		})
	}

	// Deduplicate candidates
	candidates = deduplicateCandidates(candidates)

	// Step 3: Refine candidates using HNSW graph traversal
	refineTopK := config.RefineTopK
	if refineTopK == 0 {
		refineTopK = k
	}

	// Ensure we don't try to refine more than we have
	if refineTopK > len(candidates) {
		refineTopK = len(candidates)
	}

	// Sort candidates by GPU distance for initial ranking
	// Then take top refineTopK for detailed CPU refinement
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].distance < candidates[j].distance
	})

	candidatesToRefine := candidates[:refineTopK]

	// Perform CPU refinement on selected candidates
	refinedResults := h.refineWithCPU(ctx, query, candidatesToRefine, k)

	// Calculate CPU refinement duration
	cpuDuration := time.Since(start) - gpuDuration

	// Record metrics
	metrics.GPUSearchDurationSeconds.WithLabelValues("gpu").Observe(gpuDuration.Seconds())
	metrics.GPUSearchDurationSeconds.WithLabelValues("cpu_refinement").Observe(cpuDuration.Seconds())

	totalDuration := time.Since(start)
	metrics.VectorSearchGPULatencySeconds.WithLabelValues("hybrid").Observe(totalDuration.Seconds())

	// Cache results if enabled
	if config.EnableGPUCache && h.gpuResultCache != nil {
		h.gpuResultCache.put(query, refinedResults)
	}

	return refinedResults, nil
}

// searchCPUOnly performs pure CPU search
func (h *ArrowHNSW) searchCPUOnly(ctx context.Context, query []float32, k int) ([]SearchResult, error) {
	start := time.Now()
	results, err := h.SearchVectors(ctx, query, k, nil, any(nil))
	if err != nil {
		return nil, err
	}

	duration := time.Since(start).Seconds()
	metrics.GPUSearchDurationSeconds.WithLabelValues("cpu_fallback").Observe(duration)

	return results, nil
}

// refineWithCPU performs CPU-based refinement on GPU candidates
func (h *ArrowHNSW) refineWithCPU(ctx context.Context, query []float32, candidates []candidateResult, k int) []SearchResult {
	if len(candidates) == 0 {
		return []SearchResult{}
	}

	// Convert candidates to search results with accurate distances
	results := make([]SearchResult, 0, len(candidates))

	for _, c := range candidates {
		// Get the actual vector from the index
		vector, err := h.GetVector(uint32(c.id))
		if err != nil {
			continue // Skip if vector not found
		}

		// Calculate accurate distance using CPU distance function
		var distance float32
		switch v := vector.(type) {
		case []float32:
			distance = h.calculateDistance(query, v)
		default:
			// For other types, use the GPU distance as approximation
			distance = c.distance
		}

		results = append(results, SearchResult{
			ID:    lbtypes.VectorID(c.id),
			Score: distance,
		})
	}

	// Sort by distance and return top k
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score < results[j].Score
	})

	if len(results) > k {
		return results[:k]
	}
	return results
}

// calculateDistance computes the distance between two vectors
func (h *ArrowHNSW) calculateDistance(a, b []float32) float32 {
	if h.distFunc != nil {
		d, _ := h.distFunc(a, b)
		return d
	}

	// Default to L2 distance
	var sum float32
	for i := 0; i < len(a) && i < len(b); i++ {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return sum
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
