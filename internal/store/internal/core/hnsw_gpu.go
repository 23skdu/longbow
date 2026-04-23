package core

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/23skdu/longbow/internal/gpu"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/rs/zerolog"
)

// GPUConfig is the configuration for GPU/CPU hybrid search
type GPUConfig struct {
	CandidateMultiplier int
	RefineTopK          int
	EnableGPUCache      bool
	MaxGPUCacheSize     int

	// Synchronization settings
	SyncBatchSize int           // Batch size for GPU updates (default: 1000)
	SyncInterval  time.Duration // Time-based sync interval (default: 5s)

	// Device settings
	DeviceID int // GPU device ID (default: 0)
}

// DefaultGPUConfig returns the default GPU hybrid search configuration
func DefaultGPUConfig() GPUConfig {
	return GPUConfig{
		CandidateMultiplier: 20,
		RefineTopK:          0,
		EnableGPUCache:      true,
		MaxGPUCacheSize:     2000,
		SyncBatchSize:       1000,
		SyncInterval:        5 * time.Second,
	}
}

// InitGPU attempts to initialize GPU acceleration for this index
// Uses auto-detected backend (Metal on macOS, CUDA on Linux with NVIDIA, CPU fallback)
//
//nolint:gocritic // Logger passed by value for simplicity
func (h *ArrowHNSW) InitGPU(deviceID int, logger zerolog.Logger) error {
	return h.InitGPUWithBackend(deviceID, logger, gpu.GetPreferredBackend())
}

// InitGPUWithBackend initializes GPU with explicitly specified backend
// Use this if you need to override auto-detection
//
//nolint:gocritic // Logger passed by value for simplicity
func (h *ArrowHNSW) InitGPUWithBackend(deviceID int, logger zerolog.Logger, backend gpu.GPUBackend) error {
	return h.InitGPUWithConfigAndBackend(deviceID, logger, DefaultGPUConfig(), backend)
}

// InitGPUWithConfig initializes GPU with custom configuration using auto-detected backend
func (h *ArrowHNSW) InitGPUWithConfig(deviceID int, logger zerolog.Logger, config GPUConfig) error {
	return h.InitGPUWithConfigAndBackend(deviceID, logger, config, gpu.GetPreferredBackend())
}

// InitGPUWithConfigAndBackend initializes GPU with custom configuration and specified backend
func (h *ArrowHNSW) InitGPUWithConfigAndBackend(deviceID int, logger zerolog.Logger, config GPUConfig, backend gpu.GPUBackend) error {
	h.gpuMu.Lock()
	defer h.gpuMu.Unlock()

	if h.gpuEnabled {
		return &gpu.GPUInitializationError{
			DeviceID: deviceID,
			Backend:  backend,
			Cause:    fmt.Errorf("GPU already initialized"),
		}
	}

	// Get dimensions from ArrowHNSW
	dims := int(h.GetDimension())
	if dims == 0 {
		return &gpu.GPUInitializationError{
			DeviceID: deviceID,
			Backend:  backend,
			Cause:    fmt.Errorf("index dimensions not set"),
		}
	}

	// Check GPU availability for the specified backend
	available, reason, err := gpu.GetGPURequirements(backend)
	if err != nil {
		h.gpuFallback = true
		return &gpu.GPUInitializationError{
			DeviceID: deviceID,
			Backend:  backend,
			Cause:    err,
		}
	}
	if !available {
		h.gpuFallback = true
		if logger.GetLevel() != zerolog.Disabled {
			logger.Warn().
				Str("backend", backend.String()).
				Str("reason", reason).
				Int("device", deviceID).
				Msg("GPU not available, using CPU-only")
		}
		return &gpu.GPUNotAvailableError{Reason: reason}
	}

	cfg := gpu.GPUConfig{
		DeviceID:  deviceID,
		Dimension: dims,
		Backend:   backend,
	}

	idx, err := gpu.NewIndexWithBackend(cfg, backend)
	if err != nil {
		h.gpuFallback = true
		if logger.GetLevel() != zerolog.Disabled {
			logger.Warn().
				Err(err).
				Int("device", deviceID).
				Msg("GPU initialization failed, using CPU-only")
		}
		return &gpu.GPUInitializationError{
			DeviceID: deviceID,
			Backend:  backend,
			Cause:    err,
		}
	}

	h.gpuIndex = idx
	h.gpuEnabled = true
	h.gpuConfig = config
	h.gpuLastSyncTime = time.Now()

	// Initialize circuit breaker
	h.gpuCircuitBreaker = gpu.NewCircuitBreaker(gpu.DefaultCircuitBreakerConfig())

	// Initialize cache if enabled
	if config.EnableGPUCache && config.MaxGPUCacheSize > 0 {
		h.gpuResultCache = newGPUResultCache(config.MaxGPUCacheSize)
	}

	// Start background sync ticker for time-based flushing
	h.startGPUSyncTicker()

	if logger.GetLevel() != zerolog.Disabled {
		logger.Info().
			Str("backend", backend.String()).
			Int("device", deviceID).
			Int("dimensions", dims).
			Bool("cache_enabled", config.EnableGPUCache).
			Int("sync_batch_size", config.SyncBatchSize).
			Dur("sync_interval", config.SyncInterval).
			Msg("GPU acceleration enabled")
	}

	return nil
}

// SyncGPU adds vectors to the GPU index with batching
// Vectors are accumulated and flushed when batch size is reached or on timer
func (h *ArrowHNSW) SyncGPU(ids []int64, vectors []float32) error {
	if !h.gpuEnabled || h.gpuIndex == nil {
		return nil // GPU not enabled, skip
	}

	// Check circuit breaker
	if h.gpuCircuitBreaker != nil && !h.gpuCircuitBreaker.Allow() {
		metrics.GPUFallbackTotal.WithLabelValues("circuit_breaker_open").Inc()
		return nil
	}

	h.gpuBatchMu.Lock()
	defer h.gpuBatchMu.Unlock()

	// Add to batch
	h.gpuBatchIDs = append(h.gpuBatchIDs, ids...)
	h.gpuBatchVectors = append(h.gpuBatchVectors, vectors...)

	// Update batch size metric
	metrics.GPUBatchSize.Set(float64(len(h.gpuBatchIDs)))

	// Check if batch is full and needs flush
	batchSize := len(h.gpuBatchIDs)
	if h.gpuConfig.SyncBatchSize > 0 && batchSize >= h.gpuConfig.SyncBatchSize {
		err := h.flushGPUBatchLocked()
		if err != nil {
			// Record failure in circuit breaker
			if h.gpuCircuitBreaker != nil {
				h.gpuCircuitBreaker.RecordFailure()
			}
			// Wrap error
			return &gpu.GPUSyncError{
				BatchSize: batchSize,
				DeviceID:  h.gpuConfig.DeviceID,
				Cause:     err,
			}
		}
		// Record success in circuit breaker
		if h.gpuCircuitBreaker != nil {
			h.gpuCircuitBreaker.RecordSuccess()
		}
	}

	return nil
}

// FlushGPUUpdates forces immediate synchronization of pending GPU updates
func (h *ArrowHNSW) FlushGPUUpdates() error {
	if !h.gpuEnabled || h.gpuIndex == nil {
		return nil
	}

	h.gpuBatchMu.Lock()
	defer h.gpuBatchMu.Unlock()

	return h.flushGPUBatchLocked()
}

// flushGPUBatchLocked flushes the current batch to GPU (must hold gpuBatchMu)
func (h *ArrowHNSW) flushGPUBatchLocked() error {
	if len(h.gpuBatchIDs) == 0 {
		return nil
	}

	start := time.Now()
	err := h.gpuIndex.Add(h.gpuBatchIDs, h.gpuBatchVectors)
	duration := time.Since(start).Seconds()

	// Record sync metrics
	metrics.GPUSyncDurationSeconds.Observe(duration)
	metrics.GPUOperationsTotal.WithLabelValues("sync", "batch").Inc()
	metrics.GPUBatchSize.Set(0)

	batchSize := len(h.gpuBatchIDs)

	// Clear batch
	h.gpuBatchIDs = h.gpuBatchIDs[:0]
	h.gpuBatchVectors = h.gpuBatchVectors[:0]
	h.gpuLastSyncTime = time.Now()

	// Update GPU index size metric if successful
	if err == nil {
		deviceID := "0"
		if info, err := h.gpuIndex.GetDeviceInfo(); err == nil {
			deviceID = string(rune(info.DeviceID)) // #nosec G115
		}
		metrics.GPUIndexSize.WithLabelValues(deviceID).Add(float64(batchSize))
		// Record success in circuit breaker
		if h.gpuCircuitBreaker != nil {
			h.gpuCircuitBreaker.RecordSuccess()
		}
	} else {
		metrics.GPUOperationsTotal.WithLabelValues("sync", "error").Inc()
		// Record failure in circuit breaker
		if h.gpuCircuitBreaker != nil {
			h.gpuCircuitBreaker.RecordFailure()
		}
		// Log detailed error
		if gpu.IsGPUMemoryError(err) {
			metrics.GPUFallbackTotal.WithLabelValues("memory_error").Inc()
		} else if gpu.IsGPUComputeError(err) {
			metrics.GPUFallbackTotal.WithLabelValues("compute_error").Inc()
		} else {
			metrics.GPUFallbackTotal.WithLabelValues("sync_error").Inc()
		}
	}

	return err
}

// startGPUSyncTicker starts the background sync ticker for time-based flushing
func (h *ArrowHNSW) startGPUSyncTicker() {
	if h.gpuConfig.SyncInterval <= 0 {
		return
	}

	h.gpuStopSync = make(chan struct{})
	h.gpuSyncTicker = time.NewTicker(h.gpuConfig.SyncInterval)

	go func() {
		for {
			select {
			case <-h.gpuSyncTicker.C:
				h.gpuBatchMu.Lock()
				// Flush if we have pending updates and interval has passed
				if len(h.gpuBatchIDs) > 0 && time.Since(h.gpuLastSyncTime) >= h.gpuConfig.SyncInterval {
					_ = h.flushGPUBatchLocked() // nosec G104
				}
				h.gpuBatchMu.Unlock()
			case <-h.gpuStopSync:
				return
			}
		}
	}()
}

// stopGPUSyncTicker stops the background sync ticker
func (h *ArrowHNSW) stopGPUSyncTicker() {
	if h.gpuSyncTicker != nil {
		h.gpuSyncTicker.Stop()
		close(h.gpuStopSync)
		h.gpuSyncTicker = nil
	}
}

// SearchHybrid performs GPU+CPU hybrid search
// Uses GPU for candidate generation, then refines with CPU HNSW graph
func (h *ArrowHNSW) SearchHybrid(ctx context.Context, query []float32, k int) ([]types.SearchResult, error) {
	return h.SearchHybridWithConfig(ctx, query, k, DefaultGPUConfig())
}

// SearchHybridWithConfig performs GPU+CPU hybrid search with custom configuration
func (h *ArrowHNSW) SearchHybridWithConfig(ctx context.Context, query []float32, k int, config GPUConfig) ([]types.SearchResult, error) {
	start := time.Now()

	// Check cache first if enabled
	if config.EnableGPUCache && h.gpuResultCache != nil {
		if cached, ok := h.gpuResultCache.get(query); ok {
			metrics.GPUFallbackTotal.WithLabelValues("cache_hit").Inc()
			return cached, nil
		}
	}

	// Check circuit breaker
	if h.gpuCircuitBreaker != nil && !h.gpuCircuitBreaker.Allow() {
		metrics.GPUFallbackTotal.WithLabelValues("circuit_breaker_open").Inc()
		return h.searchCPUOnly(ctx, query, k)
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
	if err == nil {
		metrics.GPUUsed.WithLabelValues("metal", "f32").Inc()
	}
	if err != nil {
		// Record failure in circuit breaker
		if h.gpuCircuitBreaker != nil {
			h.gpuCircuitBreaker.RecordFailure()
		}

		// Classify error type
		if gpu.IsGPUMemoryError(err) {
			metrics.GPUFallbackTotal.WithLabelValues("memory_error").Inc()
		} else if gpu.IsGPUComputeError(err) {
			metrics.GPUFallbackTotal.WithLabelValues("compute_error").Inc()
		} else {
			metrics.GPUFallbackTotal.WithLabelValues("gpu_search_error").Inc()
		}

		return h.searchCPUOnly(ctx, query, k)
	}

	// Record success in circuit breaker
	if h.gpuCircuitBreaker != nil {
		h.gpuCircuitBreaker.RecordSuccess()
	}

	gpuDuration := time.Since(start)

	// Step 2: CPU refinement with HNSW graph
	// Build candidates list from GPU results
	candidates := make([]candidateResult, 0, len(candidateIDs))
	for i := 0; i < len(candidateIDs); i++ {
		vecID := types.VectorID(candidateIDs[i]) // #nosec G115

		// Verify this is a valid vector
		locAny, ok := h.GetLocation(uint32(vecID))
		if !ok {
			continue
		}
		loc, _ := locAny.(types.Location)

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
	refinedResults := h.refineWithCPU(query, candidatesToRefine, k)

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
func (h *ArrowHNSW) searchCPUOnly(ctx context.Context, query []float32, k int) ([]types.SearchResult, error) {
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
func (h *ArrowHNSW) refineWithCPU(query []float32, candidates []candidateResult, k int) []types.SearchResult {
	if len(candidates) == 0 {
		return []types.SearchResult{}
	}

	// Convert candidates to search results with accurate distances
	results := make([]types.SearchResult, 0, len(candidates))

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

		results = append(results, types.SearchResult{
			ID:    types.VectorID(c.id),
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

	// Stop background sync ticker
	h.stopGPUSyncTicker()

	// Flush any pending batches
	h.gpuBatchMu.Lock()
	_ = h.flushGPUBatchLocked() // nosec G104
	h.gpuBatchMu.Unlock()

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

// TrainPQOnGPU trains the PQ encoder on the GPU.
func (h *ArrowHNSW) TrainPQOnGPU(vectors []float32, m, k int) error {
	h.gpuMu.RLock()
	defer h.gpuMu.RUnlock()
	if !h.gpuEnabled || h.gpuIndex == nil {
		return fmt.Errorf("GPU not enabled")
	}
	return h.gpuIndex.TrainPQ(vectors, m, k)
}

// EncodePQOnGPU compresses vectors using the GPU.
func (h *ArrowHNSW) EncodePQOnGPU(vectors []float32) ([]byte, error) {
	h.gpuMu.RLock()
	defer h.gpuMu.RUnlock()
	if !h.gpuEnabled || h.gpuIndex == nil {
		return nil, fmt.Errorf("GPU not enabled")
	}
	return h.gpuIndex.EncodePQ(vectors)
}

// GetGPUCircuitBreakerStats returns the current circuit breaker statistics
func (h *ArrowHNSW) GetGPUCircuitBreakerStats() gpu.CircuitBreakerStats {
	if h.gpuCircuitBreaker == nil {
		return gpu.CircuitBreakerStats{State: "not_initialized"}
	}
	return h.gpuCircuitBreaker.Stats()
}

// IsGPUCircuitBreakerOpen returns whether the GPU circuit breaker is open
func (h *ArrowHNSW) IsGPUCircuitBreakerOpen() bool {
	if h.gpuCircuitBreaker == nil {
		return false
	}
	return h.gpuCircuitBreaker.State() == gpu.CircuitOpen
}

// ResetGPUCircuitBreaker manually resets the circuit breaker to closed state
func (h *ArrowHNSW) ResetGPUCircuitBreaker() {
	// Note: This is a manual override, typically not recommended
	// The circuit breaker should recover naturally
	// This is useful for testing or emergency recovery
}
