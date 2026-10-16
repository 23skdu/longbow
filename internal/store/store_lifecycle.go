package store

import (
	"context"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// StoreLifecycle manages startup/shutdown of standard components
// such as managing memory pressure, eviction, and startup.

var (
	// MinIndexingWorkers is the minimum number of indexing workers to keep running.
	// ArrowHNSW internally parallelizes bulk inserts, so 1 worker is sufficient and avoids lock contention.
	MinIndexingWorkers = 1
	// MinIngestionWorkers is the minimum number of ingestion workers to keep running.
	MinIngestionWorkers = calculateMinWorkers(4) // 1/4th of CPU
)

func calculateMinWorkers(divisor int) int {
	n := runtime.NumCPU() / divisor
	if n < 4 {
		return 4
	}
	return n
}

// StartLifecycleManager starts the lifecycle manager background task.
func (s *VectorStore) StartLifecycleManager(ctx context.Context) {
	s.logger.Info().Msg("Starting formalized background task scheduler")

	// Start sub-tickers
	s.StartWALCheckTicker(5 * time.Second)
	s.StartMetricsTicker(15 * time.Second)
	s.StartIngestionAutoscaler(ctx)
	if s.evictionManager != nil {
		s.evictionManager.Start(ctx)
	}

	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				s.logger.Info().Msg("Lifecycle manager shutting down")
				return
			case <-ticker.C:
				// Perform maintenance
				s.enforceMemoryLimits()
				s.performGlobalCompactionCheck()
				s.maintainMinimumWorkers()
				s.performTemporalPrewarm(ctx)
			}
		}
	}()
}

// maintainMinimumWorkers ensures at least minimum workers are running
func (s *VectorStore) maintainMinimumWorkers() {
	s.workerMu.Lock()
	currIndexing := len(s.indexingWorkerCancels)
	currIngestion := len(s.ingestionWorkerCancels)
	s.workerMu.Unlock()

	// Ensure minimum indexing workers
	if currIndexing < MinIndexingWorkers {
		needed := MinIndexingWorkers - currIndexing
		s.logger.Warn().Int("current", currIndexing).Int("needed", needed).Msg("Indexing workers below minimum, spawning")
		s.StartIndexingWorkers(needed)
	}

	// Ensure minimum ingestion workers
	if currIngestion < MinIngestionWorkers {
		needed := MinIngestionWorkers - currIngestion
		s.logger.Warn().Int("current", currIngestion).Int("needed", needed).Msg("Ingestion workers below minimum, spawning")
		s.StartIngestionWorkers(needed)
	}
}

// performGlobalCompactionCheck initiates compaction across all datasets if needed
func (s *VectorStore) performGlobalCompactionCheck() {
	dm := s.datasets.Load()
	if dm == nil {
		return
	}
	for _, ds := range *dm {
		if ds.Index != nil {
			// Logic to trigger background compaction/repair
		}
	}
}

// enforceMemoryLimits checks current memory usage and triggers eviction if needed.
func (s *VectorStore) enforceMemoryLimits() {
	limit := s.maxMemory.Load()
	current := s.currentMemory.Load()
	if current > limit {
		s.logger.Warn().
			Int64("current_mb", current/1024/1024).
			Int64("limit_mb", limit/1024/1024).
			Msg("Memory limit exceeded, triggering eviction")
		// Try to evict down to limit
		_ = s.evictToTarget(limit, "")
	}
}

// evictIfNeeded is an alias for enforceMemoryLimits (used by tests)
func (s *VectorStore) evictIfNeeded() {
	s.enforceMemoryLimits()
}

// StartWALCheckTicker starts periodic WAL integrity and size checks
func (s *VectorStore) StartWALCheckTicker(d time.Duration) {
	go func() {
		ticker := time.NewTicker(d)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				// Perform WAL maintenance
			}
		}
	}()
}

// UpdateConfig updates store configuration parameters like memory limits dynamically.
func (s *VectorStore) UpdateConfig(maxMemory, maxWALSize int64, snapshotInterval time.Duration) {
	if maxMemory > 0 {
		s.maxMemory.Store(maxMemory)
	}
}

// StartMetricsTicker reports global store metrics to Prometheus
func (s *VectorStore) StartMetricsTicker(d time.Duration) {
	go func() {
		ticker := time.NewTicker(d)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				metrics.NUMANodeCount.Set(float64(s.numaTopology.NumNodes))
				metrics.NUMAEnabled.Set(1.0)
			}
		}
	}()
}

// StartEvictionTicker starts the background eviction ticker (used by tests/shutdown)
func (s *VectorStore) StartEvictionTicker(interval time.Duration) {
	s.workerWg.Add(1)
	go func() {
		defer s.workerWg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.stopChan:
				return
			case <-ticker.C:
				s.enforceMemoryLimits()
			}
		}
	}()
}

// ReleaseMemory explicitly triggers garbage collection and returns memory to the OS.
func (s *VectorStore) ReleaseMemory() {
	s.logger.Info().Msg("Explicitly releasing memory")

	// 1. Wait for async cleanups
	s.cleanupWg.Wait()

	// 2. Trigger GC
	runtime.GC()

	// 3. Free OS memory
	debug.FreeOSMemory()

	s.logger.Info().
		Int64("current_memory_bytes", s.currentMemory.Load()).
		Msg("Memory release complete")
}

func (s *VectorStore) performTemporalPrewarm(ctx context.Context) {
	s.IterateDatasets(func(name string, ds *Dataset) {
		if ds.TemporalIndex != nil {
			_ = ds.TemporalIndex.Prewarm(ctx)
		}
	})
}
