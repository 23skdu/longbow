package store

import (
	"context"
	"time"
)

// StoreLifecycle manages startup/shutdown of standard components
// such as managing memory pressure, eviction, and startup.

// evictDataset evicts a dataset from memory.
func (s *VectorStore) evictDataset(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ds, ok := s.datasets[name]
	if !ok {
		return
	}

	size := ds.SizeBytes.Load()
	s.currentMemory.Add(-size)

	if ds.Index != nil {
		ds.Index.Close()
	}

	// Release records
	for _, r := range ds.Records {
		r.Release()
	}

	delete(s.datasets, name)
	// metrics.DatasetCount.Dec()
	// metrics.EvaluatedEvictions.Inc()
}

// StartLifecycleManager starts the lifecycle manager background task.
func (s *VectorStore) StartLifecycleManager(ctx context.Context) {
	// Simple background task placeholder
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Perform maintenance
				s.enforceMemoryLimits()
			}
		}
	}()
}

// enforceMemoryLimits checks current memory usage and triggers eviction if needed.
func (s *VectorStore) enforceMemoryLimits() {
	limit := s.maxMemory.Load()
	current := s.currentMemory.Load()
	if current > limit {
		// Try to evict down to limit
		_ = s.evictToTarget(limit)
	}
}

// evictIfNeeded is an alias for enforceMemoryLimits (used by tests)
func (s *VectorStore) evictIfNeeded() {
	s.enforceMemoryLimits()
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
