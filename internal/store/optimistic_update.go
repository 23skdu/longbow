package store

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

type OptimisticUpdate struct {
	VectorID   string
	Vector     []float32
	Version    uint64
	Timestamp  time.Time
	RetryCount int
}

type VectorVersion struct {
	Version    uint64
	Vector     []float32
	Timestamp  time.Time
	ModifiedBy string
}

type OptimisticConcurrentConfig struct {
	MaxRetries       int    `json:"max_retries"`
	RetryDelayMs     int    `json:"retry_delay_ms"`
	VersionCacheSize int    `json:"version_cache_size"`
	EnableVersioning bool   `json:"enable_versioning"`
	ConflictStrategy string `json:"conflict_strategy"` // abort, overwrite, merge
}

type OptimisticConcurrentUpdates struct {
	logger       zerolog.Logger
	config       OptimisticConcurrentConfig
	versionCache map[string]*VectorVersion
	cacheMu      sync.RWMutex
	stats        OptimisticStats
	wg           sync.WaitGroup
	stopChan     chan struct{}
}

type OptimisticStats struct {
	UpdatesAttempted  atomic.Int64
	UpdatesSucceeded  atomic.Int64
	UpdatesConflicted atomic.Int64
	UpdatesRetried    atomic.Int64
	UpdatesAborted    atomic.Int64
}

type UpdateResult struct {
	Success    bool
	NewVersion uint64
	Conflict   bool
	Error      error
}

func NewOptimisticConcurrentUpdates(logger zerolog.Logger, config OptimisticConcurrentConfig) *OptimisticConcurrentUpdates {
	if config.MaxRetries <= 0 {
		config.MaxRetries = 3
	}
	if config.RetryDelayMs <= 0 {
		config.RetryDelayMs = 10
	}
	if config.VersionCacheSize <= 0 {
		config.VersionCacheSize = 10000
	}

	return &OptimisticConcurrentUpdates{
		logger:       logger,
		config:       config,
		versionCache: make(map[string]*VectorVersion, config.VersionCacheSize),
		stopChan:     make(chan struct{}),
	}
}

func (o *OptimisticConcurrentUpdates) GetVersion(vectorID string) (uint64, bool) {
	o.cacheMu.RLock()
	defer o.cacheMu.RUnlock()

	if v, ok := o.versionCache[vectorID]; ok {
		return v.Version, true
	}
	return 0, false
}

func (o *OptimisticConcurrentUpdates) SetVersion(vectorID string, version uint64, vector []float32) {
	o.cacheMu.Lock()
	defer o.cacheMu.Unlock()

	o.versionCache[vectorID] = &VectorVersion{
		Version:   version,
		Vector:    vector,
		Timestamp: time.Now(),
	}
}

func (o *OptimisticConcurrentUpdates) UpdateVector(vectorID string, newVector []float32, expectedVersion uint64, modifiedBy string) UpdateResult {
	o.stats.UpdatesAttempted.Add(1)

	currentVersion, exists := o.GetVersion(vectorID)

	if !exists {
		initialVersion := uint64(1)
		o.SetVersion(vectorID, initialVersion, newVector)
		o.stats.UpdatesSucceeded.Add(1)
		return UpdateResult{
			Success:    true,
			NewVersion: initialVersion,
		}
	}

	if expectedVersion != currentVersion {
		o.stats.UpdatesConflicted.Add(1)

		switch o.config.ConflictStrategy {
		case "abort":
			o.stats.UpdatesAborted.Add(1)
			return UpdateResult{
				Success:  false,
				Conflict: true,
				Error:    fmt.Errorf("version mismatch: expected %d, got %d", expectedVersion, currentVersion),
			}
		case "overwrite":
			newVersion := currentVersion + 1
			o.SetVersion(vectorID, newVersion, newVector)
			o.stats.UpdatesSucceeded.Add(1)
			return UpdateResult{
				Success:    true,
				NewVersion: newVersion,
			}
		case "merge":
			return o.mergeUpdate(vectorID, currentVersion, newVector, modifiedBy)
		default:
			o.stats.UpdatesConflicted.Add(1)
			return UpdateResult{
				Success:  false,
				Conflict: true,
				Error:    fmt.Errorf("unknown conflict strategy: %s", o.config.ConflictStrategy),
			}
		}
	}

	newVersion := currentVersion + 1
	o.SetVersion(vectorID, newVersion, newVector)
	o.stats.UpdatesSucceeded.Add(1)

	return UpdateResult{
		Success:    true,
		NewVersion: newVersion,
	}
}

func (o *OptimisticConcurrentUpdates) UpdateVectorWithRetry(vectorID string, newVector []float32, expectedVersion uint64, modifiedBy string) UpdateResult {
	o.stats.UpdatesAttempted.Add(1)

	var lastErr error
	for i := 0; i < o.config.MaxRetries; i++ {
		result := o.UpdateVector(vectorID, newVector, expectedVersion, modifiedBy)

		if result.Success {
			return result
		}

		if !result.Conflict {
			o.stats.UpdatesAborted.Add(1)
			return result
		}

		o.stats.UpdatesRetried.Add(1)
		lastErr = result.Error

		if i < o.config.MaxRetries-1 {
			currentVersion, _ := o.GetVersion(vectorID)
			expectedVersion = currentVersion
			time.Sleep(time.Duration(o.config.RetryDelayMs) * time.Millisecond)
		}
	}

	o.stats.UpdatesAborted.Add(1)
	return UpdateResult{
		Success:  false,
		Conflict: true,
		Error:    fmt.Errorf("max retries exceeded: %w", lastErr),
	}
}

func (o *OptimisticConcurrentUpdates) mergeUpdate(vectorID string, currentVersion uint64, newVector []float32, modifiedBy string) UpdateResult {
	o.cacheMu.RLock()
	oldVersion, ok := o.versionCache[vectorID]
	o.cacheMu.RUnlock()

	if !ok {
		return o.UpdateVector(vectorID, newVector, currentVersion, modifiedBy)
	}

	mergedVector := make([]float32, len(newVector))
	copy(mergedVector, newVector)

	for i := 0; i < len(mergedVector) && i < len(oldVersion.Vector); i++ {
		mergedVector[i] = (mergedVector[i] + oldVersion.Vector[i]) / 2
	}

	newVersion := currentVersion + 1
	o.SetVersion(vectorID, newVersion, mergedVector)
	o.stats.UpdatesSucceeded.Add(1)

	return UpdateResult{
		Success:    true,
		NewVersion: newVersion,
	}
}

func (o *OptimisticConcurrentUpdates) DeleteVector(vectorID string) {
	o.cacheMu.Lock()
	defer o.cacheMu.Unlock()

	delete(o.versionCache, vectorID)
}

func (o *OptimisticConcurrentUpdates) BatchUpdate(updates []OptimisticUpdate, modifiedBy string) []UpdateResult {
	results := make([]UpdateResult, len(updates))

	var wg sync.WaitGroup
	for i, update := range updates {
		wg.Add(1)
		go func(idx int, u OptimisticUpdate) {
			defer wg.Done()
			results[idx] = o.UpdateVectorWithRetry(u.VectorID, u.Vector, u.Version, modifiedBy)
		}(i, update)
	}
	wg.Wait()

	return results
}

func (o *OptimisticConcurrentUpdates) GetStats() (attempted, succeeded, conflicted, retried, aborted int64) {
	return o.stats.UpdatesAttempted.Load(),
		o.stats.UpdatesSucceeded.Load(),
		o.stats.UpdatesConflicted.Load(),
		o.stats.UpdatesRetried.Load(),
		o.stats.UpdatesAborted.Load()
}

func (o *OptimisticConcurrentUpdates) SetConfig(config OptimisticConcurrentConfig) {
	o.config = config
}

func (o *OptimisticConcurrentUpdates) GetConfig() OptimisticConcurrentConfig {
	return o.config
}

func (o *OptimisticConcurrentUpdates) ClearCache() {
	o.cacheMu.Lock()
	defer o.cacheMu.Unlock()

	o.versionCache = make(map[string]*VectorVersion, o.config.VersionCacheSize)
}

func (o *OptimisticConcurrentUpdates) GetCacheSize() int {
	o.cacheMu.RLock()
	defer o.cacheMu.RUnlock()

	return len(o.versionCache)
}

func (o *OptimisticConcurrentUpdates) GetVectorInfo(vectorID string) (*VectorVersion, bool) {
	o.cacheMu.RLock()
	defer o.cacheMu.RUnlock()

	v, ok := o.versionCache[vectorID]
	if !ok {
		return nil, false
	}

	return &VectorVersion{
		Version:    v.Version,
		Vector:     v.Vector,
		Timestamp:  v.Timestamp,
		ModifiedBy: v.ModifiedBy,
	}, true
}
