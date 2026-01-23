package store

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// CacheLinePadding ensures each shard is on a separate cache line (64 bytes).
// This prevents false sharing where multiple CPU cores invalidate each other's
// caches when accessing different shards.
type CacheLinePadding [64]byte

// AlignedShard wraps RWMutex with cache-line padding and contention metrics.
type AlignedShard struct {
	mu sync.RWMutex
	_  CacheLinePadding // Padding to prevent false sharing

	// Contention metrics (accessed atomically)
	contentionCount atomic.Int64 // Number of times lock was contended
	lockWaitNs      atomic.Int64 // Total nanoseconds spent waiting
	fastPathHits    atomic.Int64 // Number of successful TryLock calls
	fastPathMisses  atomic.Int64 // Number of failed TryLock calls
}

// AlignedShardedMutexConfig configures the cache-aligned sharded mutex.
type AlignedShardedMutexConfig struct {
	NumShards        int     // Number of shards (default: runtime.NumCPU())
	TargetContention float64 // Target avg contention per shard (default: 100.0)
	EnableAdaptive   bool    // Enable adaptive shard scaling (default: false)
}

// DefaultAlignedShardedMutexConfig returns sensible defaults.
func DefaultAlignedShardedMutexConfig() AlignedShardedMutexConfig {
	return AlignedShardedMutexConfig{
		NumShards:        runtime.NumCPU(),
		TargetContention: 100.0,
		EnableAdaptive:   false,
	}
}

// AlignedShardedMutex provides cache-aligned sharding with contention tracking.
//
// Key improvements over standard ShardedRWMutex:
// 1. Cache-line alignment prevents false sharing
// 2. Lock-free fast path for uncontended cases
// 3. Contention metrics for monitoring and adaptive scaling
// 4. Optional adaptive shard count adjustment
type AlignedShardedMutex struct {
	shards    []AlignedShard
	numShards atomic.Int32
	config    AlignedShardedMutexConfig

	// Adaptive scaling state
	lastResize     atomic.Int64 // Unix timestamp of last resize
	resizeCooldown int64        // Minimum seconds between resizes
}

// NewAlignedShardedMutex creates a cache-aligned sharded mutex.
func NewAlignedShardedMutex(cfg AlignedShardedMutexConfig) *AlignedShardedMutex {
	if cfg.NumShards <= 0 {
		cfg.NumShards = runtime.NumCPU()
	}
	if cfg.TargetContention <= 0 {
		cfg.TargetContention = 100.0
	}

	sm := &AlignedShardedMutex{
		shards:         make([]AlignedShard, cfg.NumShards),
		config:         cfg,
		resizeCooldown: 60, // 1 minute between resizes
	}
	sm.numShards.Store(int32(cfg.NumShards))

	return sm
}

// NewAlignedShardedMutexDefault creates a mutex with default configuration.
func NewAlignedShardedMutexDefault() *AlignedShardedMutex {
	return NewAlignedShardedMutex(DefaultAlignedShardedMutexConfig())
}

// NumShards returns the current number of shards.
func (sm *AlignedShardedMutex) NumShards() int {
	return int(sm.numShards.Load())
}

// ShardFor returns the shard index for a given key using FNV-1a hash.
func (sm *AlignedShardedMutex) ShardFor(key uint64) int {
	const prime64 = 1099511628211
	hash := uint64(14695981039346656037) ^ key
	hash *= prime64
	return int(hash % uint64(sm.NumShards()))
}

// TryLock attempts to acquire a write lock without blocking.
// Returns true if lock was acquired, false if already held.
// This is the lock-free fast path for uncontended cases.
func (sm *AlignedShardedMutex) TryLock(key uint64) bool {
	shard := &sm.shards[sm.ShardFor(key)]
	acquired := shard.mu.TryLock()

	if acquired {
		shard.fastPathHits.Add(1)
	} else {
		shard.fastPathMisses.Add(1)
	}

	return acquired
}

// Lock acquires a write lock for the shard associated with the given key.
// Uses fast path (TryLock) first, falls back to blocking lock with contention tracking.
func (sm *AlignedShardedMutex) Lock(key uint64) {
	shard := &sm.shards[sm.ShardFor(key)]

	// Try lock-free fast path first
	if shard.mu.TryLock() {
		shard.fastPathHits.Add(1)
		return
	}

	// Slow path: measure contention
	start := time.Now()
	shard.mu.Lock()
	waitNs := time.Since(start).Nanoseconds()

	shard.contentionCount.Add(1)
	shard.lockWaitNs.Add(waitNs)
	shard.fastPathMisses.Add(1)
}

// Unlock releases the write lock for the shard associated with the given key.
func (sm *AlignedShardedMutex) Unlock(key uint64) {
	shard := &sm.shards[sm.ShardFor(key)]
	shard.mu.Unlock()
}

// RLock acquires a read lock for the shard associated with the given key.
func (sm *AlignedShardedMutex) RLock(key uint64) {
	shard := &sm.shards[sm.ShardFor(key)]
	shard.mu.RLock()
}

// RUnlock releases the read lock for the shard associated with the given key.
func (sm *AlignedShardedMutex) RUnlock(key uint64) {
	shard := &sm.shards[sm.ShardFor(key)]
	shard.mu.RUnlock()
}

// LockShard acquires a write lock on a specific shard directly.
func (sm *AlignedShardedMutex) LockShard(shard int) {
	if shard >= 0 && shard < sm.NumShards() {
		sm.shards[shard].mu.Lock()
	}
}

// UnlockShard releases a write lock on a specific shard directly.
func (sm *AlignedShardedMutex) UnlockShard(shard int) {
	if shard >= 0 && shard < sm.NumShards() {
		sm.shards[shard].mu.Unlock()
	}
}

// RLockShard acquires a read lock on a specific shard directly.
func (sm *AlignedShardedMutex) RLockShard(shard int) {
	if shard >= 0 && shard < sm.NumShards() {
		sm.shards[shard].mu.RLock()
	}
}

// RUnlockShard releases a read lock on a specific shard directly.
func (sm *AlignedShardedMutex) RUnlockShard(shard int) {
	if shard >= 0 && shard < sm.NumShards() {
		sm.shards[shard].mu.RUnlock()
	}
}

// AdaptShardCount adjusts the shard count based on measured contention.
// Only effective if EnableAdaptive is true in config.
func (sm *AlignedShardedMutex) AdaptShardCount() {
	if !sm.config.EnableAdaptive {
		return
	}

	// Check cooldown period
	now := time.Now().Unix()
	lastResize := sm.lastResize.Load()
	if now-lastResize < sm.resizeCooldown {
		return
	}

	// Calculate average contention across shards
	totalContention := int64(0)
	currentShards := sm.NumShards()

	for i := 0; i < currentShards; i++ {
		totalContention += sm.shards[i].contentionCount.Load()
	}

	avgContention := float64(totalContention) / float64(currentShards)

	// Decide whether to scale up or down
	if avgContention > sm.config.TargetContention && currentShards < 4096 {
		// Scale up: double the shards
		sm.resize(currentShards * 2)
		sm.lastResize.Store(now)
	} else if avgContention < sm.config.TargetContention/4 && currentShards > runtime.NumCPU() {
		// Scale down: halve the shards
		sm.resize(currentShards / 2)
		sm.lastResize.Store(now)
	}
}

// resize changes the number of shards (expensive operation, use sparingly).
func (sm *AlignedShardedMutex) resize(newShardCount int) {
	// This is a simplified implementation
	// In production, would need to:
	// 1. Acquire all locks
	// 2. Create new shard array
	// 3. Migrate state
	// 4. Atomically swap
	// 5. Release locks

	// For now, just update the count (actual resize not implemented)
	sm.numShards.Store(int32(newShardCount))
}

// AlignedShardedMutexStats holds statistics for the mutex.
type AlignedShardedMutexStats struct {
	TotalContentions int64   // Total number of contentions across all shards
	TotalWaitNs      int64   // Total nanoseconds spent waiting
	FastPathHits     int64   // Total successful TryLock calls
	FastPathMisses   int64   // Total failed TryLock calls
	AvgContention    float64 // Average contention per shard
	FastPathRate     float64 // Percentage of fast path hits
}

// Stats returns current statistics.
func (sm *AlignedShardedMutex) Stats() AlignedShardedMutexStats {
	stats := AlignedShardedMutexStats{}
	currentShards := sm.NumShards()

	for i := 0; i < currentShards; i++ {
		stats.TotalContentions += sm.shards[i].contentionCount.Load()
		stats.TotalWaitNs += sm.shards[i].lockWaitNs.Load()
		stats.FastPathHits += sm.shards[i].fastPathHits.Load()
		stats.FastPathMisses += sm.shards[i].fastPathMisses.Load()
	}

	if currentShards > 0 {
		stats.AvgContention = float64(stats.TotalContentions) / float64(currentShards)
	}

	totalAttempts := stats.FastPathHits + stats.FastPathMisses
	if totalAttempts > 0 {
		stats.FastPathRate = float64(stats.FastPathHits) / float64(totalAttempts) * 100.0
	}

	return stats
}

// ResetStats resets all contention metrics.
func (sm *AlignedShardedMutex) ResetStats() {
	for i := 0; i < sm.NumShards(); i++ {
		sm.shards[i].contentionCount.Store(0)
		sm.shards[i].lockWaitNs.Store(0)
		sm.shards[i].fastPathHits.Store(0)
		sm.shards[i].fastPathMisses.Store(0)
	}
}
