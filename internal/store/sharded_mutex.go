package store

import (
	"errors"
	"hash/fnv"
	"runtime"
	"sync"
	"sync/atomic"
)

// =============================================================================
// ShardedRWMutex - Subtask 1: Sharded Read-Write Lock
// =============================================================================

// ShardedRWMutexConfig configures the sharded mutex.
type ShardedRWMutexConfig struct {
	NumShards int // Number of shards (default: runtime.NumCPU())
}

// DefaultShardedRWMutexConfig returns sensible defaults.
func DefaultShardedRWMutexConfig() ShardedRWMutexConfig {
	return ShardedRWMutexConfig{
		NumShards: runtime.NumCPU(),
	}
}

// Validate checks configuration validity.
func (c ShardedRWMutexConfig) Validate() error {
	if c.NumShards <= 0 {
		return errors.New("NumShards must be positive")
	}
	return nil
}

// ShardedRWMutexStats holds statistics for the sharded mutex.
type ShardedRWMutexStats struct {
	TotalLocks  int64
	TotalRLocks int64
}

// ShardedRWMutex provides a sharded read-write mutex for reduced contention.
// Different keys may hash to different shards, allowing concurrent access.
type ShardedRWMutex struct {
	shards    []sync.RWMutex
	numShards int

	// Statistics
	totalLocks  atomic.Int64
	totalRLocks atomic.Int64
}

// NewShardedRWMutex creates a new sharded mutex with the given configuration.
func NewShardedRWMutex(cfg ShardedRWMutexConfig) *ShardedRWMutex {
	if cfg.NumShards <= 0 {
		cfg.NumShards = runtime.NumCPU()
	}
	return &ShardedRWMutex{
		shards:    make([]sync.RWMutex, cfg.NumShards),
		numShards: cfg.NumShards,
	}
}

// NewShardedRWMutexDefault creates a sharded mutex with default configuration.
func NewShardedRWMutexDefault() *ShardedRWMutex {
	return NewShardedRWMutex(DefaultShardedRWMutexConfig())
}

// NumShards returns the number of shards.
func (sm *ShardedRWMutex) NumShards() int {
	return sm.numShards
}

// ShardFor returns the shard index for a given key using FNV-1a hash.
func (sm *ShardedRWMutex) ShardFor(key uint64) int {
	h := fnv.New64a()
	b := make([]byte, 8)
	b[0] = byte(key)
	b[1] = byte(key >> 8)
	b[2] = byte(key >> 16)
	b[3] = byte(key >> 24)
	b[4] = byte(key >> 32)
	b[5] = byte(key >> 40)
	b[6] = byte(key >> 48)
	b[7] = byte(key >> 56)
	h.Write(b)
	return int(h.Sum64() % uint64(sm.numShards)) //nolint:gosec // G115 - shard index guaranteed to fit in int
}

// Lock acquires a write lock for the shard associated with the given key.
func (sm *ShardedRWMutex) Lock(key uint64) {
	shard := sm.ShardFor(key)
	sm.shards[shard].Lock()
	sm.totalLocks.Add(1)
}

// Unlock releases the write lock for the shard associated with the given key.
func (sm *ShardedRWMutex) Unlock(key uint64) {
	shard := sm.ShardFor(key)
	sm.shards[shard].Unlock()
}

// RLock acquires a read lock for the shard associated with the given key.
func (sm *ShardedRWMutex) RLock(key uint64) {
	shard := sm.ShardFor(key)
	sm.shards[shard].RLock()
	sm.totalRLocks.Add(1)
}

// RUnlock releases the read lock for the shard associated with the given key.
func (sm *ShardedRWMutex) RUnlock(key uint64) {
	shard := sm.ShardFor(key)
	sm.shards[shard].RUnlock()
}

// LockShard acquires a write lock on a specific shard directly.
func (sm *ShardedRWMutex) LockShard(shard int) {
	if shard >= 0 && shard < sm.numShards {
		sm.shards[shard].Lock()
		sm.totalLocks.Add(1)
	}
}

// UnlockShard releases a write lock on a specific shard directly.
func (sm *ShardedRWMutex) UnlockShard(shard int) {
	if shard >= 0 && shard < sm.numShards {
		sm.shards[shard].Unlock()
	}
}

// RLockShard acquires a read lock on a specific shard directly.
func (sm *ShardedRWMutex) RLockShard(shard int) {
	if shard >= 0 && shard < sm.numShards {
		sm.shards[shard].RLock()
		sm.totalRLocks.Add(1)
	}
}

// RUnlockShard releases a read lock on a specific shard directly.
func (sm *ShardedRWMutex) RUnlockShard(shard int) {
	if shard >= 0 && shard < sm.numShards {
		sm.shards[shard].RUnlock()
	}
}

// LockAll acquires write locks on all shards (use sparingly).
func (sm *ShardedRWMutex) LockAll() {
	for i := range sm.shards {
		sm.shards[i].Lock()
	}
	sm.totalLocks.Add(int64(sm.numShards))
}

// UnlockAll releases write locks on all shards.
func (sm *ShardedRWMutex) UnlockAll() {
	for i := range sm.shards {
		sm.shards[i].Unlock()
	}
}

// RLockAll acquires read locks on all shards.
func (sm *ShardedRWMutex) RLockAll() {
	for i := range sm.shards {
		sm.shards[i].RLock()
	}
	sm.totalRLocks.Add(int64(sm.numShards))
}

// RUnlockAll releases read locks on all shards.
func (sm *ShardedRWMutex) RUnlockAll() {
	for i := range sm.shards {
		sm.shards[i].RUnlock()
	}
}

// Stats returns current statistics.
func (sm *ShardedRWMutex) Stats() ShardedRWMutexStats {
	return ShardedRWMutexStats{
		TotalLocks:  sm.totalLocks.Load(),
		TotalRLocks: sm.totalRLocks.Load(),
	}
}
