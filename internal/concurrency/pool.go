package concurrency

import (
	"runtime"
	"sync"
)

// ConcurrentPool represents a concurrent pool value.
type ConcurrentPool[T any] struct {
	pools []*sync.Pool
	mask  uint32
}

// NewConcurrentPool constructs a new ConcurrentPool.
func NewConcurrentPool[T any](numPools int) *ConcurrentPool[T] {
	if numPools < 1 {
		numPools = 1
	}

	pools := make([]*sync.Pool, numPools)
	for i := 0; i < numPools; i++ {
		pools[i] = &sync.Pool{New: func() any { return new(T) }}
	}

	return &ConcurrentPool[T]{
		pools: pools,
		mask:  uint32(numPools) - 1,
	}
}

// Get returns the .
func (cp *ConcurrentPool[T]) Get() T {
	poolID := runtime.GOMAXPROCS(0) % len(cp.pools)
	return cp.pools[poolID].Get().(T)
}

// Put runs the put operation.
func (cp *ConcurrentPool[T]) Put(item T) {
	poolID := runtime.GOMAXPROCS(0) % len(cp.pools)
	cp.pools[poolID].Put(item)
}

// Stats runs the stats operation.
func (cp *ConcurrentPool[T]) Stats() PoolStats {
	stats := PoolStats{NumPools: len(cp.pools)}

	return stats
}

// PoolStats represents a pool stats value.
type PoolStats struct {
	NumPools     int
	TotalObjects int
}
