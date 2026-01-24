package concurrency

import (
	"runtime"
	"sync"
)

type ConcurrentPool[T any] struct {
	pools []*sync.Pool
	mask  uint32
}

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

func (cp *ConcurrentPool[T]) Get() T {
	poolID := runtime.GOMAXPROCS(0) % len(cp.pools)
	return cp.pools[poolID].Get().(T)
}

func (cp *ConcurrentPool[T]) Put(item T) {
	poolID := runtime.GOMAXPROCS(0) % len(cp.pools)
	cp.pools[poolID].Put(item)
}

func (cp *ConcurrentPool[T]) Stats() PoolStats {
	stats := PoolStats{NumPools: len(cp.pools)}

	for _, pool := range cp.pools {
		stats.TotalObjects++
	}

	return stats
}

type PoolStats struct {
	NumPools     int
	TotalObjects int
}
