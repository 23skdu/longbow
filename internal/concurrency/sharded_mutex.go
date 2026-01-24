package concurrency

import (
	"sync"
)

type ShardedMutex[T any] struct {
	shards    []sync.RWMutex
	numShards int
}

func NewShardedMutex[T any](numShards int) *ShardedMutex[T] {
	if numShards < 1 {
		numShards = 16
	}

	return &ShardedMutex[T]{
		shards:    make([]sync.RWMutex, numShards),
		numShards: numShards,
	}
}

func (sm *ShardedMutex[T]) Lock(key T) {
	shardIndex := sm.hash(key) % sm.numShards
	sm.shards[shardIndex].Lock()
}

func (sm *ShardedMutex[T]) Unlock(key T) {
	shardIndex := sm.hash(key) % sm.numShards
	sm.shards[shardIndex].Unlock()
}

func (sm *ShardedMutex[T]) RLock(key T) {
	shardIndex := sm.hash(key) % sm.numShards
	sm.shards[shardIndex].RLock()
}

func (sm *ShardedMutex[T]) RUnlock(key T) {
	shardIndex := sm.hash(key) % sm.numShards
	sm.shards[shardIndex].RUnlock()
}

func (sm *ShardedMutex[T]) hash(key T) int {
	switch k := any(key).(type) {
	case int:
		return k
	case int32:
		return int(k)
	case int64:
		return int(k)
	case string:
		h := 0
		for _, c := range k {
			h = h*31 + int(c)
		}
		return h
	default:
		return 0
	}
}
