package types

import (
	"fmt"
	"sync"
)

const numMapShards = 64

type mapShard[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]V
}

// MapRCU provides a high-performance concurrent sharded map with RCU-like semantics.
// Instead of cloning the entire map on every mutation (which previously generated
// >1.2GB of heap allocations during batch ingestion), updates are partitioned across
// 64 independent shards, mutating only the target shard's hash table in place.
type MapRCU[K comparable, V any] struct {
	shards [numMapShards]mapShard[K, V]
	hasher func(key K) uint64
}

func hashString(s string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

func hashUint64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func hashAny[K comparable](key K) uint64 {
	return hashString(fmt.Sprint(key))
}

// NewMapRCU creates a new sharded concurrent map.
func NewMapRCU[K comparable, V any]() *MapRCU[K, V] {
	m := &MapRCU[K, V]{}
	for i := 0; i < numMapShards; i++ {
		m.shards[i].data = make(map[K]V, 16)
	}

	var zeroK K
	switch any(zeroK).(type) {
	case string:
		m.hasher = func(key K) uint64 {
			return hashString(any(key).(string))
		}
	case uint64:
		m.hasher = func(key K) uint64 {
			return hashUint64(any(key).(uint64))
		}
	case uint32:
		m.hasher = func(key K) uint64 {
			return hashUint64(uint64(any(key).(uint32)))
		}
	case int64:
		m.hasher = func(key K) uint64 {
			return hashUint64(uint64(any(key).(int64)))
		}
	case int:
		m.hasher = func(key K) uint64 {
			return hashUint64(uint64(any(key).(int)))
		}
	case int32:
		m.hasher = func(key K) uint64 {
			return hashUint64(uint64(any(key).(int32)))
		}
	default:
		m.hasher = func(key K) uint64 {
			return hashAny(key)
		}
	}

	return m
}

func (m *MapRCU[K, V]) shardIndex(key K) uint64 {
	return m.hasher(key) & (numMapShards - 1)
}

// Get retrieves a value from the map.
func (m *MapRCU[K, V]) Get(key K) (V, bool) {
	s := &m.shards[m.shardIndex(key)]
	s.mu.RLock()
	val, ok := s.data[key]
	s.mu.RUnlock()
	return val, ok
}

// Store updates the map for a given key.
func (m *MapRCU[K, V]) Store(key K, val V) {
	s := &m.shards[m.shardIndex(key)]
	s.mu.Lock()
	s.data[key] = val
	s.mu.Unlock()
}

// Delete removes a key from the map.
func (m *MapRCU[K, V]) Delete(key K) {
	s := &m.shards[m.shardIndex(key)]
	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()
}

type keyVal[K comparable, V any] struct {
	k K
	v V
}

// BulkStore updates multiple keys across shards without copying the entire map.
func (m *MapRCU[K, V]) BulkStore(updates map[K]V) {
	if len(updates) == 0 {
		return
	}

	if len(updates) <= 8 {
		for k, v := range updates {
			m.Store(k, v)
		}
		return
	}

	var buckets [numMapShards][]keyVal[K, V]
	for k, v := range updates {
		idx := m.shardIndex(k)
		buckets[idx] = append(buckets[idx], keyVal[K, V]{k: k, v: v})
	}

	for i := 0; i < numMapShards; i++ {
		items := buckets[i]
		if len(items) == 0 {
			continue
		}
		s := &m.shards[i]
		s.mu.Lock()
		for _, item := range items {
			s.data[item.k] = item.v
		}
		s.mu.Unlock()
	}
}

// BulkDelete removes multiple keys across shards.
func (m *MapRCU[K, V]) BulkDelete(keys []K) {
	if len(keys) == 0 {
		return
	}
	if len(keys) <= 8 {
		for _, k := range keys {
			m.Delete(k)
		}
		return
	}

	var buckets [numMapShards][]K
	for _, k := range keys {
		idx := m.shardIndex(k)
		buckets[idx] = append(buckets[idx], k)
	}

	for i := 0; i < numMapShards; i++ {
		items := buckets[i]
		if len(items) == 0 {
			continue
		}
		s := &m.shards[i]
		s.mu.Lock()
		for _, k := range items {
			delete(s.data, k)
		}
		s.mu.Unlock()
	}
}

// Range iterates over all entries across all shards.
func (m *MapRCU[K, V]) Range(f func(key K, val V) bool) {
	for i := 0; i < numMapShards; i++ {
		s := &m.shards[i]
		s.mu.RLock()
		for k, v := range s.data {
			if !f(k, v) {
				s.mu.RUnlock()
				return
			}
		}
		s.mu.RUnlock()
	}
}

// Load returns a snapshot of the current map for backward compatibility.
func (m *MapRCU[K, V]) Load() map[K]V {
	totalLen := 0
	for i := 0; i < numMapShards; i++ {
		s := &m.shards[i]
		s.mu.RLock()
		totalLen += len(s.data)
		s.mu.RUnlock()
	}

	snap := make(map[K]V, totalLen)
	m.Range(func(key K, val V) bool {
		snap[key] = val
		return true
	})
	return snap
}

// Len returns the total number of elements across all shards.
func (m *MapRCU[K, V]) Len() int {
	total := 0
	for i := 0; i < numMapShards; i++ {
		s := &m.shards[i]
		s.mu.RLock()
		total += len(s.data)
		s.mu.RUnlock()
	}
	return total
}
