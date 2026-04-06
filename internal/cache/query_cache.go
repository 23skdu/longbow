package cache

import (
	"container/list"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

type CacheItem[T any] struct {
	Key       uint64
	Value     T
	ExpiresAt time.Time
}

type QueryCache[T any] struct {
	mu       sync.RWMutex
	capacity int
	ttl      time.Duration
	items    map[uint64]*list.Element
	lru      *list.List

	dataset   string
	namespace string

	warmQueries   []uint64
	warmEnabled   bool
	freqTracker   map[uint64]int
	freqThreshold int
}

func NewQueryCache[T any](capacity int, ttl time.Duration, dataset string) *QueryCache[T] {
	return &QueryCache[T]{
		capacity:      capacity,
		ttl:           ttl,
		items:         make(map[uint64]*list.Element),
		lru:           list.New(),
		dataset:       dataset,
		freqTracker:   make(map[uint64]int),
		freqThreshold: 3,
	}
}

func NewQueryCacheWithNamespace[T any](capacity int, ttl time.Duration, dataset, namespace string) *QueryCache[T] {
	c := NewQueryCache[T](capacity, ttl, dataset)
	c.namespace = namespace
	return c
}

func (c *QueryCache[T]) Get(key uint64) (T, bool) {
	c.mu.RLock()
	elem, ok := c.items[key]
	if !ok {
		c.mu.RUnlock()
		metrics.QueryCacheMissesTotal.WithLabelValues(c.dataset).Inc()
		var zero T
		return zero, false
	}

	item := elem.Value.(*CacheItem[T])
	if time.Now().After(item.ExpiresAt) {
		c.mu.RUnlock()
		metrics.QueryCacheMissesTotal.WithLabelValues(c.dataset).Inc()
		var zero T
		return zero, false
	}

	// Optimization: Skip LRU update on reads to avoid write lock contention.
	// For read-heavy workloads, the LRU update overhead exceeds the benefit
	// of keeping frequently accessed items at the front. Eviction will still
	// work correctly as expired items are cleaned up lazily.
	metrics.QueryCacheHitsTotal.WithLabelValues(c.dataset).Inc()
	c.mu.RUnlock()
	return item.Value, true
}

func (c *QueryCache[T]) Put(key uint64, value T) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if exists
	if elem, ok := c.items[key]; ok {
		c.lru.MoveToFront(elem)
		item := elem.Value.(*CacheItem[T])
		item.Value = value
		item.ExpiresAt = time.Now().Add(c.ttl)
		return
	}

	// Add new
	item := &CacheItem[T]{
		Key:       key,
		Value:     value,
		ExpiresAt: time.Now().Add(c.ttl),
	}
	elem := c.lru.PushFront(item)
	c.items[key] = elem

	metrics.QueryCacheSize.WithLabelValues(c.dataset).Set(float64(c.lru.Len()))

	// Evict if needed
	if c.lru.Len() > c.capacity {
		c.evictOldest()
	}
}

func (c *QueryCache[T]) evictOldest() {
	elem := c.lru.Back()
	if elem != nil {
		c.lru.Remove(elem)
		item := elem.Value.(*CacheItem[T])
		delete(c.items, item.Key)
		metrics.QueryCacheEvictionsTotal.WithLabelValues(c.dataset).Inc()
		metrics.QueryCacheSize.WithLabelValues(c.dataset).Set(float64(c.lru.Len()))
	}
}

// InvalidateDataset clears cache entries for a specific dataset.
func (c *QueryCache[T]) InvalidateDataset(dataset string) {
	if c.dataset != dataset {
		return
	}
	c.Clear()
}

// Clear purges the cache
func (c *QueryCache[T]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru.Init()
	c.items = make(map[uint64]*list.Element)
	metrics.QueryCacheSize.WithLabelValues(c.dataset).Set(0)
}

// CacheStats returns current cache statistics
func (c *QueryCache[T]) CacheStats() (size int, hits, misses, evictions int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	size = c.lru.Len()
	return
}

type CacheStats struct {
	Size      int
	Namespace string
	Dataset   string
}

func (c *QueryCache[T]) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return CacheStats{
		Size:      c.lru.Len(),
		Namespace: c.namespace,
		Dataset:   c.dataset,
	}
}

func (c *QueryCache[T]) RecordAccess(key uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.freqTracker[key]++
	return c.freqTracker[key] >= c.freqThreshold
}

func (c *QueryCache[T]) GetFrequentQueries() []uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result []uint64
	for k, v := range c.freqTracker {
		if v >= c.freqThreshold {
			result = append(result, k)
		}
	}
	return result
}
