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

	// Dataset label for metrics
	dataset string
}

func NewQueryCache[T any](capacity int, ttl time.Duration, dataset string) *QueryCache[T] {
	return &QueryCache[T]{
		capacity: capacity,
		ttl:      ttl,
		items:    make(map[uint64]*list.Element),
		lru:      list.New(),
		dataset:  dataset,
	}
}

func (c *QueryCache[T]) Get(key uint64) (T, bool) {
	c.mu.RLock()
	elem, ok := c.items[key]
	c.mu.RUnlock()

	if !ok {
		metrics.QueryCacheMissesTotal.WithLabelValues(c.dataset).Inc()
		var zero T
		return zero, false
	}

	// Double check expiry
	item := elem.Value.(*CacheItem[T])
	if time.Now().After(item.ExpiresAt) {
		// Expired
		// We need to upgrade lock to remove it?
		// Or just let lazy cleanup/overwrite handle it?
		// Better to return miss and let caller overwrite.
		// Technically we should delete it but that requires lock upgrade.
		// For simplicity, treat as miss.
		// Actual cleanup happens on Put or explicit cleanup.
		metrics.QueryCacheMissesTotal.WithLabelValues(c.dataset).Inc()
		var zero T
		return zero, false
	}

	// Move to front (requires write lock)
	// To minimize contention, we could skip moving to front on every read?
	// Or simpler: Upgrade lock.
	c.mu.Lock()
	defer c.mu.Unlock()

	// Re-check existence after lock
	if elem, ok := c.items[key]; ok {
		c.lru.MoveToFront(elem)
		metrics.QueryCacheHitsTotal.WithLabelValues(c.dataset).Inc()
		return elem.Value.(*CacheItem[T]).Value, true
	}

	metrics.QueryCacheMissesTotal.WithLabelValues(c.dataset).Inc()
	var zero T
	return zero, false
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

// Clear purges the cache
func (c *QueryCache[T]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru.Init()
	c.items = make(map[uint64]*list.Element)
	metrics.QueryCacheSize.WithLabelValues(c.dataset).Set(0)
}
