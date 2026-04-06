package cache

import (
	"sync"
	"time"
)

type NamespaceCacheManager struct {
	mu       sync.RWMutex
	caches   map[string]*QueryCache[[]float32]
	capacity int
	ttl      time.Duration
}

func NewNamespaceCacheManager(capacity int, ttl time.Duration) *NamespaceCacheManager {
	return &NamespaceCacheManager{
		capacity: capacity,
		ttl:      ttl,
		caches:   make(map[string]*QueryCache[[]float32]),
	}
}

func (m *NamespaceCacheManager) GetCache(namespace, dataset string) *QueryCache[[]float32] {
	m.mu.RLock()
	cache, ok := m.caches[namespace]
	m.mu.RUnlock()

	if ok {
		return cache
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if cache, ok = m.caches[namespace]; ok {
		return cache
	}

	cacheName := namespace + "/" + dataset
	cache = NewQueryCacheWithNamespace[[]float32](m.capacity, m.ttl, cacheName, namespace)
	m.caches[namespace] = cache

	return cache
}

func (m *NamespaceCacheManager) InvalidateNamespace(namespace string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cache, ok := m.caches[namespace]; ok {
		cache.Clear()
		delete(m.caches, namespace)
	}
}

func (m *NamespaceCacheManager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, cache := range m.caches {
		cache.Clear()
	}
	m.caches = make(map[string]*QueryCache[[]float32])
}

func (m *NamespaceCacheManager) Stats() map[string]CacheStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]CacheStats)
	for ns, cache := range m.caches {
		stats[ns] = cache.Stats()
	}
	return stats
}
