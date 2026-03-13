package store

import (
	"container/list"
	"hash/fnv"
	"sync"
)

// gpuResultCache is an LRU cache for GPU search results
type gpuResultCache struct {
	mu      sync.RWMutex
	maxSize int
	items   map[uint64]*list.Element
	order   *list.List
}

type gpuCacheEntry struct {
	key    uint64
	query  []float32
	result []SearchResult
}

// newGPUResultCache creates a new GPU result cache
func newGPUResultCache(maxSize int) *gpuResultCache {
	if maxSize <= 0 {
		return nil
	}
	return &gpuResultCache{
		maxSize: maxSize,
		items:   make(map[uint64]*list.Element),
		order:   list.New(),
	}
}

// get retrieves a cached result if available
func (c *gpuResultCache) get(query []float32) ([]SearchResult, bool) {
	if c == nil {
		return nil, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	key := hashQuery(query)
	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}

	// Move to front (most recently used)
	c.order.MoveToFront(elem)
	entry := elem.Value.(*gpuCacheEntry)

	return entry.result, true
}

// put adds a result to the cache
func (c *gpuResultCache) put(query []float32, result []SearchResult) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	key := hashQuery(query)

	// Check if already exists
	if elem, ok := c.items[key]; ok {
		// Update existing entry
		c.order.MoveToFront(elem)
		entry := elem.Value.(*gpuCacheEntry)
		entry.result = result
		return
	}

	// Add new entry
	entry := &gpuCacheEntry{
		key:    key,
		query:  query,
		result: result,
	}
	elem := c.order.PushFront(entry)
	c.items[key] = elem

	// Evict oldest if over capacity
	if c.order.Len() > c.maxSize {
		c.evictOldest()
	}
}

// evictOldest removes the least recently used entry
func (c *gpuResultCache) evictOldest() {
	elem := c.order.Back()
	if elem == nil {
		return
	}

	entry := elem.Value.(*gpuCacheEntry)
	delete(c.items, entry.key)
	c.order.Remove(elem)
}

// hashQuery creates a hash of a query vector for cache lookup
func hashQuery(query []float32) uint64 {
	h := fnv.New64a()
	for i := 0; i < len(query); i++ {
		// Convert float32 to bytes
		bits := uint32(query[i])
		_, _ = h.Write([]byte{byte(bits), byte(bits >> 8), byte(bits >> 16), byte(bits >> 24)}) // nosec G104
	}
	return h.Sum64()
}
