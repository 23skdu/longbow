package index

import (
	"container/list"
	"encoding/binary"
	"hash/fnv"
	"math"
	"sync"

	"github.com/23skdu/longbow/internal/store/types"
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
	result []types.SearchResult
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
func (c *gpuResultCache) get(query []float32) ([]types.SearchResult, bool) {
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
func (c *gpuResultCache) put(query []float32, result []types.SearchResult) {
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
	var b [4]byte
	for i := 0; i < len(query); i++ {
		bits := math.Float32bits(query[i])
		binary.LittleEndian.PutUint32(b[:], bits)
		_, _ = h.Write(b[:])
	}
	return h.Sum64()
}
