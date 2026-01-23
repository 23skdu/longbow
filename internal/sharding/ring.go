package sharding

import (
	"container/list"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ConsistentHash implements a consistent hashing ring with hot key caching
type ConsistentHash struct {
	mu           sync.RWMutex
	nodes        map[uint64]string // hash -> nodeID
	sortedHashes []uint64          // sorted hash values
	vnodes       int               // virtual nodes per real node

	// Hot key cache for frequently accessed keys
	cacheMu       sync.RWMutex
	cache         map[string]*cacheEntry
	cacheList     *list.List
	cacheCapacity int
	cacheTTL      time.Duration

	// Metrics
	hits   int64
	misses int64
}

type cacheEntry struct {
	key       string
	node      string
	hash      uint64
	expiresAt time.Time
	element   *list.Element
}

// ConsistentHashMetrics holds performance metrics for the ring
type ConsistentHashMetrics struct {
	CacheHits   int64
	CacheMisses int64
	CacheSize   int
}

// NewConsistentHash creates a new ring with specified vnodes and hot key caching
func NewConsistentHash(vnodes int) *ConsistentHash {
	return NewConsistentHashWithCache(vnodes, 1000, 5*time.Minute)
}

// NewConsistentHashWithCache creates a new ring with custom cache configuration
func NewConsistentHashWithCache(vnodes int, cacheCapacity int, cacheTTL time.Duration) *ConsistentHash {
	return &ConsistentHash{
		nodes:         make(map[uint64]string),
		vnodes:        vnodes,
		cache:         make(map[string]*cacheEntry),
		cacheList:     list.New(),
		cacheCapacity: cacheCapacity,
		cacheTTL:      cacheTTL,
	}
}

// AddNode adds a new node to the ring
func (c *ConsistentHash) AddNode(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := 0; i < c.vnodes; i++ {
		h := c.hash(fmt.Sprintf("%s:%d", nodeID, i))
		c.nodes[h] = nodeID
		c.sortedHashes = append(c.sortedHashes, h)
	}
	sort.Slice(c.sortedHashes, func(i, j int) bool {
		return c.sortedHashes[i] < c.sortedHashes[j]
	})

	// Invalidate cache on topology change
	c.invalidateCache()
}

// RemoveNode removes a node from the ring
func (c *ConsistentHash) RemoveNode(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	newHashes := make([]uint64, 0, len(c.sortedHashes))
	for _, h := range c.sortedHashes {
		if c.nodes[h] == nodeID {
			delete(c.nodes, h)
		} else {
			newHashes = append(newHashes, h)
		}
	}
	c.sortedHashes = newHashes

	// Invalidate cache on topology change
	c.invalidateCache()
}

// GetNode returns the node responsible for the given key
// Uses hot key caching for improved performance on repeated lookups
func (c *ConsistentHash) GetNode(key string) string {
	// Try cache first (fast path)
	if node := c.getCached(key); node != "" {
		atomic.AddInt64(&c.hits, 1)
		return node
	}

	// Cache miss - compute and cache
	c.mu.RLock()
	if len(c.sortedHashes) == 0 {
		c.mu.RUnlock()
		atomic.AddInt64(&c.misses, 1)
		return ""
	}

	h := c.hash(key)
	idx := sort.Search(len(c.sortedHashes), func(i int) bool {
		return c.sortedHashes[i] >= h
	})

	if idx == len(c.sortedHashes) {
		idx = 0
	}

	node := c.nodes[c.sortedHashes[idx]]
	c.mu.RUnlock()

	atomic.AddInt64(&c.misses, 1)

	// Cache the result
	c.setCache(key, node, h)

	return node
}

// GetNodeUncached returns the node without using cache (for testing or cache-invalidated paths)
func (c *ConsistentHash) GetNodeUncached(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.sortedHashes) == 0 {
		return ""
	}

	h := c.hash(key)
	idx := sort.Search(len(c.sortedHashes), func(i int) bool {
		return c.sortedHashes[i] >= h
	})

	if idx == len(c.sortedHashes) {
		idx = 0
	}

	return c.nodes[c.sortedHashes[idx]]
}

// GetPreferenceList returns the top N distinct nodes for a key (Replication Strategy)
func (c *ConsistentHash) GetPreferenceList(key string, n int) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.sortedHashes) == 0 {
		return nil
	}

	h := c.hash(key)
	idx := sort.Search(len(c.sortedHashes), func(i int) bool {
		return c.sortedHashes[i] >= h
	})

	seen := make(map[string]bool)
	result := make([]string, 0, n)

	// Walk around the ring
	start := idx
	if start == len(c.sortedHashes) {
		start = 0
	}

	for i := 0; i < len(c.sortedHashes); i++ {
		currIdx := (start + i) % len(c.sortedHashes)
		nodeID := c.nodes[c.sortedHashes[currIdx]]
		if !seen[nodeID] {
			result = append(result, nodeID)
			seen[nodeID] = true
		}
		if len(result) == n {
			break
		}
	}

	return result
}

// GetMetrics returns current cache metrics
func (c *ConsistentHash) GetMetrics() ConsistentHashMetrics {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()

	return ConsistentHashMetrics{
		CacheHits:   atomic.LoadInt64(&c.hits),
		CacheMisses: atomic.LoadInt64(&c.misses),
		CacheSize:   len(c.cache),
	}
}

// ClearCache purges the hot key cache
func (c *ConsistentHash) ClearCache() {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	c.cache = make(map[string]*cacheEntry)
	c.cacheList.Init()
}

// invalidateCache clears cache when topology changes
func (c *ConsistentHash) invalidateCache() {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	c.cache = make(map[string]*cacheEntry)
	c.cacheList.Init()
}

// getCached retrieves a cached node lookup
func (c *ConsistentHash) getCached(key string) string {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()

	entry, ok := c.cache[key]
	if !ok {
		return ""
	}

	if time.Now().After(entry.expiresAt) {
		// Expired - remove from cache
		c.cacheList.Remove(entry.element)
		delete(c.cache, key)
		return ""
	}

	// Move to front (MRU)
	c.cacheList.MoveToFront(entry.element)
	return entry.node
}

// setCache stores a node lookup in the cache
func (c *ConsistentHash) setCache(key, node string, hash uint64) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	// Check if already cached
	if entry, ok := c.cache[key]; ok {
		entry.node = node
		entry.hash = hash
		entry.expiresAt = time.Now().Add(c.cacheTTL)
		c.cacheList.MoveToFront(entry.element)
		return
	}

	// Evict oldest if at capacity
	if len(c.cache) >= c.cacheCapacity {
		elem := c.cacheList.Back()
		if elem != nil {
			oldEntry := elem.Value.(*cacheEntry)
			delete(c.cache, oldEntry.key)
			c.cacheList.Remove(elem)
		}
	}

	// Add new entry
	entry := &cacheEntry{
		key:       key,
		node:      node,
		hash:      hash,
		expiresAt: time.Now().Add(c.cacheTTL),
	}
	entry.element = c.cacheList.PushFront(entry)
	c.cache[key] = entry
}

// hash computes a hash value for the key
func (c *ConsistentHash) hash(key string) uint64 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(h[:8])
}

// NodeCount returns the number of nodes in the ring
func (c *ConsistentHash) NodeCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	uniqueNodes := make(map[string]bool)
	for _, node := range c.nodes {
		uniqueNodes[node] = true
	}
	return len(uniqueNodes)
}

// VirtualNodeCount returns the number of virtual nodes
func (c *ConsistentHash) VirtualNodeCount() int {
	return c.vnodes
}

// TotalHashCount returns total number of hash points
func (c *ConsistentHash) TotalHashCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.sortedHashes)
}
