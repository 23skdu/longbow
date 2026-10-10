package store

import (
	"container/list"
	"github.com/23skdu/longbow/internal/metrics"
	"sync"
	"sync/atomic"
	"time"

	lbtypes "github.com/23skdu/longbow/internal/store/types"
)

type TemporalResultCache struct {
	mu        sync.Mutex
	items     map[string]*list.Element
	evict     *list.List
	max       int
	label     atomic.Value // string, for the Prometheus dataset label
	hits      atomic.Int64
	misses    atomic.Int64
	expiries  atomic.Int64 // dropped because their TTL had passed
	evictions atomic.Int64 // dropped because the LRU was full
}

// TemporalCacheStats is a point-in-time snapshot of cache counters.
type TemporalCacheStats struct {
	Hits      int64
	Misses    int64
	Expiries  int64
	Evictions int64
	Entries   int
}

type temporalCacheEntry struct {
	key     string
	results []lbtypes.SearchResult
	expiry  time.Time
}

// NewTemporalResultCache creates a new TemporalResultCache with the specified capacity.
func NewTemporalResultCache(size int) *TemporalResultCache {
	return &TemporalResultCache{
		items: make(map[string]*list.Element),
		evict: list.New(),
		max:   size,
	}
}

// Get retrieves search results from the cache if they exist and are not expired.
func (c *TemporalResultCache) Get(key string) ([]lbtypes.SearchResult, bool) {
	c.mu.Lock()

	element, ok := c.items[key]
	if !ok {
		c.misses.Add(1)
		entries := len(c.items)
		c.mu.Unlock()
		c.emit("misses", entries)
		return nil, false
	}

	entry := element.Value.(*temporalCacheEntry)
	if time.Now().After(entry.expiry) {
		c.evict.Remove(element)
		delete(c.items, key)
		c.expiries.Add(1)
		c.misses.Add(1)
		entries := len(c.items)
		c.mu.Unlock()
		c.emit("misses", entries)
		c.emit("expiries", entries)
		return nil, false
	}

	c.evict.MoveToFront(element)
	c.hits.Add(1)
	results := entry.results
	entries := len(c.items)
	c.mu.Unlock()
	c.emit("hits", entries)
	return results, true
}

// Set adds search results to the cache with the specified TTL.
func (c *TemporalResultCache) Set(key string, results []lbtypes.SearchResult, ttl time.Duration) {
	c.mu.Lock()

	if element, ok := c.items[key]; ok {
		c.evict.MoveToFront(element)
		entry := element.Value.(*temporalCacheEntry)
		entry.results = results
		entry.expiry = time.Now().Add(ttl)
		c.mu.Unlock()
		return
	}

	entry := &temporalCacheEntry{
		key:     key,
		results: results,
		expiry:  time.Now().Add(ttl),
	}
	element := c.evict.PushFront(entry)
	c.items[key] = element

	if c.evict.Len() > c.max {
		oldest := c.evict.Back()
		if oldest != nil {
			c.evict.Remove(oldest)
			delete(c.items, oldest.Value.(*temporalCacheEntry).key)
			c.evictions.Add(1)
			entries := len(c.items)
			c.mu.Unlock()
			c.emit("evictions", entries)
			return
		}
	}
	c.mu.Unlock()
}

// SetDatasetLabel names the dataset these counters belong to, so the exported
// metrics carry the same label as the rest of the store's series. It is called
// once when the dataset takes ownership of the index; counters observed before
// that report under "unknown" rather than being dropped.
func (c *TemporalResultCache) SetDatasetLabel(name string) {
	if name == "" {
		name = "unknown"
	}
	c.label.Store(name)
}

func (c *TemporalResultCache) datasetLabel() string {
	if v, ok := c.label.Load().(string); ok && v != "" {
		return v
	}
	return "unknown"
}

// Stats returns a snapshot of the counters. It is the only reader of them, which
// is the point of R11a: these were maintained and never observed.
func (c *TemporalResultCache) Stats() TemporalCacheStats {
	c.mu.Lock()
	entries := len(c.items)
	c.mu.Unlock()
	return TemporalCacheStats{
		Hits:      c.hits.Load(),
		Misses:    c.misses.Load(),
		Expiries:  c.expiries.Load(),
		Evictions: c.evictions.Load(),
		Entries:   entries,
	}
}

// emit increments the Prometheus counter for one outcome and refreshes the
// resident-size gauge. Called outside the lock so a scrape cannot stall the
// search path; `entries` is captured by the caller while it still holds the lock.
func (c *TemporalResultCache) emit(kind string, entries int) {
	label := c.datasetLabel()
	metrics.TemporalCacheEntries.WithLabelValues(label).Set(float64(entries))
	switch kind {
	case "hits":
		metrics.TemporalCacheHitsTotal.WithLabelValues(label).Inc()
	case "misses":
		metrics.TemporalCacheMissesTotal.WithLabelValues(label).Inc()
	case "expiries":
		metrics.TemporalCacheExpiriesTotal.WithLabelValues(label).Inc()
	case "evictions":
		metrics.TemporalCacheEvictionsTotal.WithLabelValues(label).Inc()
	}
}

const nodesPerChunk = 1024

// TemporalEntry represents a vector ID and its norm at a specific point in time.
