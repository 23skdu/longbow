package store

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/client_golang/prometheus"
)

// QueryCache implements a thread-safe LRU cache for query results.
type QueryCache struct {
	mu          sync.Mutex
	capacity    int
	ttl         time.Duration
	items       map[string]*list.Element
	evictList   *list.List
	datasetName string

	// Cached metrics
	metricOps  *prometheus.CounterVec
	metricSize prometheus.Gauge
}

type cacheItem struct {
	key       string
	results   []SearchResult
	expiresAt time.Time
}

// NewQueryCache creates a new QueryCache.
func NewQueryCache(capacity int, ttl time.Duration) *QueryCache {
	return &QueryCache{
		capacity:    capacity,
		ttl:         ttl,
		items:       make(map[string]*list.Element),
		evictList:   list.New(),
		datasetName: "default",
		metricOps:   metrics.QueryCacheOpsTotal,
		metricSize:  metrics.QueryCacheSize.WithLabelValues("default"),
	}
}

// SetDatasetName updates the dataset name for metric labels.
func (c *QueryCache) SetDatasetName(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.datasetName = name
	c.metricSize = metrics.QueryCacheSize.WithLabelValues(name)
}

// Get retrieves results from the cache if they exist and haven't expired.
func (c *QueryCache) Get(query []float32, params string) ([]SearchResult, bool) {
	key := c.hashQuery(query, params)

	c.mu.Lock()
	defer c.mu.Unlock()

	if ent, ok := c.items[key]; ok {
		item := ent.Value.(*cacheItem)
		if time.Now().After(item.expiresAt) {
			c.removeElement(ent)
			c.metricOps.WithLabelValues(c.datasetName, "miss").Inc()
			return nil, false
		}
		c.evictList.MoveToFront(ent)
		c.metricOps.WithLabelValues(c.datasetName, "hit").Inc()
		return item.results, true
	}

	c.metricOps.WithLabelValues(c.datasetName, "miss").Inc()
	return nil, false
}

// Set adds or updates results in the cache.
func (c *QueryCache) Set(query []float32, params string, results []SearchResult) {
	key := c.hashQuery(query, params)

	c.mu.Lock()
	defer c.mu.Unlock()

	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		item := ent.Value.(*cacheItem)
		item.results = results
		item.expiresAt = time.Now().Add(c.ttl)
		return
	}

	item := &cacheItem{
		key:       key,
		results:   results,
		expiresAt: time.Now().Add(c.ttl),
	}
	ent := c.evictList.PushFront(item)
	c.items[key] = ent

	if c.evictList.Len() > c.capacity {
		c.removeOldest()
	}

	c.metricOps.WithLabelValues(c.datasetName, "set").Inc()
	c.metricSize.Set(float64(c.evictList.Len()))
}

func (c *QueryCache) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
		c.metricOps.WithLabelValues(c.datasetName, "evict").Inc()
	}
}

func (c *QueryCache) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	item := e.Value.(*cacheItem)
	delete(c.items, item.key)
	c.metricSize.Set(float64(c.evictList.Len()))
}

func (c *QueryCache) hashQuery(query []float32, params string) string {
	h := xxhash.New()
	// Convert float32 slice to byte slice for hashing (not ultra-safe but fast for this)
	// We'll just write the float values to the hash.
	for _, v := range query {
		// Manual float32 to bits to bytes?
		// Actually, xxhash.New().Write is better.
		// We'll use fmt.Fprintf or similar if unsure, but let's be more direct.
		// xxhash has sum64, but we need to feed it data.
		// For simplicity in this task, let's use a string representation or a binary write.
		fmt.Fprintf(h, "%f", v)
	}
	h.WriteString(params)
	return fmt.Sprintf("%x", h.Sum64())
}
