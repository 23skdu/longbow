package store

import (
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
)

// RecordSizeCache caches the size of Arrow record batches to avoid repeated calculations
type RecordSizeCache struct {
	mu sync.RWMutex
	// Using uintptr to key by pointer address of the underlying object
	// This avoids holding references preventing GC, if we wanted weak refs, but simple map is ok for now.
	// However, using the interface as key `map[arrow.RecordBatch]int64` is safest if they are comparable pointers.
	cache  map[arrow.RecordBatch]int64
	hits   atomic.Int64
	misses atomic.Int64
}

// Global instance
var globalSizeCache = NewRecordSizeCache()

// NewRecordSizeCache creates a new cache
func NewRecordSizeCache() *RecordSizeCache {
	return &RecordSizeCache{
		cache: make(map[arrow.RecordBatch]int64),
	}
}

// GetOrCompute returns the cached size or computes it
func (c *RecordSizeCache) GetOrCompute(rec arrow.RecordBatch) int64 {
	if rec == nil {
		return 0
	}

	c.mu.RLock()
	size, ok := c.cache[rec]
	c.mu.RUnlock()

	if ok {
		c.hits.Add(1)
		return size
	}

	c.misses.Add(1)
	size = calculateRecordSize(rec)

	c.mu.Lock()
	c.cache[rec] = size
	c.mu.Unlock()
	return size
}

// Hits returns hit count
func (c *RecordSizeCache) Hits() int64 {
	return c.hits.Load()
}

// Misses returns miss count
func (c *RecordSizeCache) Misses() int64 {
	return c.misses.Load()
}

// Invalidate removes a record from cache
func (c *RecordSizeCache) Invalidate(rec arrow.RecordBatch) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cache, rec)
}

// Clear empties the cache
func (c *RecordSizeCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[arrow.RecordBatch]int64)
}

// Len returns cache size
func (c *RecordSizeCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}

// calculateRecordSize computes exact size in bytes
// It iterates buffers to get allocated size.
func calculateRecordSize(rec arrow.RecordBatch) int64 {
	if rec == nil {
		return 0
	}
	var size int64
	for _, col := range rec.Columns() {
		// Helper to safely access buffers
		data := col.Data()
		if data == nil {
			continue
		}
		for _, buf := range data.Buffers() {
			if buf != nil {
				size += int64(buf.Len())
			}
		}
		// Also count children for nested types
		// This is a simplified calculation, usually sufficient for memory tracking
	}
	return size
}

// EstimateRecordSize is a heuristic or fast path.
// For now, we alias to calculateRecordSize as it's reasonably fast (O(cols)).
func EstimateRecordSize(rec arrow.RecordBatch) int64 {
	return calculateRecordSize(rec)
}

// ResetGlobalSizeCache resets the global cache
func ResetGlobalSizeCache() {
	globalSizeCache.Clear()
}

// CachedRecordSize uses global cache
func CachedRecordSize(rec arrow.RecordBatch) int64 {
	return globalSizeCache.GetOrCompute(rec)
}
