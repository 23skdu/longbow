package store

import (
"sync"
"sync/atomic"
"unsafe"

"github.com/apache/arrow-go/v18/arrow"
	"github.com/23skdu/longbow/internal/metrics"
)

// RecordSizeCache caches computed sizes for Arrow record batches.
// Uses pointer identity for cache keys to avoid expensive buffer iteration.
type RecordSizeCache struct {
mu     sync.RWMutex
cache  map[uintptr]int64
hits   atomic.Int64
misses atomic.Int64
}

// NewRecordSizeCache creates a new size cache.
func NewRecordSizeCache() *RecordSizeCache {
return &RecordSizeCache{
cache: make(map[uintptr]int64),
}
}

// recordKey returns a unique key for the record based on pointer identity.
func recordKey(rec arrow.RecordBatch) uintptr {
if rec == nil {
return 0
}
// Use the underlying data pointer as cache key
// Extract data pointer from interface for stable identity
	type iface struct {
		typ  uintptr
		data uintptr
	}
	return (*iface)(unsafe.Pointer(&rec)).data
}

// GetOrCompute returns cached size or computes and caches it.
func (c *RecordSizeCache) GetOrCompute(rec arrow.RecordBatch) int64 {
if rec == nil {
return 0
}

key := recordKey(rec)

// Fast path: check cache with read lock
c.mu.RLock()
if size, ok := c.cache[key]; ok {
c.mu.RUnlock()
c.hits.Add(1)
		metrics.RecordSizeCacheHitsTotal.Inc()
return size
}
c.mu.RUnlock()

// Slow path: compute size
c.misses.Add(1)
	metrics.RecordSizeCacheMissesTotal.Inc()
size := calculateRecordSize(rec)

// Store in cache
c.mu.Lock()
c.cache[key] = size
c.mu.Unlock()

return size
}

// Invalidate removes a specific record from the cache.
func (c *RecordSizeCache) Invalidate(rec arrow.RecordBatch) {
if rec == nil {
return
}
key := recordKey(rec)
c.mu.Lock()
delete(c.cache, key)
c.mu.Unlock()
}

// Clear removes all entries from the cache.
func (c *RecordSizeCache) Clear() {
c.mu.Lock()
c.cache = make(map[uintptr]int64)
c.mu.Unlock()
}

// Len returns the number of cached entries.
func (c *RecordSizeCache) Len() int {
c.mu.RLock()
defer c.mu.RUnlock()
return len(c.cache)
}

// Hits returns the number of cache hits.
func (c *RecordSizeCache) Hits() int64 {
return c.hits.Load()
}

// Misses returns the number of cache misses.
func (c *RecordSizeCache) Misses() int64 {
return c.misses.Load()
}

// Global size cache for convenience functions
var globalSizeCache = NewRecordSizeCache()

// ResetGlobalSizeCache resets the global cache (for testing).
func ResetGlobalSizeCache() {
globalSizeCache = NewRecordSizeCache()
}

// CachedRecordSize returns the size of a record using the global cache.
func CachedRecordSize(rec arrow.RecordBatch) int64 {
return globalSizeCache.GetOrCompute(rec)
}

// EstimateRecordSize provides a fast O(1) heuristic estimate based on
// NumRows * sum(fixed widths). More accurate for fixed-width columns.
func EstimateRecordSize(rec arrow.RecordBatch) int64 {
if rec == nil {
return 0
}

numRows := rec.NumRows()
if numRows == 0 {
return 0
}

schema := rec.Schema()
var totalWidth int64

for i := 0; i < schema.NumFields(); i++ {
field := schema.Field(i)
width := getFixedWidth(field.Type)
if width > 0 {
totalWidth += int64(width)
} else {
// Variable-width: estimate 32 bytes average
totalWidth += 32
}
}

// Add validity bitmap overhead (~1 bit per row per column)
bitmapOverhead := (numRows * int64(schema.NumFields()) + 7) / 8

return numRows*totalWidth + bitmapOverhead
}

// getFixedWidth returns the byte width for fixed-width types, 0 for variable.
func getFixedWidth(dt arrow.DataType) int {
switch dt.ID() {
case arrow.BOOL:
return 1 // stored as bits, but round up
case arrow.INT8, arrow.UINT8:
return 1
case arrow.INT16, arrow.UINT16, arrow.FLOAT16:
return 2
case arrow.INT32, arrow.UINT32, arrow.FLOAT32, arrow.DATE32, arrow.TIME32:
return 4
case arrow.INT64, arrow.UINT64, arrow.FLOAT64, arrow.DATE64, arrow.TIME64, arrow.TIMESTAMP, arrow.DURATION:
return 8
case arrow.DECIMAL128, arrow.INTERVAL_MONTH_DAY_NANO:
return 16
case arrow.DECIMAL256:
return 32
case arrow.FIXED_SIZE_BINARY:
if fsb, ok := dt.(*arrow.FixedSizeBinaryType); ok {
return fsb.ByteWidth
}
return 0
case arrow.FIXED_SIZE_LIST:
// Would need recursive calculation
return 0
default:
// Variable-width: STRING, BINARY, LIST, STRUCT, etc.
return 0
}
}

// Dataset size caching methods

// cachedSize stores the cached total size for the dataset.
// Accessed via atomic operations.
var datasetSizeCache sync.Map // map[*Dataset]int64

// TotalSize returns the total size of all records in the dataset.
// Uses caching for efficiency.
func (d *Dataset) TotalSize() int64 {
if d == nil {
return 0
}

// Check cache first
if cached, ok := datasetSizeCache.Load(d); ok {
return cached.(int64)
}

// Compute total size
d.mu.RLock()
var total int64
for _, rec := range d.Records {
total += CachedRecordSize(rec)
}
d.mu.RUnlock()

// Cache the result
datasetSizeCache.Store(d, total)

return total
}

// InvalidateSizeCache clears the cached size for this dataset.
func (d *Dataset) InvalidateSizeCache() {
if d != nil {
datasetSizeCache.Delete(d)
}
}

// AddRecordWithSize adds a record and updates the cached size.
func (d *Dataset) AddRecordWithSize(rec arrow.RecordBatch) {
if d == nil || rec == nil {
return
}

recSize := CachedRecordSize(rec)

d.mu.Lock()
d.Records = append(d.Records, rec)
d.mu.Unlock()

// Update cached size atomically
if cached, ok := datasetSizeCache.Load(d); ok {
datasetSizeCache.Store(d, cached.(int64)+recSize)
} else {
// Cache wasn't populated, compute fresh
_ = d.TotalSize()
}
}
