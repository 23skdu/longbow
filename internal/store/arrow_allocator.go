package store

import (
"errors"
"sync"
"sync/atomic"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/memory"
)

// =============================================================================
// ZeroCopyBufferConfig - Configuration for zero-copy buffer operations
// =============================================================================

// ZeroCopyBufferConfig configures zero-copy buffer behavior
type ZeroCopyBufferConfig struct {
Enabled            bool
MaxRetainedBuffers int
MaxBufferSize      int64
}

// DefaultZeroCopyBufferConfig returns sensible defaults
func DefaultZeroCopyBufferConfig() ZeroCopyBufferConfig {
return ZeroCopyBufferConfig{
Enabled:            true,
MaxRetainedBuffers: 1024,
MaxBufferSize:      64 * 1024 * 1024, // 64MB
}
}

// Validate checks configuration validity
func (c ZeroCopyBufferConfig) Validate() error {
if !c.Enabled {
return nil // Disabled config is always valid
}
if c.MaxRetainedBuffers <= 0 {
return errors.New("MaxRetainedBuffers must be > 0")
}
if c.MaxBufferSize <= 0 {
return errors.New("MaxBufferSize must be > 0")
}
return nil
}

// =============================================================================
// DirectBufferReader - Read Arrow buffers without copying
// =============================================================================

// DirectBufferReader provides zero-copy access to Arrow buffers
type DirectBufferReader struct {
alloc memory.Allocator
}

// NewDirectBufferReader creates a new direct buffer reader
func NewDirectBufferReader(alloc memory.Allocator) *DirectBufferReader {
return &DirectBufferReader{alloc: alloc}
}

// Allocator returns the underlying allocator
func (r *DirectBufferReader) Allocator() memory.Allocator {
return r.alloc
}

// GetBufferReference returns a reference to the buffer without copying
// The returned slice points to the same underlying array as the input
func (r *DirectBufferReader) GetBufferReference(data []byte) []byte {
// Zero-copy: return the same slice
return data
}

// CreateArrayDataFromBuffer creates arrow.ArrayData directly from a byte buffer
// without copying the data. The buffer must remain valid for the lifetime of
// the returned ArrayData.
func (r *DirectBufferReader) CreateArrayDataFromBuffer(
dt arrow.DataType,
length int,
dataBuf []byte,
nullBitmap []byte,
) arrow.ArrayData {
// Create buffers that reference the underlying byte slices
var buffers []*memory.Buffer

// Buffer 0: validity/null bitmap (can be nil for non-nullable)
if nullBitmap != nil {
buffers = append(buffers, memory.NewBufferBytes(nullBitmap))
} else {
buffers = append(buffers, nil)
}

// Buffer 1: data buffer
buffers = append(buffers, memory.NewBufferBytes(dataBuf))

// Create ArrayData with zero-copy buffers
return array.NewData(dt, length, buffers, nil, 0, 0)
}

// =============================================================================
// AllocatorAwareCache - Cache returning direct buffer references
// =============================================================================

// AllocatorAwareCache provides cached access to Arrow RecordBatches
// with zero-copy buffer retrieval
type AllocatorAwareCache struct {
alloc    memory.Allocator
maxSize  int
mu       sync.RWMutex
entries  map[string]*cacheEntry
}

type cacheEntry struct {
rec arrow.RecordBatch
}

// NewAllocatorAwareCache creates a new cache with zero-copy support
func NewAllocatorAwareCache(alloc memory.Allocator, maxSize int) *AllocatorAwareCache {
return &AllocatorAwareCache{
alloc:   alloc,
maxSize: maxSize,
entries: make(map[string]*cacheEntry),
}
}

// Put stores a RecordBatch in the cache with proper reference counting
func (c *AllocatorAwareCache) Put(key string, rec arrow.RecordBatch) {
c.mu.Lock()
defer c.mu.Unlock()

// Release old entry if exists
if old, ok := c.entries[key]; ok {
old.rec.Release()
}

// Retain the record for cache storage
rec.Retain()
c.entries[key] = &cacheEntry{rec: rec}
}

// Get retrieves a RecordBatch from cache
// Returns a retained reference - caller must Release()
func (c *AllocatorAwareCache) Get(key string) (arrow.RecordBatch, bool) {
c.mu.RLock()
defer c.mu.RUnlock()

entry, ok := c.entries[key]
if !ok {
return nil, false
}

// Retain for caller
entry.rec.Retain()
return entry.rec, true
}

// GetBufferDirect retrieves underlying buffer bytes without copying
// colIdx: column index, bufIdx: buffer index (0=validity, 1=data)
func (c *AllocatorAwareCache) GetBufferDirect(key string, colIdx, bufIdx int) []byte {
c.mu.RLock()
defer c.mu.RUnlock()

entry, ok := c.entries[key]
if !ok {
return nil
}

if colIdx < 0 || colIdx >= int(entry.rec.NumCols()) {
return nil
}

col := entry.rec.Column(colIdx)
data := col.Data()
bufs := data.Buffers()

if bufIdx < 0 || bufIdx >= len(bufs) || bufs[bufIdx] == nil {
return nil
}

// Zero-copy: return underlying bytes directly
return bufs[bufIdx].Bytes()
}

// =============================================================================
// FlightReaderBufferRetainer - Retain flight.Reader internal buffers
// =============================================================================

// BufferRetainerStats holds statistics about retained buffers
type BufferRetainerStats struct {
ActiveBuffers      int
TotalBytesRetained int64
TotalRetained      int64
TotalReleased      int64
}

// FlightReaderBufferRetainer manages buffer lifecycles for zero-copy operations
type FlightReaderBufferRetainer struct {
maxBuffers int
mu         sync.RWMutex
buffers    map[uint64][]byte
nextHandle atomic.Uint64

// Stats
totalRetained atomic.Int64
totalReleased atomic.Int64
bytesRetained atomic.Int64
}

// NewFlightReaderBufferRetainer creates a new buffer retainer
func NewFlightReaderBufferRetainer(maxBuffers int) *FlightReaderBufferRetainer {
r := &FlightReaderBufferRetainer{
maxBuffers: maxBuffers,
buffers:    make(map[uint64][]byte),
}
r.nextHandle.Store(1) // Start at 1 so 0 is invalid
return r
}

// MaxBuffers returns the maximum number of retained buffers
func (r *FlightReaderBufferRetainer) MaxBuffers() int {
return r.maxBuffers
}

// RetainBuffer retains a buffer and returns a handle for later retrieval
func (r *FlightReaderBufferRetainer) RetainBuffer(buf []byte) uint64 {
handle := r.nextHandle.Add(1) - 1

r.mu.Lock()
r.buffers[handle] = buf
r.mu.Unlock()

r.totalRetained.Add(1)
r.bytesRetained.Add(int64(len(buf)))

return handle
}

// GetBuffer retrieves a retained buffer by handle
func (r *FlightReaderBufferRetainer) GetBuffer(handle uint64) []byte {
r.mu.RLock()
defer r.mu.RUnlock()
return r.buffers[handle]
}

// ReleaseBuffer releases a retained buffer
func (r *FlightReaderBufferRetainer) ReleaseBuffer(handle uint64) bool {
r.mu.Lock()
buf, ok := r.buffers[handle]
if ok {
delete(r.buffers, handle)
r.bytesRetained.Add(-int64(len(buf)))
}
r.mu.Unlock()

if ok {
r.totalReleased.Add(1)
}
return ok
}

// Stats returns current buffer retainer statistics
func (r *FlightReaderBufferRetainer) Stats() BufferRetainerStats {
r.mu.RLock()
activeCount := len(r.buffers)
r.mu.RUnlock()

return BufferRetainerStats{
ActiveBuffers:      activeCount,
TotalBytesRetained: r.bytesRetained.Load(),
TotalRetained:      r.totalRetained.Load(),
TotalReleased:      r.totalReleased.Load(),
}
}
