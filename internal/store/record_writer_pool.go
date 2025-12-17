package store

import (
"bytes"
"fmt"
"sync"
"sync/atomic"
)

// =============================================================================
// IPC Buffer Pool - Reduces GC pressure on DoGet hot path
// =============================================================================

// RecordWriterPoolConfig configures the IPC buffer pool.
type RecordWriterPoolConfig struct {
// InitialBufferSize is the starting capacity for pooled buffers (default: 64KB)
InitialBufferSize int
// MaxBufferSize is the maximum buffer size to keep in pool (default: 4MB)
// Buffers larger than this are discarded to prevent memory bloat
MaxBufferSize int
// UseLZ4 enables LZ4 compression for IPC messages
UseLZ4 bool
}

// DefaultRecordWriterPoolConfig returns sensible defaults for high-throughput DoGet.
func DefaultRecordWriterPoolConfig() RecordWriterPoolConfig {
return RecordWriterPoolConfig{
InitialBufferSize: 64 * 1024,     // 64KB - covers most small batches
MaxBufferSize:     4 * 1024 * 1024, // 4MB - prevents memory bloat
UseLZ4:            true,            // Enable compression by default
}
}

// Validate checks configuration validity.
func (c RecordWriterPoolConfig) Validate() error {
// Zero initial size is allowed (uses default)
if c.InitialBufferSize > 0 && c.MaxBufferSize > 0 {
if c.InitialBufferSize > c.MaxBufferSize {
return fmt.Errorf("InitialBufferSize (%d) cannot exceed MaxBufferSize (%d)",
c.InitialBufferSize, c.MaxBufferSize)
}
}
return nil
}

// IPCBufferPoolStats tracks pool usage metrics.
type IPCBufferPoolStats struct {
Gets      int64 // Total Get() calls
Puts      int64 // Total Put() calls
Hits      int64 // Buffer reused from pool
Misses    int64 // New buffer allocated
Discarded int64 // Oversized buffers discarded
}

// IPCBufferPool manages reusable bytes.Buffer instances for IPC serialization.
// Uses sync.Pool internally for thread-safe, lock-free buffer management.
type IPCBufferPool struct {
pool   sync.Pool
config RecordWriterPoolConfig

// Stats - atomic for lock-free access
gets      int64
puts      int64
misses    int64
discarded int64
}

// NewIPCBufferPool creates a new buffer pool with the given configuration.
func NewIPCBufferPool(cfg RecordWriterPoolConfig) *IPCBufferPool {
initSize := cfg.InitialBufferSize
if initSize <= 0 {
initSize = 64 * 1024 // Default 64KB
}

p := &IPCBufferPool{
config: cfg,
}

p.pool.New = func() interface{} {
atomic.AddInt64(&p.misses, 1)
return bytes.NewBuffer(make([]byte, 0, initSize))
}

return p
}

// Get retrieves a buffer from the pool or allocates a new one.
// The returned buffer is reset and ready for use.
func (p *IPCBufferPool) Get() *bytes.Buffer {
atomic.AddInt64(&p.gets, 1)
buf := p.pool.Get().(*bytes.Buffer)
buf.Reset()
return buf
}

// Put returns a buffer to the pool for reuse.
// Oversized buffers (> MaxBufferSize) are discarded to prevent memory bloat.
func (p *IPCBufferPool) Put(buf *bytes.Buffer) {
if buf == nil {
return
}

atomic.AddInt64(&p.puts, 1)

// Discard oversized buffers to prevent memory bloat
if p.config.MaxBufferSize > 0 && buf.Cap() > p.config.MaxBufferSize {
atomic.AddInt64(&p.discarded, 1)
return // Let GC collect it
}

buf.Reset()
p.pool.Put(buf)
}

// Stats returns current pool statistics.
func (p *IPCBufferPool) Stats() IPCBufferPoolStats {
gets := atomic.LoadInt64(&p.gets)
misses := atomic.LoadInt64(&p.misses)

return IPCBufferPoolStats{
Gets:      gets,
Puts:      atomic.LoadInt64(&p.puts),
Hits:      gets - misses, // Hits = total gets minus new allocations
Misses:    misses,
Discarded: atomic.LoadInt64(&p.discarded),
}
}

// Reset clears all statistics.
func (p *IPCBufferPool) Reset() {
atomic.StoreInt64(&p.gets, 0)
atomic.StoreInt64(&p.puts, 0)
atomic.StoreInt64(&p.misses, 0)
atomic.StoreInt64(&p.discarded, 0)
}
