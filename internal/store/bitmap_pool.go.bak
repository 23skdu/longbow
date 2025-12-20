package store

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
)

// =============================================================================
// BitmapPoolConfig
// =============================================================================

// BitmapPoolConfig configures the bitmap buffer pool
type BitmapPoolConfig struct {
	// SizeBuckets defines the pooled buffer sizes (in bits)
	SizeBuckets []int
	// MaxBufferSize is the largest buffer that will be pooled
	MaxBufferSize int
	// MaxPoolSize per bucket (0 = unlimited, managed by sync.Pool)
	MaxPoolSize int
	// MetricsEnabled enables Prometheus metrics
	MetricsEnabled bool
}

// DefaultBitmapPoolConfig returns production defaults
func DefaultBitmapPoolConfig() BitmapPoolConfig {
	return BitmapPoolConfig{
		SizeBuckets:    []int{1024, 4096, 16384, 65536, 262144, 1048576},
		MaxBufferSize:  1048576, // 1M bits = 128KB
		MaxPoolSize:    0,       // Let sync.Pool manage
		MetricsEnabled: true,
	}
}

// Validate checks configuration
func (c *BitmapPoolConfig) Validate() error {
	if len(c.SizeBuckets) == 0 {
		return fmt.Errorf("SizeBuckets cannot be empty")
	}
	if c.MaxBufferSize <= 0 {
		return fmt.Errorf("MaxBufferSize must be positive")
	}
	if c.MaxPoolSize < 0 {
		return fmt.Errorf("MaxPoolSize cannot be negative")
	}
	// Sort buckets
	sort.Ints(c.SizeBuckets)
	return nil
}

// =============================================================================
// BitmapPoolStats
// =============================================================================

// BitmapPoolStats contains pool statistics
type BitmapPoolStats struct {
	Gets     uint64 // Total get operations
	Puts     uint64 // Total put operations
	Hits     uint64 // Buffers returned from pool
	Misses   uint64 // New allocations (pool empty)
	Discards uint64 // Buffers too large to pool
}

// =============================================================================
// BitmapPool
// =============================================================================

// BitmapPool manages pooled bitmap buffers to reduce GC pressure
type BitmapPool struct {
	config   BitmapPoolConfig
	buckets  []*sync.Pool
	gets     uint64
	puts     uint64
	hits     uint64
	misses   uint64
	discards uint64
}

// NewBitmapPool creates a new bitmap pool
func NewBitmapPool(cfg BitmapPoolConfig) *BitmapPool {
	if err := cfg.Validate(); err != nil {
		cfg = DefaultBitmapPoolConfig()
	}

	bp := &BitmapPool{
		config:  cfg,
		buckets: make([]*sync.Pool, len(cfg.SizeBuckets)),
	}

	// Create sync.Pool for each bucket WITHOUT New func
	// This allows us to detect hits vs misses
	for i := range cfg.SizeBuckets {
		bp.buckets[i] = &sync.Pool{}
	}

	return bp
}

// Get retrieves a bitmap buffer of at least numBits capacity
func (bp *BitmapPool) Get(numBits int) *PooledBitmap {
	atomic.AddUint64(&bp.gets, 1)

	if bp.config.MetricsEnabled {
		metrics.BitmapPoolGetsTotal.Inc()
	}

	// Find smallest bucket that fits
	bucketIdx := -1
	for i, size := range bp.config.SizeBuckets {
		if size >= numBits {
			bucketIdx = i
			break
		}
	}

	var buf *PooledBitmap

	if bucketIdx >= 0 {
		// Try to get from pool
		if pooled := bp.buckets[bucketIdx].Get(); pooled != nil {
			// HIT - got buffer from pool
			buf = pooled.(*PooledBitmap)
			buf.pool = bp
			buf.bucketIdx = bucketIdx
			buf.Reset()
			atomic.AddUint64(&bp.hits, 1)
			if bp.config.MetricsEnabled {
				metrics.BitmapPoolHitsTotal.Inc()
			}
		} else {
			// MISS - pool empty, allocate new
			atomic.AddUint64(&bp.misses, 1)
			if bp.config.MetricsEnabled {
				metrics.BitmapPoolMissesTotal.Inc()
			}
			bucketSize := bp.config.SizeBuckets[bucketIdx]
			byteSize := (bucketSize + 7) / 8
			buf = &PooledBitmap{
				Data:      make([]byte, byteSize),
				Capacity:  bucketSize,
				pool:      bp,
				bucketIdx: bucketIdx,
			}
		}
	} else {
		// Oversized - allocate directly (won't be pooled)
		atomic.AddUint64(&bp.misses, 1)
		if bp.config.MetricsEnabled {
			metrics.BitmapPoolMissesTotal.Inc()
		}
		byteSize := (numBits + 7) / 8
		buf = &PooledBitmap{
			Data:      make([]byte, byteSize),
			Capacity:  numBits,
			pool:      bp,
			bucketIdx: -1, // Mark as not poolable
		}
	}

	return buf
}

// Put returns a bitmap buffer to the pool
func (bp *BitmapPool) Put(buf *PooledBitmap) {
	if buf == nil {
		return
	}

	atomic.AddUint64(&bp.puts, 1)

	if bp.config.MetricsEnabled {
		metrics.BitmapPoolPutsTotal.Inc()
	}

	// Don't pool oversized buffers
	if buf.bucketIdx < 0 || buf.Capacity > bp.config.MaxBufferSize {
		atomic.AddUint64(&bp.discards, 1)
		if bp.config.MetricsEnabled {
			metrics.BitmapPoolDiscardsTotal.Inc()
		}
		return
	}

	// Return to appropriate bucket
	buf.Reset()
	bp.buckets[buf.bucketIdx].Put(buf)
}

// Stats returns pool statistics
func (bp *BitmapPool) Stats() BitmapPoolStats {
	return BitmapPoolStats{
		Gets:     atomic.LoadUint64(&bp.gets),
		Puts:     atomic.LoadUint64(&bp.puts),
		Hits:     atomic.LoadUint64(&bp.hits),
		Misses:   atomic.LoadUint64(&bp.misses),
		Discards: atomic.LoadUint64(&bp.discards),
	}
}

// Close clears the pool
func (bp *BitmapPool) Close() {
	// sync.Pool doesn't have Close, just let GC reclaim
}

// =============================================================================
// PooledBitmap
// =============================================================================

// PooledBitmap is a pooled bitmap buffer
type PooledBitmap struct {
	Data      []byte
	Capacity  int // in bits
	pool      *BitmapPool
	bucketIdx int
}

// Reset clears the bitmap
func (pb *PooledBitmap) Reset() {
	for i := range pb.Data {
		pb.Data[i] = 0
	}
}

// SetBit sets a bit at the given index
func (pb *PooledBitmap) SetBit(index int) {
	if index < 0 || index >= pb.Capacity {
		return
	}
	byteIdx := index / 8
	bitIdx := uint(index % 8) //nolint:gosec // G115 - modulo 8 safe for uint shift
	pb.Data[byteIdx] |= 1 << bitIdx
}

// GetBit gets a bit at the given index
func (pb *PooledBitmap) GetBit(index int) bool {
	if index < 0 || index >= pb.Capacity {
		return false
	}
	byteIdx := index / 8
	bitIdx := uint(index % 8)
	return (pb.Data[byteIdx] & (1 << bitIdx)) != 0
}

// Release returns the bitmap to the pool
func (pb *PooledBitmap) Release() {
	if pb.pool != nil {
		pb.pool.Put(pb)
	}
}
