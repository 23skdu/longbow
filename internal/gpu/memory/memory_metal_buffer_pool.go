//go:build gpu && darwin && arm64

package memory

import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cretu/craft/types/mtl"
)

type MTLBufferPool struct {
	device    *mtl.Device
	pools    map[int]*sync.Pool
	mu       sync.RWMutex
	stats    atomic.Pointer[bufferPoolStats]
	hitCount int64
	missCount int64
}

type bufferPoolStats struct {
	hitCount  int64
	missCount int64
	active   int64
}

var metalBufferPools map[unsafe.Pointer]*MTLBufferPool
var metalBufferPoolsMu sync.RWMutex

const (
	MetalMinSize    = 1024
	MetalMaxSize    = 64 * 1024 * 1024 // 64MB
	MetalSizeSteps = 16
)

func GetMTLBufferPool(dev *mtl.Device) *MTLBufferPool {
	key := unsafe.Pointer(dev)
	
	metalBufferPoolsMu.RLock()
	pool, exists := metalBufferPools[key]
	metalBufferPoolsMu.RUnlock()

	if exists {
		return pool
	}

	metalBufferPoolsMu.Lock()
	defer metalBufferPoolsMu.Unlock()

	pool = &MTLBufferPool{
		device: dev,
		pools:  make(map[int]*sync.Pool),
	}

	for size := MetalMinSize; size <= MetalMaxSize; size *= 2 {
		stepSize := size
		pool.pools[stepSize] = &sync.Pool{
			New: func() interface{} {
				buf := dev.NewBufferWithLength(stepSize, mtl.ResourceStorageModeShared)
				return buf
			},
		}
	}

	metalBufferPools[key] = pool
	return pool
}

func (p *MTLBufferPool) Get(size int) interface{} {
	if size < MetalMinSize {
		size = MetalMinSize
	}
	aligned := nextPowerOfTwo(size)
	if aligned > MetalMaxSize {
		aligned = MetalMaxSize
	}

	p.mu.RLock()
	pool, exists := p.pools[aligned]
	p.mu.RUnlock()

	if pool == nil || !exists {
		atomic.AddInt64(&p.missCount, 1)
		return p.device.NewBufferWithLength(size, mtl.ResourceStorageModeShared)
	}

	buf := pool.Get()
	if buf == nil {
		atomic.AddInt64(&p.missCount, 1)
		return p.device.NewBufferWithLength(size, mtl.ResourceStorageModeShared)
	}

	atomic.AddInt64(&p.hitCount, 1)
	return buf
}

func (p *MTLBufferPool) Put(buf interface{}) {
	mtlBuf, ok := buf.(mtl.Buffer)
	if !ok {
		return
	}

	size := mtlBuf.Length()
	if size < MetalMinSize {
		return
	}

	aligned := nextPowerOfTwo(size)
	if aligned > MetalMaxSize {
		return
	}

	p.mu.RLock()
	pool, exists := p.pools[aligned]
	p.mu.RUnlock()

	if pool != nil && exists {
		pool.Put(mtlBuf)
	}
}

func (p *MTLBufferPool) Stats() (hitCount, missCount int64) {
	return atomic.LoadInt64(&p.hitCount), atomic.LoadInt64(&p.missCount)
}

func nextPowerOfTwo(n int) int {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	return n + 1
}

type PooledBuffer struct {
	buffer mtl.Buffer
	pool   *MTLBufferPool
	size   int
}

func NewPooledBuffer(dev *mtl.Device, size int, pool *MTLBufferPool) (*PooledBuffer, error) {
	if size <= 0 {
		return nil, errors.New("invalid size")
	}

	buf := pool.Get(size)
	mtlBuf, ok := buf.(mtl.Buffer)
	if !ok {
		return nil, errors.New("failed to get buffer from pool")
	}

	return &PooledBuffer{
		buffer: mtlBuf,
		pool:   pool,
		size:  size,
	}, nil
}

func (pb *PooledBuffer) Buffer() mtl.Buffer {
	return pb.buffer
}

func (pb *PooledBuffer) Release() {
	if pb.pool != nil && pb.buffer != nil {
		pb.pool.Put(pb.buffer)
	}
	pb.buffer = nil
	pb.pool = nil
}