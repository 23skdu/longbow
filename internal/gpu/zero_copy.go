//go:build !windows

package gpu

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

// ZeroCopyBuffer represents a zero copy buffer value.
type ZeroCopyBuffer struct {
	ptr    unsafe.Pointer
	size   int
	pinned bool
	refCnt atomic.Int32
}

// NewZeroCopyBuffer constructs a new ZeroCopyBuffer.
func NewZeroCopyBuffer(size int) (*ZeroCopyBuffer, error) {
	buf := &ZeroCopyBuffer{
		size:   size,
		refCnt: atomic.Int32{},
	}

	buf.refCnt.Store(1)

	runtime.SetFinalizer(buf, func(b *ZeroCopyBuffer) {
		b.Free()
	})

	return buf, nil
}

// NewPinnedBuffer constructs a new PinnedBuffer.
func NewPinnedBuffer(size int) (*ZeroCopyBuffer, error) {
	buf := &ZeroCopyBuffer{
		size:   size,
		pinned: true,
		refCnt: atomic.Int32{},
	}

	buf.refCnt.Store(1)

	ptr, err := unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, populatedMmapFlags)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate pinned memory: %w", err)
	}

	buf.ptr = unsafe.Pointer(&ptr[0]) // #nosec G103

	runtime.SetFinalizer(buf, func(b *ZeroCopyBuffer) {
		b.Free()
	})

	return buf, nil
}

// Ptr runs the ptr operation.
func (b *ZeroCopyBuffer) Ptr() unsafe.Pointer {
	return b.ptr
}

// Size runs the size operation.
func (b *ZeroCopyBuffer) Size() int {
	return b.size
}

// Slice runs the slice operation.
func (b *ZeroCopyBuffer) Slice() []byte {
	if b.ptr == nil {
		return nil
	}
	return unsafe.Slice((*byte)(b.ptr), b.size) // #nosec G103
}

// AddRef runs the add ref operation.
func (b *ZeroCopyBuffer) AddRef() {
	b.refCnt.Add(1)
}

// Release runs the release operation.
func (b *ZeroCopyBuffer) Release() int {
	newCnt := b.refCnt.Add(-1)
	if newCnt <= 0 {
		b.Free()
	}
	return int(newCnt)
}

// Free runs the free operation.
func (b *ZeroCopyBuffer) Free() {
	if b.ptr == nil {
		return
	}

	if b.pinned {
		_ = unix.Munmap(b.Slice()) // nosec G104
	}

	b.ptr = nil
}

// ZeroCopyPool represents a zero copy pool value.
type ZeroCopyPool struct {
	mu      sync.Mutex
	sizes   []int
	pools   map[int]chan *ZeroCopyBuffer
	maxSize int
}

// NewZeroCopyPool constructs a new ZeroCopyPool.
func NewZeroCopyPool(sizes []int, maxPerSize int) *ZeroCopyPool {
	pools := make(map[int]chan *ZeroCopyBuffer)
	for _, size := range sizes {
		pools[size] = make(chan *ZeroCopyBuffer, maxPerSize)
	}

	return &ZeroCopyPool{
		sizes:   sizes,
		pools:   pools,
		maxSize: maxPerSize,
	}
}

// Acquire runs the acquire operation.
func (p *ZeroCopyPool) Acquire(size int) (*ZeroCopyBuffer, error) {
	p.mu.Lock()
	pool, ok := p.pools[size]
	p.mu.Unlock()

	if !ok {
		return NewZeroCopyBuffer(size)
	}

	select {
	case buf := <-pool:
		buf.AddRef()
		return buf, nil
	default:
		return NewZeroCopyBuffer(size)
	}
}

// Release runs the release operation.
func (p *ZeroCopyPool) Release(buf *ZeroCopyBuffer) {
	if buf == nil {
		return
	}

	size := buf.Size()
	if size <= 0 {
		buf.Release()
		return
	}

	p.mu.Lock()
	pool, ok := p.pools[size]
	p.mu.Unlock()

	if !ok || buf.pinned {
		buf.Release()
		return
	}

	refs := buf.refCnt.Load()
	if refs <= 1 {
		select {
		case pool <- buf:
		default:
			buf.Release()
		}
	} else {
		buf.Release()
	}
}

// TransferRequest represents a transfer request value.
type TransferRequest struct {
	Src       *ZeroCopyBuffer
	Dst       *ZeroCopyBuffer
	Offset    int
	Size      int
	Completed chan error
}

// ZeroCopyManager represents a zero copy manager value.
type ZeroCopyManager struct {
	pinnedPool *ZeroCopyPool
	transferCh chan TransferRequest
	workers    int
	wg         sync.WaitGroup
}

// NewZeroCopyManager constructs a new ZeroCopyManager.
func NewZeroCopyManager(workers int, bufferSizes []int) *ZeroCopyManager {
	zcm := &ZeroCopyManager{
		transferCh: make(chan TransferRequest, 1024),
		workers:    workers,
		pinnedPool: NewZeroCopyPool(bufferSizes, 100),
	}

	for i := 0; i < workers; i++ {
		zcm.wg.Add(1)
		go zcm.transferWorker()
	}

	return zcm
}

func (zcm *ZeroCopyManager) transferWorker() {
	defer zcm.wg.Done()

	for req := range zcm.transferCh {
		if req.Dst != nil && req.Src != nil {
			copy(req.Dst.Slice()[req.Offset:req.Offset+req.Size],
				req.Src.Slice()[req.Offset:req.Offset+req.Size])
		}
		req.Completed <- nil
	}
}

// TransferAsync runs the transfer async operation.
func (zcm *ZeroCopyManager) TransferAsync(req TransferRequest) {
	zcm.transferCh <- req
}

// TransferSync runs the transfer sync operation.
func (zcm *ZeroCopyManager) TransferSync(req TransferRequest) error {
	select {
	case zcm.transferCh <- req:
	case <-req.Completed:
	}
	return nil
}

// Close runs the close operation.
func (zcm *ZeroCopyManager) Close() {
	close(zcm.transferCh)
	zcm.wg.Wait()
}

// GetGPUFriendlySlice returns the gpu friendly slice.
func GetGPUFriendlySlice(data []float32) []byte {
	slice := unsafe.Slice((*byte)(unsafe.Pointer(&data[0])), len(data)*4) // #nosec G103
	return slice
}

// GetGPUFriendlyPtr returns the gpu friendly ptr.
func GetGPUFriendlyPtr(data []float32) unsafe.Pointer {
	if len(data) == 0 {
		return nil
	}
	return unsafe.Pointer(&data[0]) // #nosec G103
}
