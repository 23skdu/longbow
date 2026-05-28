//go:build gpu && linux

package memory

import (
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

// PageState tracks whether a GPU page is resident or evicted.
type PageState int32

const (
	PageEvicted   PageState = 0
	PageReserved  PageState = 1 // CPU buffer allocated, not yet on GPU
	PageResident  PageState = 2 // On GPU, clean
	PageResidentDirty PageState = 3 // On GPU, modified since last load
)

// PageID uniquely identifies a GPU memory page.
type PageID uint64

// PageInfo tracks metadata for one GPU memory page.
type PageInfo struct {
	id         PageID
	gpuPtr     unsafe.Pointer
	cpuBuf     []byte
	state      atomic.Int32 // PageState
	size       int64
	dirty      atomic.Bool
	lruElement *list.Element
}

// GPUPager manages a fixed pool of GPU memory pages with LRU eviction.
// Pages are evicted to CPU pinned memory when GPU memory is exhausted.
// The pager is concurrency-safe: all public methods are goroutine-safe.
type GPUPager struct {
	pool        *GPUMemPool
	maxVRAM     int64
	usedVRAM    int64
	pageSize    int64 // bytes per page
	maxPages    int   // max pages that fit in VRAM

	pages    map[PageID]*PageInfo
	lruList  *list.List
	evictMu  sync.Mutex

	statsMu      sync.Mutex
	totalEvicts  int64
	totalRestore int64
	totalFails   int64

	closed atomic.Bool
}

// NewGPUPager creates a GPU page table that evicts via LRU when VRAM is exhausted.
// maxVRAM: total GPU memory budget in bytes (0 = no limit).
// pageSize: size of each page in bytes.
func NewGPUPager(pool *GPUMemPool, maxVRAM, pageSize int64) *GPUPager {
	maxPages := 0
	if pageSize > 0 && maxVRAM > 0 {
		maxPages = int(maxVRAM / pageSize)
		if maxPages < 1 {
			maxPages = 1
		}
	}

	return &GPUPager{
		pool:     pool,
		maxVRAM:  maxVRAM,
		pageSize: pageSize,
		maxPages: maxPages,
		pages:    make(map[PageID]*PageInfo),
		lruList:  list.New(),
	}
}

// Alloc allocates a new page in the GPU page table.
// The page starts in Reserved state (CPU buffer allocated, GPU memory may be
// allocated lazily when the page is first accessed for compute).
func (p *GPUPager) Alloc(id PageID) (*PageInfo, error) {
	if p.closed.Load() {
		return nil, fmt.Errorf("pager is closed")
	}

	p.evictMu.Lock()
	defer p.evictMu.Unlock()

	if _, exists := p.pages[id]; exists {
		return nil, fmt.Errorf("page %d already allocated", id)
	}

	// Allocate CPU pinned memory as the backing store
	cpuBuf := make([]byte, p.pageSize)

	pi := &PageInfo{
		id:     id,
		cpuBuf: cpuBuf,
		size:   p.pageSize,
	}
	pi.state.Store(int32(PageReserved))

	// Insert at front (most recently used)
	elem := p.lruList.PushFront(pi)
	pi.lruElement = elem
	p.pages[id] = pi

	return pi, nil
}

// Promote ensures a page is resident on the GPU. If VRAM is full, the
// least-recently-used page is evicted to CPU memory.
func (p *GPUPager) Promote(pi *PageInfo) error {
	if p.closed.Load() {
		return fmt.Errorf("pager is closed")
	}

	state := PageState(pi.state.Load())

	if state == PageResident || state == PageResidentDirty {
		// Already on GPU, just update LRU position
		p.evictMu.Lock()
		p.lruList.MoveToFront(pi.lruElement)
		p.evictMu.Unlock()
		return nil
	}

	// Need to bring to GPU: either fresh load or restore from CPU
	p.evictMu.Lock()
	defer p.evictMu.Unlock()

	// Double-check after acquiring lock
	state = PageState(pi.state.Load())
	if state == PageResident || state == PageResidentDirty {
		p.lruList.MoveToFront(pi.lruElement)
		return nil
	}

	// Move this page to front so it is not selected as an eviction victim
	p.lruList.MoveToFront(pi.lruElement)

	// Ensure we have room, evicting if necessary
	for p.maxPages > 0 && p.usedVRAM+p.pageSize > p.maxVRAM {
		if !p.evictOne() {
			p.statsMu.Lock()
			p.totalFails++
			p.statsMu.Unlock()
			return fmt.Errorf("VRAM exhausted and no evictable pages available")
		}
	}

	// Allocate GPU memory
	gpuPtr, err := p.pool.AllocateGPU(p.pageSize)
	if err != nil {
		// Try evicting one more aggressively
		p.evictOne()
		gpuPtr, err = p.pool.AllocateGPU(p.pageSize)
		if err != nil {
			p.statsMu.Lock()
			p.totalFails++
			p.statsMu.Unlock()
			return fmt.Errorf("failed to allocate GPU memory for page %d: %w", pi.id, err)
		}
	}

	pi.gpuPtr = gpuPtr
	p.usedVRAM += p.pageSize

	if state == PageReserved {
		// First-time upload: copy from CPU to GPU
		if err := p.pool.MemcpyHostToDevice(gpuPtr, unsafe.Pointer(&pi.cpuBuf[0]), p.pageSize); err != nil {
			p.pool.FreeGPU(gpuPtr)
			p.usedVRAM -= p.pageSize
			pi.gpuPtr = nil
			return fmt.Errorf("failed to copy page %d to GPU: %w", pi.id, err)
		}
	} else {
		// Restore from CPU backup
		if err := p.pool.MemcpyHostToDevice(gpuPtr, unsafe.Pointer(&pi.cpuBuf[0]), p.pageSize); err != nil {
			p.pool.FreeGPU(gpuPtr)
			p.usedVRAM -= p.pageSize
			pi.gpuPtr = nil
			return fmt.Errorf("failed to restore page %d to GPU: %w", pi.id, err)
		}
		p.statsMu.Lock()
		p.totalRestore++
		p.statsMu.Unlock()
	}

	pi.state.Store(int32(PageResident))
	pi.dirty.Store(false)
	p.lruList.MoveToFront(pi.lruElement)

	return nil
}

// Demote evicts a page from GPU to CPU memory.
func (p *GPUPager) Demote(pi *PageInfo) error {
	p.evictMu.Lock()
	defer p.evictMu.Unlock()

	state := PageState(pi.state.Load())
	if state != PageResident && state != PageResidentDirty {
		return nil // Already evicted
	}

	// Write back if dirty
	if pi.dirty.Load() || state == PageResidentDirty {
		if err := p.pool.MemcpyDeviceToHost(unsafe.Pointer(&pi.cpuBuf[0]), pi.gpuPtr, p.pageSize); err != nil {
			return fmt.Errorf("failed to write back page %d: %w", pi.id, err)
		}
	}

	if err := p.pool.FreeGPU(pi.gpuPtr); err != nil {
		return fmt.Errorf("failed to free GPU memory for page %d: %w", pi.id, err)
	}

	pi.gpuPtr = nil
	p.usedVRAM -= p.pageSize
	pi.state.Store(int32(PageEvicted))
	pi.dirty.Store(false)

	return nil
}

// MarkDirty marks a page as modified on the GPU so it will be written back on eviction.
func (p *GPUPager) MarkDirty(pi *PageInfo) {
	pi.dirty.Store(true)
	state := PageState(pi.state.Load())
	if state == PageResident {
		pi.state.Store(int32(PageResidentDirty))
	}
}

// Access marks a page as recently used without promoting it.
func (p *GPUPager) Access(pi *PageInfo) {
	p.evictMu.Lock()
	p.lruList.MoveToFront(pi.lruElement)
	p.evictMu.Unlock()
}

// Free releases a page from the page table entirely.
func (p *GPUPager) Free(id PageID) error {
	p.evictMu.Lock()
	defer p.evictMu.Unlock()

	pi, ok := p.pages[id]
	if !ok {
		return nil
	}

	// Evict from GPU if resident
	state := PageState(pi.state.Load())
	if state == PageResident || state == PageResidentDirty {
		if pi.gpuPtr != nil {
			p.pool.FreeGPU(pi.gpuPtr)
			p.usedVRAM -= p.pageSize
		}
	}

	p.lruList.Remove(pi.lruElement)
	delete(p.pages, id)
	return nil
}

// evictOne evicts the least-recently-used page. Caller must hold evictMu.
func (p *GPUPager) evictOne() bool {
	back := p.lruList.Back()
	if back == nil {
		return false
	}

	pi := back.Value.(*PageInfo)
	state := PageState(pi.state.Load())

	if state != PageResident && state != PageResidentDirty {
		// Already evicted, remove from LRU list
		p.lruList.Remove(back)
		return false
	}

	// Write back if dirty
	if pi.dirty.Load() || state == PageResidentDirty {
		if err := p.pool.MemcpyDeviceToHost(unsafe.Pointer(&pi.cpuBuf[0]), pi.gpuPtr, p.pageSize); err != nil {
			return false
		}
	}

	if err := p.pool.FreeGPU(pi.gpuPtr); err != nil {
		return false
	}

	pi.gpuPtr = nil
	p.usedVRAM -= p.pageSize
	pi.state.Store(int32(PageEvicted))
	pi.dirty.Store(false)

	p.lruList.MoveToFront(pi.lruElement)

	p.statsMu.Lock()
	p.totalEvicts++
	p.statsMu.Unlock()

	return true
}

// GetGPUAddr returns the GPU pointer for a resident page, or nil if evicted.
func (p *GPUPager) GetGPUAddr(pi *PageInfo) unsafe.Pointer {
	return pi.gpuPtr
}

// PageInfo returns the PageInfo for a given page ID, or nil if not found.
func (p *GPUPager) PageInfo(id PageID) *PageInfo {
	p.evictMu.Lock()
	defer p.evictMu.Unlock()
	return p.pages[id]
}

// GetCPUBuf returns the CPU backing buffer for a page.
func (p *GPUPager) GetCPUBuf(pi *PageInfo) []byte {
	return pi.cpuBuf
}

// Stats returns pager performance counters.
func (p *GPUPager) Stats() (totalEvicts, totalRestore, totalFails int64) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	return p.totalEvicts, p.totalRestore, p.totalFails
}

// VRAMUsage returns current GPU memory usage.
func (p *GPUPager) VRAMUsage() int64 {
	p.evictMu.Lock()
	defer p.evictMu.Unlock()
	return p.usedVRAM
}

// TotalPages returns total tracked pages.
func (p *GPUPager) TotalPages() int {
	p.evictMu.Lock()
	defer p.evictMu.Unlock()
	return len(p.pages)
}

// ResidentPages returns pages currently on GPU.
func (p *GPUPager) ResidentPages() int {
	p.evictMu.Lock()
	defer p.evictMu.Unlock()
	count := 0
	for _, pi := range p.pages {
		s := PageState(pi.state.Load())
		if s == PageResident || s == PageResidentDirty {
			count++
		}
	}
	return count
}

// Close releases all resources.
func (p *GPUPager) Close() error {
	p.closed.Store(true)

	p.evictMu.Lock()
	defer p.evictMu.Unlock()

	for _, pi := range p.pages {
		state := PageState(pi.state.Load())
		if state == PageResident || state == PageResidentDirty {
			if pi.gpuPtr != nil {
				p.pool.FreeGPU(pi.gpuPtr)
			}
		}
		pi.cpuBuf = nil
	}

	p.pages = nil
	p.lruList.Init()
	return nil
}
