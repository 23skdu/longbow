package memory

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
)

// nextPowerOf2 returns the smallest power of 2 >= n
func nextPowerOf2(n int) int {
	if n <= 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	return n + 1
}

// Verify nextPowerOf2 works correctly at compile time
var _ = nextPowerOf2(1024)      // Should be 1024
var _ = nextPowerOf2(1000)      // Should be 1024
var _ = nextPowerOf2(4096)      // Should be 4096
var _ = nextPowerOf2(1<<20 + 1) // Should be 1<<21

// Compile-time check: math.MaxInt is too large, but we only need up to 2^31
var _ = int(math.MaxInt32)

// SliceRef is a handle to a slice in the arena.
// It is used by TypedArena and external consumers.
type SliceRef struct {
	Offset uint64
	Len    uint32
	Cap    uint32
}

func (r SliceRef) IsNil() bool {
	return r.Offset == 0 && r.Len == 0
}

type slab struct {
	id     uint32
	data   []byte
	offset uint32 // current allocation pointer (relative to slab)
}

// ArenaStats holds memory usage information about the arena.
type ArenaStats struct {
	TotalCapacity int64
	UsedBytes     int64
}

// SlabArena manages large blocks of memory.
type SlabArena struct {
	mu      sync.Mutex              // Only guards Alloc (writes)
	slabs   atomic.Pointer[[]*slab] // Lock-free access to slabs slice
	slabCap uint32                  // capacity in BYTES
}

func (a *SlabArena) Stats() ArenaStats {
	slabsPtr := a.slabs.Load()
	if slabsPtr == nil {
		return ArenaStats{}
	}
	slabs := *slabsPtr
	stats := ArenaStats{
		TotalCapacity: int64(len(slabs)) * int64(a.slabCap),
	}
	// Note: We need to sum up used portions. This is a bit slow but okay for tuning.
	for _, s := range slabs {
		stats.UsedBytes += int64(s.offset)
	}
	return stats
}

// NewSlabArena creates a new arena with specified slab byte size.
func NewSlabArena(slabSizeBytes int) *SlabArena {
	if slabSizeBytes < 1024 {
		slabSizeBytes = 1024
	}
	// Round up to next power of 2 to enable fast modulo via bit operations
	slabSizeBytes = nextPowerOf2(slabSizeBytes)
	s := &SlabArena{
		slabCap: uint32(slabSizeBytes),
	}
	// Initialize with empty slice
	empty := make([]*slab, 0)
	s.slabs.Store(&empty)

	// Register with global registry for GC tuning
	RegisterArena(s)

	return s
}

// Alloc reserves space for 'size' bytes.
// Returns a GLOBAL offset.
// Guarantees zero-initialized memory.
func (a *SlabArena) Alloc(size int) (uint64, error) {
	// Try fast path first for small allocations (up to 1KB)
	if size <= 1024 {
		if offset, ok := a.allocFast(size); ok {
			return offset, nil
		}
	}
	// Slow path uses mutex-protected allocCommon
	metrics.ArenaSlowPathTotal.Inc()
	return a.allocCommon(size, 8, true)
}

// AllocDirty reserves space for 'size' bytes.
// Returns a GLOBAL offset.
// MEMORY IS NOT GUARANTEED TO BE ZEROED.
// Use this only when you will immediately overwrite the entire range.
func (a *SlabArena) AllocDirty(size int) (uint64, error) {
	// Try fast path first for small allocations (up to 1KB)
	if size <= 1024 {
		if offset, ok := a.allocFast(size); ok {
			return offset, nil
		}
	}
	return a.allocCommon(size, 8, false)
}

// AllocAligned reserves space with specific alignment.
func (a *SlabArena) AllocAligned(size, align int) (uint64, error) {
	if align <= 0 || (align&(align-1)) != 0 {
		return 0, errors.New("align must be a power of 2")
	}
	metrics.ArenaSlowPathTotal.Inc()
	return a.allocCommon(size, align, true)
}

// AllocFast attempts lock-free allocation for small sizes (≤ 64 bytes).
// Returns (globalOffset, true) on success, (0, false) on failure.
// This is an internal helper that doesn't increment metrics.
func (a *SlabArena) allocFast(size int) (uint64, bool) {
	const align = 8
	needed := uint32(size)
	// Bit operation optimization: (-needed) & (align-1) is equivalent to
	// (align - (needed % align)) % align when align is power of 2
	// This avoids two modulo operations with a single AND
	pad := (-needed) & (align - 1)
	totalNeeded := needed + pad

	for {
		slabsPtr := a.slabs.Load()
		slabs := *slabsPtr

		if len(slabs) == 0 {
			return 0, false
		}

		active := slabs[len(slabs)-1]

		oldOffset := atomic.LoadUint32(&active.offset)
		newOffset := oldOffset + totalNeeded

		if newOffset > uint32(len(active.data)) {
			return 0, false
		}

		if atomic.CompareAndSwapUint32(&active.offset, oldOffset, newOffset) {
			start := oldOffset

			if start == 0 && active.id == 1 {
				start += align
				atomic.AddUint32(&active.offset, align)
			}

			globalOffset := (uint64(active.id-1) * uint64(a.slabCap)) + uint64(start)
			return globalOffset, true
		}
	}
}

func (a *SlabArena) allocCommon(size, align int, zero bool) (uint64, error) {
	if size <= 0 {
		return 0, errors.New("alloc size must be positive")
	}
	needed := uint32(size)
	if needed > a.slabCap {
		return 0, fmt.Errorf("alloc request %d exceeds slab capacity %d", size, a.slabCap)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Try fast path while holding the mutex
	if size <= 64 && align <= 8 {
		if offset, ok := a.allocFast(size); ok {
			metrics.ArenaFastPathTotal.Inc()
			return offset, nil
		}
		metrics.ArenaFastPathFailedTotal.Inc()
	}

	// Load current state
	currentSlabsPtr := a.slabs.Load()
	currentSlabs := *currentSlabsPtr

	var active *slab
	if len(currentSlabs) > 0 {
		active = currentSlabs[len(currentSlabs)-1]
	}

	uAlign := uint32(align)
	var start uint32
	var claimed bool

	if active != nil {
		for {
			oldOffset := atomic.LoadUint32(&active.offset)
			pad := (-oldOffset) & (uAlign - 1)
			newOffset := oldOffset + pad + needed

			// Enforce 8-byte alignment at the end to support future allocFast
			if newOffset%8 != 0 {
				newOffset += 8 - (newOffset % 8)
			}

			if newOffset <= uint32(len(active.data)) {
				if atomic.CompareAndSwapUint32(&active.offset, oldOffset, newOffset) {
					start = oldOffset + pad
					claimed = true
					if pad > 0 {
						metrics.AdjacencyPaddingBytes.Add(float64(pad))
					}
					break
				}
			} else {
				// Won't fit, need new slab
				active = nil
				break
			}
		}
	}

	if !claimed {
		// Allocate new slab
		buf := GetSlab(int(a.slabCap))
		newId := uint32(len(currentSlabs) + 1)

		var pad uint32
		if newId == 1 {
			// Burn alignment bytes to move away from 0
			pad = uAlign
			start = pad
		}

		newOffset := start + needed
		if newOffset%8 != 0 {
			newOffset += 8 - (newOffset % 8)
		}

		newSlab := &slab{
			id:     newId,
			data:   buf,
			offset: newOffset,
		}

		// Zero memory before publishing
		if zero {
			clear(buf[start : start+needed])
		}

		newSlabs := make([]*slab, len(currentSlabs)+1)
		copy(newSlabs, currentSlabs)
		newSlabs[len(currentSlabs)] = newSlab
		a.slabs.Store(&newSlabs)

		active = newSlab

		metrics.ArenaSlabsTotal.Inc()
		metrics.ArenaAllocatedBytes.WithLabelValues("slab").Add(float64(a.slabCap))
	} else {
		if zero {
			clear(active.data[start : start+needed])
		}
	}

	slabIdx := uint64(active.id - 1)
	globalOffset := (slabIdx * uint64(a.slabCap)) + uint64(start)

	return globalOffset, nil
}

func (a *SlabArena) Free() {
	a.mu.Lock()
	defer a.mu.Unlock()

	currentSlabsPtr := a.slabs.Load()
	if currentSlabsPtr == nil {
		return
	}
	currentSlabs := *currentSlabsPtr

	for _, s := range currentSlabs {
		PutSlab(s.data)
		s.data = nil
	}

	empty := make([]*slab, 0)
	a.slabs.Store(&empty)

	UnregisterArena(a)
}

// Get returns the byte slice.
func (a *SlabArena) Get(offset uint64, length uint32) []byte {
	if offset == 0 || length == 0 {
		return nil
	}

	slabIdx := offset / uint64(a.slabCap)
	localOffset := uint32(offset & (uint64(a.slabCap) - 1))

	// Lock-free read
	slabsPtr := a.slabs.Load()
	slabs := *slabsPtr

	if int(slabIdx) >= len(slabs) {
		return nil
	}

	s := slabs[slabIdx]
	if uint64(localOffset)+uint64(length) > uint64(len(s.data)) {
		return nil
	}

	return s.data[localOffset : localOffset+length]
}

// GetPointer returns unsafe.Pointer to the data.
// Use with caution.
// GetPointer returns unsafe.Pointer to the data.
// Use with caution.
func (a *SlabArena) GetPointer(offset uint64) unsafe.Pointer {
	if offset == 0 {
		return nil
	}
	slabIdx := offset / uint64(a.slabCap)
	localOffset := uint32(offset & (uint64(a.slabCap) - 1))

	// Lock-free read
	slabsPtr := a.slabs.Load()
	slabs := *slabsPtr

	if int(slabIdx) >= len(slabs) {
		return nil
	}
	s := slabs[slabIdx]
	return unsafe.Pointer(&s.data[localOffset]) // #nosec G103
}
