package memory

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/23skdu/longbow/internal/metrics"
	"io"
	"encoding/binary"
	"os"
	"runtime"
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
	n |= n >> 32
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

// IsNil returns true if the slice reference is empty.
func (r SliceRef) IsNil() bool {
	return r.Offset == 0 && r.Len == 0
}

type slab struct {
	id         uint32
	generation uint64 // Generation ID for isolation
	data       []byte
	offset     uint32 // current allocation pointer (relative to slab)
}

// ArenaStats holds memory usage information about the arena.
type ArenaStats struct {
	TotalCapacity int64
	UsedBytes     int64
}

// ArenaStatsRecord holds atomic counters for arena pressure tracking.
// This object is registered in the global registry to avoid leaking the arena itself.
type ArenaStatsRecord struct {
	TotalCapacity atomic.Int64
	UsedBytes     atomic.Int64
	Active        atomic.Bool
}

// SlabArena manages large blocks of memory.
type SlabArena struct {
	mu         sync.Mutex              // Only guards Alloc (writes)
	slabs      atomic.Pointer[[]*slab] // Lock-free access to slabs slice
	slabCap    uint32                  // capacity in BYTES
	generation atomic.Uint64           // Current generation for new slabs
	alloc      memory.Allocator        // Optional custom allocator (e.g. NUMA)
	stats      *ArenaStatsRecord
	refs       atomic.Int32            // Reference count for safe shared use
}

// Stats returns the current memory usage statistics for the arena.
func (a *SlabArena) Stats() ArenaStats {
	return ArenaStats{
		TotalCapacity: a.stats.TotalCapacity.Load(),
		UsedBytes:     a.stats.UsedBytes.Load(),
	}
}

// StatsRecord returns the underlying atomic stats record.
func (a *SlabArena) StatsRecord() *ArenaStatsRecord {
	return a.stats
}

// Retain increments the reference count.
func (a *SlabArena) Retain() {
	a.refs.Add(1)
}

// Release decrements the reference count and calls Free if it reaches zero.
func (a *SlabArena) Release() {
	if a.refs.Add(-1) == 0 {
		a.Free()
	}
}

// NewSlabArena creates a new arena with specified slab byte size.
func NewSlabArena(slabSizeBytes int) *SlabArena {
	return NewSlabArenaWithAllocator(slabSizeBytes, nil)
}

// NewSlabArenaWithAllocator creates a new arena using a specific allocator for slabs.
func NewSlabArenaWithAllocator(slabSizeBytes int, alloc memory.Allocator) *SlabArena {
	if slabSizeBytes < 1024 {
		slabSizeBytes = 1024
	}
	// Round up to next power of 2 to enable fast modulo via bit operations
	slabSizeBytes = nextPowerOf2(slabSizeBytes)
	s := &SlabArena{
		slabCap: uint32(slabSizeBytes), // #nosec G115
		alloc:   alloc,
		stats:   &ArenaStatsRecord{},
	}
	s.refs.Store(1) // Initial reference
	s.stats.Active.Store(true)

	// Initialize with empty slice
	empty := make([]*slab, 0)
	s.slabs.Store(&empty)

	// Set finalizer to automatically unregister when the arena is GC'd.
	// This ensures we don't leak stats records even if Release/Free isn't called.
	runtime.SetFinalizer(s, func(arena *SlabArena) {
		UnregisterArena(arena.stats)
	})

	return s
}

// BumpGeneration increments the arena's generation ID.
// All subsequent allocations will occur in a new slab belonging to this generation.
func (a *SlabArena) BumpGeneration() uint64 {
	return a.generation.Add(1)
}

// GetGeneration returns the current generation ID of the arena.
func (a *SlabArena) GetGeneration() uint64 {
	return a.generation.Load()
}

// Alloc reserves space for 'size' bytes.
// Returns a GLOBAL offset.
// Guarantees zero-initialized memory.
func (a *SlabArena) Alloc(size int) (uint64, error) {
	// Try fast path first for small allocations (up to 4KB)
	// This covers float32 vectors up to dim 1024 (4096 bytes)
	if size <= 4096 {
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
	// Try fast path first for small allocations (up to 4KB)
	if size <= 4096 {
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

// AllocFast attempts lock-free allocation for small sizes (≤ 4096 bytes).
// Returns (globalOffset, true) on success, (0, false) on failure.
// This is an internal helper that doesn't increment metrics.
func (a *SlabArena) allocFast(size int) (uint64, bool) {
	const align = 8
	needed := uint32(size) // #nosec G115
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
		if active.generation != a.generation.Load() {
			return 0, false // Force slow path for new generation
		}

		oldOffset := atomic.LoadUint32(&active.offset)
		var padStart uint32
		if oldOffset == 0 && active.id == 1 {
			padStart = align
		}
		newOffset := oldOffset + padStart + totalNeeded

		if newOffset > uint32(len(active.data)) { // #nosec G115
			return 0, false
		}

		if atomic.CompareAndSwapUint32(&active.offset, oldOffset, newOffset) {
			start := oldOffset + padStart
			globalOffset := (uint64(active.id-1) * uint64(a.slabCap)) + uint64(start)
			return globalOffset, true
		}
	}
}

func (a *SlabArena) allocCommon(size, align int, zero bool) (uint64, error) {
	if size <= 0 {
		return 0, errors.New("alloc size must be positive")
	}
	needed := uint32(size) // #nosec G115
	if needed > a.slabCap {
		// Dynamic slab capacity expansion: allocate a contiguous block spanning multiple virtual slots
		numSlots := (needed + a.slabCap - 1) / a.slabCap
		allocSize := int(numSlots * a.slabCap)

		var buf []byte
		if a.alloc != nil {
			buf = a.alloc.Allocate(allocSize)
		} else {
			buf = GetSlab(allocSize)
		}

		a.mu.Lock()
		currentSlabsPtr := a.slabs.Load()
		var currentSlabs []*slab
		if currentSlabsPtr != nil {
			currentSlabs = *currentSlabsPtr
		}

		newID := uint32(len(currentSlabs) + 1) // #nosec G115
		var start uint32
		if newID == 1 {
			start = uint32(align) // #nosec G115 -- intentional conversion
		}

		newOffset := start + needed
		if newOffset%8 != 0 {
			newOffset += 8 - (newOffset % 8)
		}

		primarySlab := &slab{
			id:         newID,
			generation: a.generation.Load(),
			data:       buf,
			offset:     newOffset,
		}

		if zero {
			clear(buf[start : start+needed])
		}

		newSlabs := make([]*slab, len(currentSlabs)+int(numSlots))
		copy(newSlabs, currentSlabs)
		newSlabs[len(currentSlabs)] = primarySlab

		for i := 1; i < int(numSlots); i++ {
			newSlabs[len(currentSlabs)+i] = &slab{
				id:         newID + uint32(i),
				generation: a.generation.Load(),
				data:       nil,
				offset:     0,
			}
		}

		a.slabs.Store(&newSlabs)

		a.stats.TotalCapacity.Add(int64(allocSize))
		a.stats.UsedBytes.Add(int64(newOffset))

		a.mu.Unlock()

		metrics.ArenaSlabsTotal.Add(float64(numSlots))
		metrics.ArenaAllocatedBytes.WithLabelValues("slab").Add(float64(allocSize))

		globalOffset := (uint64(newID-1) * uint64(a.slabCap)) + uint64(start)
		return globalOffset, nil
	}

	var isFastPath bool
	var fastPathFailed bool
	var newSlabAllocated bool

	a.mu.Lock()

	// Try fast path while holding the mutex
	if size <= 4096 && align <= 8 {
		if offset, ok := a.allocFast(size); ok {
			a.mu.Unlock()
			metrics.ArenaFastPathTotal.Inc()
			a.stats.UsedBytes.Add(int64(size))
			return offset, nil
		}
		fastPathFailed = true
	}

	// Load current state
	currentSlabsPtr := a.slabs.Load()
	currentSlabs := *currentSlabsPtr

	var active *slab
	if len(currentSlabs) > 0 {
		active = currentSlabs[len(currentSlabs)-1]
		// Generation isolation: if the active slab belongs to an older generation,
		// force a new slab allocation to ensure isolation.
		if active.generation != a.generation.Load() {
			active = nil
		}
	}

	uAlign := uint32(align) // #nosec G115
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

			if newOffset <= uint32(len(active.data)) { // #nosec G115
				if atomic.CompareAndSwapUint32(&active.offset, oldOffset, newOffset) {
					start = oldOffset + pad
					claimed = true
					a.stats.UsedBytes.Add(int64(newOffset - oldOffset))
					// Metrics moved out of lock
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
		var buf []byte
		if a.alloc != nil {
			buf = a.alloc.Allocate(int(a.slabCap))
		} else {
			buf = GetSlab(int(a.slabCap))
		}
		newID := uint32(len(currentSlabs) + 1) // #nosec G115

		var pad uint32
		if newID == 1 {
			// Burn alignment bytes to move away from 0
			pad = uAlign
			start = pad
		}

		newOffset := start + needed
		if newOffset%8 != 0 {
			newOffset += 8 - (newOffset % 8)
		}

		newSlab := &slab{
			id:         newID,
			generation: a.generation.Load(),
			data:       buf,
			offset:     newOffset,
		}

		// Zero memory before publishing
		if zero {
			clear(buf[start : start+needed])
		}

		newSlabs := make([]*slab, len(currentSlabs)+1)
		copy(newSlabs, currentSlabs)
		newSlabs[len(currentSlabs)] = newSlab
		a.slabs.Store(&newSlabs)

		a.stats.TotalCapacity.Add(int64(a.slabCap))
		a.stats.UsedBytes.Add(int64(newOffset))

		active = newSlab
		newSlabAllocated = true
	} else {
		if zero {
			clear(active.data[start : start+needed])
		}
	}

	a.mu.Unlock()

	// Update metrics outside the lock
	if isFastPath {
		metrics.ArenaFastPathTotal.Inc()
	} else if fastPathFailed {
		metrics.ArenaFastPathFailedTotal.Inc()
	}

	if newSlabAllocated {
		metrics.ArenaSlabsTotal.Inc()
		metrics.ArenaAllocatedBytes.WithLabelValues("slab").Add(float64(a.slabCap))
	}

	slabIdx := uint64(active.id - 1)
	globalOffset := (slabIdx * uint64(a.slabCap)) + uint64(start)

	return globalOffset, nil
}

// SlabSize returns the capacity of each slab in this arena.
func (a *SlabArena) SlabSize() int {
	return int(a.slabCap)
}

// Free releases all memory associated with the arena.
func (a *SlabArena) Free() {
	a.mu.Lock()
	defer a.mu.Unlock()

	currentSlabsPtr := a.slabs.Load()
	if currentSlabsPtr == nil {
		return
	}
	currentSlabs := *currentSlabsPtr
	empty := make([]*slab, 0)
	a.slabs.Store(&empty)

	for _, s := range currentSlabs {
		if s.data != nil {
			if a.alloc != nil {
				a.alloc.Free(s.data)
			} else {
				PutSlab(s.data)
			}
		}
	}


	UnregisterArena(a.stats)
	a.stats.Active.Store(false)
	a.stats.TotalCapacity.Store(0)
	a.stats.UsedBytes.Store(0)
}

// Get retrieves the byte slice from the arena.
func (a *SlabArena) Get(offset uint64, length uint32) []byte {
	return a.GetWithGeneration(offset, length, math.MaxUint64)
}

// GetWithGeneration retrieves the byte slice from the arena, enforcing generation isolation.
func (a *SlabArena) GetWithGeneration(offset uint64, length uint32, maxGeneration uint64) []byte {
	if length == 0 {
		return nil
	}

	slabIdx := offset / uint64(a.slabCap)
	localOffset := uint32(offset & (uint64(a.slabCap) - 1)) // #nosec G115

	// Lock-free read
	slabsPtr := a.slabs.Load()
	slabs := *slabsPtr

	if int(slabIdx) >= len(slabs) { // #nosec G115
		return nil
	}

	s := slabs[slabIdx]

	// Handle placeholder slabs for large allocations
	if s.data == nil {
		var realSlab *slab
		var realIdx int
		for j := int(slabIdx); j >= 0; j-- { // #nosec G115 -- intentional conversion
			if slabs[j].data != nil {
				realSlab = slabs[j]
				realIdx = j
				break
			}
		}
		if realSlab == nil {
			return nil
		}
		// Generation isolation check
		if realSlab.generation > maxGeneration {
			return nil
		}
		adjustedOffset := localOffset + uint32(int(slabIdx)-realIdx)*a.slabCap // #nosec G115 -- intentional conversion
		if uint64(adjustedOffset)+uint64(length) > uint64(len(realSlab.data)) {
			return nil
		}
		return realSlab.data[adjustedOffset : adjustedOffset+length]
	}

	// Generation isolation check
	if s.generation > maxGeneration {
		return nil
	}

	if uint64(localOffset)+uint64(length) > uint64(len(s.data)) {
		return nil
	}

	return s.data[localOffset : localOffset+length]
}

// GetPointer returns unsafe.Pointer to the data.
// Use with caution.
func (a *SlabArena) GetPointer(offset uint64) unsafe.Pointer {
	if offset == 0 {
		return nil
	}
	slabIdx := offset / uint64(a.slabCap)
	localOffset := uint32(offset & (uint64(a.slabCap) - 1)) // #nosec G115

	// Lock-free read
	slabsPtr := a.slabs.Load()
	slabs := *slabsPtr

	if int(slabIdx) >= len(slabs) { // #nosec G115
		return nil
	}
	s := slabs[slabIdx]
	if s.data == nil {
		var realSlab *slab
		var realIdx int
		for j := int(slabIdx); j >= 0; j-- { // #nosec G115 -- intentional conversion
			if slabs[j].data != nil {
				realSlab = slabs[j]
				realIdx = j
				break
			}
		}
		if realSlab != nil {
			adjusted := localOffset + uint32(int(slabIdx)-realIdx)*a.slabCap // #nosec G115 -- intentional conversion
			return unsafe.Pointer(&realSlab.data[adjusted]) // #nosec G103
		}
		return nil
	}
	return unsafe.Pointer(&s.data[localOffset]) // #nosec G103
}
// Save serializes the arena's contents to the given writer.
func (a *SlabArena) Save(w io.Writer) error {
	slabsPtr := a.slabs.Load()
	if slabsPtr == nil {
		return nil
	}
	slabs := *slabsPtr

	// Write slab capacity
	if err := binary.Write(w, binary.LittleEndian, a.slabCap); err != nil {
		return err
	}
	// Write num slabs
	if err := binary.Write(w, binary.LittleEndian, uint32(len(slabs))); err != nil { // #nosec G115 -- intentional conversion for binary write
		return err
	}

	for _, s := range slabs {
		// Write slab ID and offset
		if err := binary.Write(w, binary.LittleEndian, s.id); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, s.offset); err != nil {
			return err
		}
		
		// Align to page boundary for mmap
		if seeker, ok := w.(io.Seeker); ok {
			pageSize := os.Getpagesize()
			curr, _ := seeker.Seek(0, io.SeekCurrent)
			padding := (pageSize - (int(curr) % pageSize)) % pageSize
			if padding > 0 {
				if _, err := w.Write(make([]byte, padding)); err != nil {
					return err
				}
			}
		}

		// Write data
		// fmt.Printf("Saving slab %d, data len %d, offset %d\n", s.id, len(s.data), s.offset)
		data := s.data
		if len(data) == 0 {
			data = make([]byte, a.slabCap)
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
	return nil
}

// Load deserializes the arena's contents from the given reader.
func (a *SlabArena) Load(r io.Reader) error {
	var slabCap uint32
	if err := binary.Read(r, binary.LittleEndian, &slabCap); err != nil {
		return err
	}
	a.slabCap = slabCap

	var numSlabs uint32
	if err := binary.Read(r, binary.LittleEndian, &numSlabs); err != nil {
		return err
	}

	slabs := make([]*slab, numSlabs)
	for i := 0; i < int(numSlabs); i++ {
		var id, offset uint32
		if err := binary.Read(r, binary.LittleEndian, &id); err != nil {
			return err
		}
		if err := binary.Read(r, binary.LittleEndian, &offset); err != nil {
			return err
		}

		// Align to page boundary
		if seeker, ok := r.(io.Seeker); ok {
			pageSize := os.Getpagesize()
			curr, _ := seeker.Seek(0, io.SeekCurrent)
			padding := (pageSize - (int(curr) % pageSize)) % pageSize
			if padding > 0 {
				if _, err := seeker.Seek(int64(padding), io.SeekCurrent); err != nil {
					return err
				}
			}
		}

		data := make([]byte, a.slabCap)
		if _, err := io.ReadFull(r, data); err != nil {
			return err
		}
		slabs[i] = &slab{
			id:     id,
			data:   data,
			offset: offset,
		}
	}
	a.slabs.Store(&slabs)
	return nil
}

// LoadMmap maps the arena's contents from the given file using mmap.
func (a *SlabArena) LoadMmap(f *os.File) error {
	var slabCap uint32
	if err := binary.Read(f, binary.LittleEndian, &slabCap); err != nil {
		return err
	}
	a.slabCap = slabCap

	var numSlabs uint32
	if err := binary.Read(f, binary.LittleEndian, &numSlabs); err != nil {
		return err
	}

	slabs := make([]*slab, numSlabs)
	for i := 0; i < int(numSlabs); i++ {
		var id, offset uint32
		if err := binary.Read(f, binary.LittleEndian, &id); err != nil {
			return err
		}
		if err := binary.Read(f, binary.LittleEndian, &offset); err != nil {
			return err
		}

		// Calculate current file offset and align
		currOff, _ := f.Seek(0, io.SeekCurrent)
		pageSize := os.Getpagesize()
		padding := (pageSize - (int(currOff) % pageSize)) % pageSize
		if padding > 0 {
			currOff, _ = f.Seek(int64(padding), io.SeekCurrent)
		}
		
		// Mmap the slab data
		data, err := Mmap(int(f.Fd()), currOff, int(a.slabCap), true)
		if err != nil {
			return fmt.Errorf("mmap slab %d failed: %v", i, err)
		}
		
		// Advance file pointer
		if _, err := f.Seek(int64(a.slabCap), io.SeekCurrent); err != nil {
			return err
		}

		slabs[i] = &slab{
			id:     id,
			data:   data,
			offset: offset,
		}
	}
	a.slabs.Store(&slabs)
	return nil
}
// IsOffHeap returns true if the arena is backed by off-heap memory.
func (a *SlabArena) IsOffHeap() bool {
	if a.alloc == nil {
		return false
	}
	_, ok := a.alloc.(*OffHeapAllocator)
	return ok
}

// ConvertToOffHeap migrates all existing slabs to off-heap memory using the provided allocator.
func (a *SlabArena) ConvertToOffHeap(alloc memory.Allocator) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	ptr := a.slabs.Load()
	if ptr == nil {
		return nil
	}
	slabs := *ptr
	for _, s := range slabs {
		newData := alloc.Allocate(len(s.data))
		if newData == nil {
			return fmt.Errorf("off-heap allocation failed")
		}
		copy(newData, s.data)
		s.data = newData
	}
	a.alloc = alloc
	return nil
}
