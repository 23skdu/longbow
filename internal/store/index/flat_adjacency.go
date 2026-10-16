package index

import (
	"errors"
	"io"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// FlatAdjacency provides a strictly sequential, cache-aligned storage for neighbor lists.
// It avoids dynamic allocation by enforcing a strict static size per node.
// The layout per node is: [Header (64-bit atomic uint64: gen:32, length:32), Neighbors...].
type FlatAdjacency struct {
	baseArena    *memory.SlabArena
	arena        *memory.TypedArena[uint32]
	maxNeighbors int
	stride       int // Total elements per node (including 64-bit header, padded to 64 bytes)
	chunks       atomic.Pointer[[]uint64]
	mu           sync.Mutex
	refs         atomic.Int32
	// MissCallback is called when a chunk offset is MaxUint64 (evicted).
	// If it returns nil, the caller retries the lookup.
	MissCallback func(layer int) error
	// missLayer is the HNSW layer this adjacency represents, for MissCallback.
	missLayer int
}

func packHeader(gen uint32, length uint32) uint64 {
	return (uint64(gen) << 32) | uint64(length)
}

func unpackHeader(h uint64) (gen uint32, length uint32) {
	return uint32(h >> 32), uint32(h)
}

func NewFlatAdjacency(arena *memory.SlabArena, maxNeighbors int, initialCapacity int) *FlatAdjacency {
	// 64 bytes = 16 uint32s.
	// We need 2 uint32s for length & generation header, so maxNeighbors+2 elements.
	// Pad to nearest multiple of 16 uint32s.
	stride := (maxNeighbors + 2 + 15) & ^15

	fa := &FlatAdjacency{
		baseArena:    arena,
		arena:        memory.NewTypedArena[uint32](arena),
		maxNeighbors: maxNeighbors,
		stride:       stride,
	}
	fa.arena.Retain()
	fa.refs.Store(1)

	numChunks := (initialCapacity + adjacencyChunkSize - 1) / adjacencyChunkSize
	if numChunks < 1 {
		numChunks = 1
	}
	chunks := make([]uint64, numChunks)
	for i := range chunks {
		chunks[i] = math.MaxUint64
	}
	fa.chunks.Store(&chunks)

	return fa
}

func (fa *FlatAdjacency) EnsureCapacity(id uint32) {
	chunkIdx := int(id) / adjacencyChunkSize

	curPtr := fa.chunks.Load()
	if curPtr != nil && chunkIdx < len(*curPtr) {
		return
	}

	fa.mu.Lock()
	defer fa.mu.Unlock()

	curPtr = fa.chunks.Load()
	if curPtr != nil && chunkIdx < len(*curPtr) {
		return
	}

	curLen := 0
	if curPtr != nil {
		curLen = len(*curPtr)
	}
	newLen := chunkIdx + 1
	if curLen > 0 && newLen < curLen*2 {
		newLen = curLen * 2
	}

	newChunks := make([]uint64, newLen)
	for i := range newChunks {
		newChunks[i] = math.MaxUint64
	}
	if curPtr != nil {
		copy(newChunks, *curPtr)
	}
	fa.chunks.Store(&newChunks)
}

func (fa *FlatAdjacency) ensureChunk(chunkIdx int) uint64 {
	chunksPtr := fa.chunks.Load()
	if chunksPtr == nil || chunkIdx >= len(*chunksPtr) {
		return math.MaxUint64
	}

	offset := atomic.LoadUint64(&(*chunksPtr)[chunkIdx])
	if offset != math.MaxUint64 {
		return offset
	}

	fa.mu.Lock()
	defer fa.mu.Unlock()

	// Reload chunksPtr in case EnsureCapacity reallocated it
	chunksPtr = fa.chunks.Load()
	offset = atomic.LoadUint64(&(*chunksPtr)[chunkIdx])
	if offset != math.MaxUint64 {
		return offset
	}

	// Allocate a new chunk (aligned to 64 bytes)
	// We need ChunkSize * stride elements.
	ref, err := fa.arena.AllocSliceAligned(adjacencyChunkSize*fa.stride, 64)
	if err != nil {
		return math.MaxUint64
	}

	// Zero initialize the 64-bit header fields for all nodes in this chunk BEFORE publishing
	chunkData := fa.arena.Get(memory.SliceRef{Offset: ref.Offset, Len: uint32(adjacencyChunkSize * fa.stride), Cap: uint32(adjacencyChunkSize * fa.stride)}) // #nosec G115
	for i := 0; i < adjacencyChunkSize; i++ {
		headerPtr := (*uint64)(unsafe.Pointer(&chunkData[i*fa.stride])) // #nosec G103
		atomic.StoreUint64(headerPtr, 0)
	}

	atomic.StoreUint64(&(*chunksPtr)[chunkIdx], ref.Offset)
	return ref.Offset
}

func (fa *FlatAdjacency) SetNeighbors(id uint32, neighbors []uint32) error {
	fa.EnsureCapacity(id)
	chunkIdx := int(id) / adjacencyChunkSize
	cOff := int(id) % adjacencyChunkSize

	offset := fa.ensureChunk(chunkIdx)
	if offset == math.MaxUint64 {
		return errors.New("flat adjacency: chunk allocation failed")
	}

	chunkData := fa.arena.Get(memory.SliceRef{Offset: offset, Len: uint32(adjacencyChunkSize * fa.stride), Cap: uint32(adjacencyChunkSize * fa.stride)}) // #nosec G115
	dest := chunkData[cOff*fa.stride : (cOff+1)*fa.stride]
	headerPtr := (*uint64)(unsafe.Pointer(&dest[0])) // #nosec G103

	if len(neighbors) > fa.maxNeighbors {
		neighbors = neighbors[:fa.maxNeighbors]
	}

	backoff := 1
	for {
		curHeader := atomic.LoadUint64(headerPtr)
		curGen, curLen := unpackHeader(curHeader)

		if curGen&1 != 0 {
			for b := 0; b < backoff; b++ {
			}
			if backoff < 64 {
				backoff <<= 1
			} else {
				runtime.Gosched()
			}
			continue
		}

		leaseHeader := packHeader(curGen+1, curLen)
		if !atomic.CompareAndSwapUint64(headerPtr, curHeader, leaseHeader) {
			if backoff < 64 {
				backoff <<= 1
			} else {
				runtime.Gosched()
			}
			continue
		}

		// Copy neighbors into dest[2:]
		copy(dest[2:], neighbors)

		// Atomic release with even generation and updated length
		finalHeader := packHeader(curGen+2, uint32(len(neighbors))) // #nosec G115
		atomic.StoreUint64(headerPtr, finalHeader)
		return nil
	}
}

func (fa *FlatAdjacency) GetNeighbors(id uint32) ([]uint32, bool) {
	return fa.GetNeighborsWithGen(id, math.MaxUint64)
}

func (fa *FlatAdjacency) GetNeighborsWithGen(id uint32, maxGen uint64) ([]uint32, bool) {
	fa.Retain()
	defer fa.Release()
	return fa.getNeighborsWithGenInner(id, maxGen)
}

// GetNeighborsWithGenFast is like GetNeighborsWithGen but skips Retain/Release
// ref-counting for callers that guarantee the FlatAdjacency remains alive.
func (fa *FlatAdjacency) GetNeighborsWithGenFast(id uint32, maxGen uint64) ([]uint32, bool) {
	return fa.getNeighborsWithGenInner(id, maxGen)
}

func (fa *FlatAdjacency) getNeighborsWithGenInner(id uint32, maxGen uint64) ([]uint32, bool) {
	chunkIdx := int(id) / adjacencyChunkSize
	cOff := int(id) % adjacencyChunkSize

	chunksPtr := fa.chunks.Load()
	if chunksPtr == nil || chunkIdx >= len(*chunksPtr) {
		return nil, false
	}
	offset := atomic.LoadUint64(&(*chunksPtr)[chunkIdx])
	if offset == math.MaxUint64 {
		if fa.MissCallback != nil {
			if err := fa.MissCallback(fa.missLayer); err == nil {
				offset = atomic.LoadUint64(&(*chunksPtr)[chunkIdx])
			}
		}
		if offset == math.MaxUint64 {
			return nil, false
		}
	}

	chunkData := fa.arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(adjacencyChunkSize * fa.stride), Cap: uint32(adjacencyChunkSize * fa.stride)}, maxGen) // #nosec G115
	if chunkData == nil {
		return nil, false
	}

	nodeData := chunkData[cOff*fa.stride : (cOff+1)*fa.stride]
	headerPtr := (*uint64)(unsafe.Pointer(&nodeData[0])) // #nosec G103

	for retries := 0; retries < 100; retries++ {
		h1 := atomic.LoadUint64(headerPtr)
		gen1, len1 := unpackHeader(h1)

		if gen1&1 != 0 {
			runtime.Gosched()
			continue
		}
		if len1 == 0 {
			return nil, true
		}
		if len1 > uint32(fa.maxNeighbors) { // #nosec G115
			len1 = uint32(fa.maxNeighbors) // #nosec G115
		}

		res := make([]uint32, len1)
		copy(res, nodeData[2:2+len1])

		h2 := atomic.LoadUint64(headerPtr)
		if h1 == h2 {
			return res, true
		}
	}

	// Fallback under extreme write contention
	h := atomic.LoadUint64(headerPtr)
	_, l := unpackHeader(h)
	if l == 0 {
		return nil, true
	}
	if l > uint32(fa.maxNeighbors) { // #nosec G115
		l = uint32(fa.maxNeighbors) // #nosec G115
	}
	res := make([]uint32, l)
	copy(res, nodeData[2:2+l])
	return res, true
}

func (fa *FlatAdjacency) GetPackedNeighbors(id uint32) (uint64, bool) {
	chunkIdx := int(id) / adjacencyChunkSize
	cOff := int(id) % adjacencyChunkSize

	chunksPtr := fa.chunks.Load()
	if chunksPtr == nil || chunkIdx >= len(*chunksPtr) {
		return 0, true
	}
	offset := atomic.LoadUint64(&(*chunksPtr)[chunkIdx])
	if offset == math.MaxUint64 {
		return 0, true
	}

	chunkData := fa.arena.Get(memory.SliceRef{Offset: offset, Len: uint32(adjacencyChunkSize * fa.stride), Cap: uint32(adjacencyChunkSize * fa.stride)}) // #nosec G115
	if chunkData == nil {
		return 0, true
	}

	nodeData := chunkData[cOff*fa.stride : (cOff+1)*fa.stride]
	headerPtr := (*uint64)(unsafe.Pointer(&nodeData[0])) // #nosec G103
	return atomic.LoadUint64(headerPtr), true
}

func (fa *FlatAdjacency) GetNeighborsFromPacked(packed uint64) []uint32 {
	res, _ := fa.GetNeighbors(uint32(packed)) // #nosec G115
	return res
}

func (fa *FlatAdjacency) GetNeighborsFromPackedWithGen(packed uint64, maxGen uint64) []uint32 {
	res, _ := fa.GetNeighborsWithGen(uint32(packed), maxGen) // #nosec G115
	return res
}

func (fa *FlatAdjacency) CASNeighbors(id uint32, oldPacked uint64, new []uint32) bool {
	fa.EnsureCapacity(id)
	chunkIdx := int(id) / adjacencyChunkSize
	cOff := int(id) % adjacencyChunkSize

	offset := fa.ensureChunk(chunkIdx)
	if offset == math.MaxUint64 {
		return false
	}

	chunkData := fa.arena.Get(memory.SliceRef{Offset: offset, Len: uint32(adjacencyChunkSize * fa.stride), Cap: uint32(adjacencyChunkSize * fa.stride)}) // #nosec G115
	dest := chunkData[cOff*fa.stride : (cOff+1)*fa.stride]
	headerPtr := (*uint64)(unsafe.Pointer(&dest[0])) // #nosec G103

	curHeader := atomic.LoadUint64(headerPtr)
	if oldPacked != uint64(id) && curHeader != oldPacked {
		return false
	}

	curGen, curLen := unpackHeader(curHeader)
	if curGen&1 != 0 {
		return false
	}

	if len(new) > fa.maxNeighbors {
		new = new[:fa.maxNeighbors]
	}

	leaseHeader := packHeader(curGen+1, curLen)
	if !atomic.CompareAndSwapUint64(headerPtr, curHeader, leaseHeader) {
		return false
	}

	copy(dest[2:], new)

	finalHeader := packHeader(curGen+2, uint32(len(new))) // #nosec G115
	atomic.StoreUint64(headerPtr, finalHeader)
	return true
}

func (fa *FlatAdjacency) UpdateNeighbors(id uint32, applier types.NeighborApplier) error {
	fa.EnsureCapacity(id)
	chunkIdx := int(id) / adjacencyChunkSize
	cOff := int(id) % adjacencyChunkSize

	offset := fa.ensureChunk(chunkIdx)
	if offset == math.MaxUint64 {
		return errors.New("flat adjacency: chunk allocation failed")
	}

	chunkData := fa.arena.Get(memory.SliceRef{Offset: offset, Len: uint32(adjacencyChunkSize * fa.stride), Cap: uint32(adjacencyChunkSize * fa.stride)}) // #nosec G115
	dest := chunkData[cOff*fa.stride : (cOff+1)*fa.stride]
	headerPtr := (*uint64)(unsafe.Pointer(&dest[0])) // #nosec G103

	backoff := 1
	var oldBuf [256]uint32
	for {
		curHeader := atomic.LoadUint64(headerPtr)
		curGen, curLen := unpackHeader(curHeader)

		if curGen&1 != 0 {
			for b := 0; b < backoff; b++ {
			}
			if backoff < 64 {
				backoff <<= 1
			} else {
				runtime.Gosched()
			}
			continue
		}

		if curLen > uint32(fa.maxNeighbors) { // #nosec G115
			curLen = uint32(fa.maxNeighbors) // #nosec G115
		}

		var old []uint32
		if curLen > 0 {
			old = oldBuf[:curLen]
			copy(old, dest[2:2+curLen])
			if atomic.LoadUint64(headerPtr) != curHeader {
				continue
			}
		}

		new := applier.ApplyNeighbors(old)
		if new == nil {
			return nil
		}

		if len(new) > fa.maxNeighbors {
			new = new[:fa.maxNeighbors]
		}

		leaseHeader := packHeader(curGen+1, curLen)
		if !atomic.CompareAndSwapUint64(headerPtr, curHeader, leaseHeader) {
			if backoff < 64 {
				backoff <<= 1
			} else {
				runtime.Gosched()
			}
			continue
		}

		copy(dest[2:], new)

		finalHeader := packHeader(curGen+2, uint32(len(new))) // #nosec G115
		atomic.StoreUint64(headerPtr, finalHeader)
		return nil
	}
}

// Lock is a no-op for lock-free FlatAdjacency, provided for types.PackedNeighbors interface compatibility.
func (fa *FlatAdjacency) Lock(id uint32) {}

// Unlock is a no-op for lock-free FlatAdjacency, provided for types.PackedNeighbors interface compatibility.
func (fa *FlatAdjacency) Unlock(id uint32) {}

func (fa *FlatAdjacency) IsOffHeap() bool {
	if fa.baseArena == nil {
		return false
	}
	return fa.baseArena.IsOffHeap()
}

func (fa *FlatAdjacency) Release() {
	if fa.refs.Add(-1) == 0 {
		if fa.arena != nil {
			fa.arena.Release()
		}
		fa.chunks.Store(nil)
	}
}

func (fa *FlatAdjacency) Retain() {
	fa.refs.Add(1)
}

func (fa *FlatAdjacency) GetNeighborsF16(id uint32) ([]uint32, []float16.Num, bool) {
	return nil, nil, false
}
func (fa *FlatAdjacency) GetNeighborsF16WithGen(id uint32, maxGen uint64) ([]uint32, []float16.Num, bool) {
	return nil, nil, false
}
func (fa *FlatAdjacency) SetNeighborsF16(id uint32, neighbors []uint32, dists []float16.Num) error {
	return errors.New("unsupported")
}

// EvictToDisk writes all populated neighbor chunks to w and clears in-memory storage.
func (fa *FlatAdjacency) EvictToDisk(gd *types.GraphData, layer int, chunkSizes []int, w interface{ Write([]byte) (int, error) }) (int, []int, int64, error) {
	chunksPtr := fa.chunks.Load()
	if chunksPtr == nil {
		return 0, chunkSizes, 0, nil
	}
	chunks := *chunksPtr
	numChunks := len(chunks)
	nWritten := 0
	var totalBytes int64

	// Grow chunkSizes if needed
	if len(chunkSizes) < numChunks {
		newSizes := make([]int, numChunks)
		copy(newSizes, chunkSizes)
		chunkSizes = newSizes
	}

	for cID := 0; cID < numChunks; cID++ {
		offset := atomic.LoadUint64(&chunks[cID])
		if offset == math.MaxUint64 {
			chunkSizes[cID] = 0
			continue
		}

		chunk := fa.arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(adjacencyChunkSize * fa.stride), // #nosec G115
			Cap:    uint32(adjacencyChunkSize * fa.stride), // #nosec G115
		})

		chunkSizes[cID] = len(chunk)
		if len(chunk) > 0 {
			byteSlice := unsafe.Slice((*byte)(unsafe.Pointer(&chunk[0])), len(chunk)*4) // #nosec G103
			if _, err := w.Write(byteSlice); err != nil {
				return nWritten, chunkSizes, totalBytes, err
			}
		}
		nWritten++
		totalBytes += int64(len(chunk)) * 4

		atomic.StoreUint64(&chunks[cID], math.MaxUint64)
	}

	return nWritten, chunkSizes, totalBytes, nil
}

// RestoreFromDisk reads neighbor chunks from r and repopulates in-memory storage.
func (fa *FlatAdjacency) RestoreFromDisk(gd *types.GraphData, layer int, chunkSizes []int, r interface{ Read([]byte) (int, error) }) error {
	chunksPtr := fa.chunks.Load()
	if chunksPtr == nil {
		return nil
	}
	chunks := *chunksPtr

	for cID := 0; cID < len(chunkSizes) && cID < len(chunks); cID++ {
		sz := chunkSizes[cID]
		if sz == 0 {
			continue
		}

		buf, err := fa.arena.AllocSliceAligned(sz, 64)
		if err != nil {
			return err
		}
		chunk := fa.arena.Get(buf)
		if len(chunk) > 0 {
			byteBuf := unsafe.Slice((*byte)(unsafe.Pointer(&chunk[0])), len(chunk)*4) // #nosec G103
			if _, err := io.ReadFull(r.(io.Reader), byteBuf); err != nil {
				return err
			}
		}
		atomic.StoreUint64(&chunks[cID], buf.Offset)
	}

	return nil
}

var _ types.PackedNeighbors = (*FlatAdjacency)(nil)
