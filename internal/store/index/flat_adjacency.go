package index

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// FlatAdjacency provides a strictly sequential, cache-aligned storage for neighbor lists.
// It avoids dynamic allocation by enforcing a strict static size per node.
// The layout per node is: [Length (1 uint32), padding/reserved, Neighbors...].
type FlatAdjacency struct {
	baseArena *memory.SlabArena
	arena *memory.TypedArena[uint32]
	maxNeighbors int
	stride int // Total elements per node (including length header, padded to 64 bytes)
	chunks atomic.Pointer[[]uint64]
	mu sync.Mutex
	locks []sync.Mutex
}

func NewFlatAdjacency(arena *memory.SlabArena, maxNeighbors int, initialCapacity int) *FlatAdjacency {
	// 64 bytes = 16 uint32s.
	// We need 1 uint32 for length, so maxNeighbors+1 elements.
	// Pad to nearest multiple of 16 uint32s.
	stride := (maxNeighbors + 1 + 15) & ^15
	
	fa := &FlatAdjacency{
		baseArena: arena,
		arena: memory.NewTypedArena[uint32](arena),
		maxNeighbors: maxNeighbors,
		stride: stride,
		locks: make([]sync.Mutex, 65536),
	}
	
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
	if curLen > 0 && newLen < curLen * 2 {
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
	ref, err := fa.arena.AllocSliceAligned(adjacencyChunkSize * fa.stride, 64)
	if err != nil {
		return math.MaxUint64
	}
	
	// Zero initialize the length fields for all nodes in this chunk BEFORE publishing
	// (Since we only need the length to be 0 for it to be considered empty)
	chunkData := fa.arena.Get(memory.SliceRef{Offset: ref.Offset, Len: uint32(adjacencyChunkSize * fa.stride), Cap: uint32(adjacencyChunkSize * fa.stride)}) // #nosec G115
	for i := 0; i < adjacencyChunkSize; i++ {
		chunkData[i*fa.stride] = 0
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
	
	chunkData := fa.arena.Get(memory.SliceRef{Offset: offset, Len: uint32(adjacencyChunkSize * fa.stride), Cap: uint32(adjacencyChunkSize * fa.stride)})
	dest := chunkData[cOff*fa.stride : (cOff+1)*fa.stride]
	
	if len(neighbors) > fa.maxNeighbors {
		neighbors = neighbors[:fa.maxNeighbors]
	}
	
	// Copy neighbors
	copy(dest[1:], neighbors)
	
	// Atomic update length to make it visible
	atomic.StoreUint32(&dest[0], uint32(len(neighbors)))
	return nil
}

func (fa *FlatAdjacency) GetNeighbors(id uint32) ([]uint32, bool) {
	return fa.GetNeighborsWithGen(id, math.MaxUint64)
}

func (fa *FlatAdjacency) GetNeighborsWithGen(id uint32, maxGen uint64) ([]uint32, bool) {
	chunkIdx := int(id) / adjacencyChunkSize
	cOff := int(id) % adjacencyChunkSize
	
	chunksPtr := fa.chunks.Load()
	if chunksPtr == nil || chunkIdx >= len(*chunksPtr) {
		return nil, false
	}
	offset := atomic.LoadUint64(&(*chunksPtr)[chunkIdx])
	if offset == math.MaxUint64 {
		return nil, false
	}
	
	chunkData := fa.arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(adjacencyChunkSize * fa.stride), Cap: uint32(adjacencyChunkSize * fa.stride)}, maxGen)
	if chunkData == nil {
		return nil, false
	}
	
	nodeData := chunkData[cOff*fa.stride : (cOff+1)*fa.stride]
	
	length := atomic.LoadUint32(&nodeData[0])
	if length == 0 {
		return nil, true
	}
	if length > uint32(fa.maxNeighbors) {
		length = uint32(fa.maxNeighbors)
	}
	return nodeData[1 : 1+length], true
}

func (fa *FlatAdjacency) GetPackedNeighbors(id uint32) (uint64, bool) {
	return uint64(id), true
}

func (fa *FlatAdjacency) GetNeighborsFromPacked(packed uint64) []uint32 {
	res, _ := fa.GetNeighbors(uint32(packed))
	return res
}

func (fa *FlatAdjacency) GetNeighborsFromPackedWithGen(packed uint64, maxGen uint64) []uint32 {
	res, _ := fa.GetNeighborsWithGen(uint32(packed), maxGen)
	return res
}

func (fa *FlatAdjacency) CASNeighbors(id uint32, oldPacked uint64, new []uint32) bool {
	fa.Lock(id)
	defer fa.Unlock(id)
	_ = fa.SetNeighbors(id, new)
	return true
}

func (fa *FlatAdjacency) UpdateNeighbors(id uint32, fn func(old []uint32) []uint32) error {
	fa.Lock(id)
	defer fa.Unlock(id)
	
	old, _ := fa.GetNeighbors(id)
	new := fn(old)
	if new != nil {
		return fa.SetNeighbors(id, new)
	}
	return nil
}

func (fa *FlatAdjacency) Lock(id uint32) {
	fa.locks[id%65536].Lock()
}

func (fa *FlatAdjacency) Unlock(id uint32) {
	fa.locks[id%65536].Unlock()
}

func (fa *FlatAdjacency) IsOffHeap() bool {
	if fa.baseArena == nil {
		return false
	}
	return fa.baseArena.IsOffHeap()
}

func (fa *FlatAdjacency) Release() {}
func (fa *FlatAdjacency) Retain() {}

func (fa *FlatAdjacency) GetNeighborsF16(id uint32) ([]uint32, []float16.Num, bool) { return nil, nil, false }
func (fa *FlatAdjacency) GetNeighborsF16WithGen(id uint32, maxGen uint64) ([]uint32, []float16.Num, bool) { return nil, nil, false }
func (fa *FlatAdjacency) SetNeighborsF16(id uint32, neighbors []uint32, dists []float16.Num) error { return errors.New("unsupported") }

var _ types.PackedNeighbors = (*FlatAdjacency)(nil)
