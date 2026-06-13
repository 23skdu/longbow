package index

import (
	"encoding/binary"
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
	baseArena    *memory.SlabArena
	arena        *memory.TypedArena[uint32]
	maxNeighbors int
	stride       int // Total elements per node (including length header, padded to 64 bytes)
	chunks       atomic.Pointer[[]uint64]
	mu           sync.Mutex
	locks        []sync.Mutex
	refs         atomic.Int32
	// MissCallback is called when a chunk offset is MaxUint64 (evicted).
	// If it returns nil, the caller retries the lookup.
	MissCallback func(layer int) error
	// missLayer is the HNSW layer this adjacency represents, for MissCallback.
	missLayer int
}

func NewFlatAdjacency(arena *memory.SlabArena, maxNeighbors int, initialCapacity int) *FlatAdjacency {
	// 64 bytes = 16 uint32s.
	// We need 1 uint32 for length, so maxNeighbors+1 elements.
	// Pad to nearest multiple of 16 uint32s.
	stride := (maxNeighbors + 1 + 15) & ^15

	fa := &FlatAdjacency{
		baseArena:    arena,
		arena:        memory.NewTypedArena[uint32](arena),
		maxNeighbors: maxNeighbors,
		stride:       stride,
		locks:        make([]sync.Mutex, 65536),
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

	chunkData := fa.arena.Get(memory.SliceRef{Offset: offset, Len: uint32(adjacencyChunkSize * fa.stride), Cap: uint32(adjacencyChunkSize * fa.stride)}) // #nosec G115
	dest := chunkData[cOff*fa.stride : (cOff+1)*fa.stride]

	if len(neighbors) > fa.maxNeighbors {
		neighbors = neighbors[:fa.maxNeighbors]
	}

	// Copy neighbors
	copy(dest[1:], neighbors)

	// Atomic update length to make it visible
	atomic.StoreUint32(&dest[0], uint32(len(neighbors))) // #nosec G115
	return nil
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

	length := atomic.LoadUint32(&nodeData[0])
	if length == 0 {
		return nil, true
	}
	if length > uint32(fa.maxNeighbors) { // #nosec G115
		length = uint32(fa.maxNeighbors) // #nosec G115
	}
	return nodeData[1 : 1+length], true
}

func (fa *FlatAdjacency) GetPackedNeighbors(id uint32) (uint64, bool) {
	return uint64(id), true
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
		if err := binary.Write(w, binary.LittleEndian, chunk); err != nil {
			return nWritten, chunkSizes, totalBytes, err
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
		if err := binary.Read(r, binary.LittleEndian, chunk); err != nil {
			return err
		}
		atomic.StoreUint64(&chunks[cID], buf.Offset)
	}

	return nil
}

var _ types.PackedNeighbors = (*FlatAdjacency)(nil)
