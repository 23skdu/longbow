package types

import (
	"fmt"
	arrowmemory "github.com/apache/arrow-go/v18/arrow/memory"
	"sync/atomic"
	"unsafe"

	"github.com/23skdu/longbow/internal/memory"
)

type VectorChunkBatch[T any] struct {
	arena      *memory.TypedArena[T]
	batch      memory.TypedBatch[T]
	offsets    []uint64
	legacy     [][]T
	pd         int
	chunkLen   int
	rejectZero bool
}

// newVectorChunkBatch builds a batch over offsets, falling back to legacy for
// chunk ids the offset table does not cover. rejectZero mirrors the reference
// accessor: every GetXChunkWithGen / GetXChunkFast treats a zero offset as "not
// resident" and returns nil, so a batch that forwarded a zero offset into the
// arena would hand back whatever happens to live at the start of the first slab
// as if it were the chunk's contents. Every accessor rejects zero, so every
// batch does too.
func newVectorChunkBatch[T any](arena *memory.TypedArena[T], offsets []uint64, legacy [][]T, pd int, maxGen uint64, rejectZero bool) VectorChunkBatch[T] {
	b := VectorChunkBatch[T]{
		arena:      arena,
		offsets:    offsets,
		legacy:     legacy,
		pd:         pd,
		chunkLen:   ChunkSize * pd,
		rejectZero: rejectZero,
	}
	if arena != nil {
		b.batch = arena.BeginBatch(maxGen)
	}
	return b
}

// Chunk returns the whole chunk, or nil when the reference chunk accessor
// would return nil.
func (b *VectorChunkBatch[T]) Chunk(chunkID int) []T {
	if chunkID < 0 {
		return nil
	}
	if b.arena != nil && chunkID < len(b.offsets) {
		offset := atomic.LoadUint64(&b.offsets[chunkID])
		if offset == 0 && b.rejectZero {
			return nil
		}
		return b.batch.Get(memory.SliceRef{Offset: offset, Len: uint32(b.chunkLen), Cap: uint32(b.chunkLen)}) // #nosec G115
	}
	if chunkID < len(b.legacy) {
		return b.legacy[chunkID]
	}
	return nil
}

// Width returns the per-vector element count the batch strides by: the padded
// dimension count for the fixed-width types, and the packed byte stride for
// TurboQuant. It is the length to pass as the dims argument of Vector.
func (b *VectorChunkBatch[T]) Width() int {
	return b.pd
}

// Vector returns the dims-long vector stored at index within chunkID, or nil
// when the reference per-vector accessor would not have served it (chunk not
// resident, hidden by generation isolation, or index past the chunk).
func (b *VectorChunkBatch[T]) Vector(chunkID, index, dims int) []T {
	if index < 0 || dims <= 0 {
		return nil
	}
	chunk := b.Chunk(chunkID)
	if chunk == nil {
		return nil
	}
	start := index * b.pd
	end := start + dims
	if start < 0 || end < start || end > len(chunk) {
		return nil
	}
	return chunk[start:end]
}

// Stale reports whether the arena was structurally mutated after the batch was
// opened. The batch re-resolves transparently, so this is only for callers
// that want to observe the mutation.
func (b *VectorChunkBatch[T]) Stale() bool {
	return b.arena == nil || b.batch.Stale()
}

// BeginFloat32ChunkBatch opens a batch-scoped view over the float32 chunk
// table, matching GetVectorsChunkWithGen / GetVectorsChunkFast.
func (g *GraphData) BeginFloat32ChunkBatch(maxGen uint64) VectorChunkBatch[float32] {
	if g == nil {
		return VectorChunkBatch[float32]{}
	}
	return newVectorChunkBatch(g.Float32Arena, g.VectorsF32, g.Vectors,
		g.GetPaddedDimsForType(VectorTypeFloat32), maxGen, true)
}

// BeginInt8ChunkBatch opens a batch-scoped view over the int8 chunk table,
// matching GetVectorsInt8ChunkWithGen / GetVectorsInt8ChunkFast.
func (g *GraphData) BeginInt8ChunkBatch(maxGen uint64) VectorChunkBatch[int8] {
	if g == nil {
		return VectorChunkBatch[int8]{}
	}
	return newVectorChunkBatch(g.Int8Arena, g.VectorsInt8, nil,
		g.GetPaddedDimsForType(VectorTypeInt8), maxGen, true)
}

// BeginInt16ChunkBatch opens a batch-scoped view over the int16 chunk table,
// matching GetVectorsInt16ChunkWithGen / GetVectorsInt16ChunkFast.
func (g *GraphData) BeginInt16ChunkBatch(maxGen uint64) VectorChunkBatch[int16] {
	if g == nil {
		return VectorChunkBatch[int16]{}
	}
	return newVectorChunkBatch(g.Int16Arena, g.VectorsInt16, nil,
		g.GetPaddedDimsForType(VectorTypeInt16), maxGen, true)
}

// BeginUint16ChunkBatch opens a batch-scoped view over the uint16 chunk table,
// matching GetVectorsUint16ChunkWithGen / GetVectorsUint16ChunkFast.
func (g *GraphData) BeginUint16ChunkBatch(maxGen uint64) VectorChunkBatch[uint16] {
	if g == nil {
		return VectorChunkBatch[uint16]{}
	}
	return newVectorChunkBatch(g.Uint16Arena, g.VectorsUint16, nil,
		g.GetPaddedDimsForType(VectorTypeUint16), maxGen, true)
}

// BeginInt32ChunkBatch opens a batch-scoped view over the int32 chunk table,
// matching GetVectorsInt32ChunkWithGen / GetVectorsInt32ChunkFast.
func (g *GraphData) BeginInt32ChunkBatch(maxGen uint64) VectorChunkBatch[int32] {
	if g == nil {
		return VectorChunkBatch[int32]{}
	}
	return newVectorChunkBatch(g.Int32Arena, g.VectorsInt32, nil,
		g.GetPaddedDimsForType(VectorTypeInt32), maxGen, true)
}

// BeginUint32ChunkBatch opens a batch-scoped view over the uint32 chunk table,
// matching GetVectorsUint32ChunkWithGen / GetVectorsUint32ChunkFast.
func (g *GraphData) BeginUint32ChunkBatch(maxGen uint64) VectorChunkBatch[uint32] {
	if g == nil {
		return VectorChunkBatch[uint32]{}
	}
	return newVectorChunkBatch(g.Uint32Arena, g.VectorsUint32, nil,
		g.GetPaddedDimsForType(VectorTypeUint32), maxGen, true)
}

// BeginInt64ChunkBatch opens a batch-scoped view over the int64 chunk table,
// matching GetVectorsInt64ChunkWithGen / GetVectorsInt64ChunkFast.
func (g *GraphData) BeginInt64ChunkBatch(maxGen uint64) VectorChunkBatch[int64] {
	if g == nil {
		return VectorChunkBatch[int64]{}
	}
	return newVectorChunkBatch(g.Int64Arena, g.VectorsInt64, nil,
		g.GetPaddedDimsForType(VectorTypeInt64), maxGen, true)
}

// BeginUint64ChunkBatch opens a batch-scoped view over the uint64 chunk table,
// matching GetVectorsUint64ChunkWithGen / GetVectorsUint64ChunkFast.
func (g *GraphData) BeginUint64ChunkBatch(maxGen uint64) VectorChunkBatch[uint64] {
	if g == nil {
		return VectorChunkBatch[uint64]{}
	}
	return newVectorChunkBatch(g.Uint64Arena, g.VectorsUint64, nil,
		g.GetPaddedDimsForType(VectorTypeUint64), maxGen, true)
}

// BeginFloat64ChunkBatch opens a batch-scoped view over the float64 chunk
// table, matching GetVectorsFloat64ChunkWithGen / GetVectorsFloat64ChunkFast.
func (g *GraphData) BeginFloat64ChunkBatch(maxGen uint64) VectorChunkBatch[float64] {
	if g == nil {
		return VectorChunkBatch[float64]{}
	}
	return newVectorChunkBatch(g.Float64Arena, g.VectorsFloat64Offsets, g.VectorsFloat64,
		g.GetPaddedDimsForType(VectorTypeFloat64), maxGen, true)
}

// BeginTQChunkBatch opens a batch-scoped view over the TurboQuant chunk table,
// matching GetVectorsTQChunkWithGen / GetVectorsTQChunkFast.
//
// TurboQuant rows have a fixed packed stride rather than padded dimensions, so
// the stride is passed as the batch's element width: Vector(chunkID, index,
// stride) returns exactly the bytes GetVectorsTQChunkWithGen would have sliced
// for that id, with the same nil results for a non-resident chunk, a zero
// offset and a generation-hidden slab.
func (g *GraphData) BeginTQChunkBatch(maxGen uint64) VectorChunkBatch[byte] {
	if g == nil {
		return VectorChunkBatch[byte]{}
	}
	return newVectorChunkBatch(g.Uint8Arena, g.VectorsTQ, nil,
		g.PackedSize(), maxGen, true)
}

// GetVectorsChunk returns the vector chunk for the given ID.

func initArenaSafe[T any](arenaPtr **memory.TypedArena[T], slabSize int, alloc arrowmemory.Allocator) {
	if atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(arenaPtr))) == nil { // #nosec G103
		var sa *memory.SlabArena
		if alloc != nil {
			sa = memory.NewSlabArenaWithAllocator(slabSize, alloc)
		} else {
			sa = memory.NewSlabArena(slabSize)
		}
		newArena := memory.NewTypedArena[T](sa)
		if !atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(arenaPtr)), nil, unsafe.Pointer(newArena)) { // #nosec G103
			// Lost race, another goroutine already initialized it.
			// Release the arena we allocated to avoid an mmap leak.
			newArena.Release()
		}
	}
}

// EnsureChunks ensures that all chunks up to newCap are allocated.
func (g *GraphData) EnsureChunks(newCap, dims int) error {
	numChunks := (newCap + ChunkSize - 1) / ChunkSize
	g.GrowMetadataSlices(numChunks)
	for i := 0; i < numChunks; i++ {
		if err := g.EnsureChunk(i, 0, dims); err != nil {
			return err
		}
	}
	g.Capacity = numChunks * ChunkSize
	return nil
}

// ReleaseChunk releases the memory for a vector chunk back to the OS using MADV_DONTNEED.
// This is used for incremental handover during index migration.
func (g *GraphData) ReleaseChunk(cID int) {
	// Release primary vector storage
	if g.Float32Arena != nil && cID < len(g.VectorsF32) {
		offset := atomic.SwapUint64(&g.VectorsF32[cID], 0)
		if offset != 0 {
			pd := g.GetPaddedDimsForType(VectorTypeFloat32)
			g.releaseArenaMemory(g.Float32Arena.Slab(), offset, uint32(ChunkSize*pd)*4) // #nosec G115
		}
	}
	if cID < len(g.Vectors) {
		g.Vectors[cID] = nil
	}
	if g.Float64Arena != nil && cID < len(g.VectorsFloat64Offsets) {
		offset := atomic.SwapUint64(&g.VectorsFloat64Offsets[cID], 0)
		if offset != 0 {
			g.releaseArenaMemory(g.Float64Arena.Slab(), offset, uint32(ChunkSize*g.Dims)*8) // #nosec G115
		}
	}
	if cID < len(g.VectorsFloat64) {
		g.VectorsFloat64[cID] = nil
	}
	if g.Uint8Arena != nil {
		if cID < len(g.VectorsSQ8) {
			offset := atomic.SwapUint64(&g.VectorsSQ8[cID], 0)
			if offset != 0 {
				paddedDims := (g.Dims + 63) & ^63
				g.releaseArenaMemory(g.Uint8Arena.Slab(), offset, uint32(ChunkSize*paddedDims)) // #nosec G115
			}
		}
		if cID < len(g.VectorsTQ) {
			offset := atomic.SwapUint64(&g.VectorsTQ[cID], 0)
			if offset != 0 {
				stride := g.PackedSize()
				g.releaseArenaMemory(g.Uint8Arena.Slab(), offset, uint32(ChunkSize*stride)) // #nosec G115
			}
		}
	}
	if cID < len(g.VectorsComplex64) {
		g.VectorsComplex64[cID] = nil
	}
	if cID < len(g.VectorsComplex128) {
		g.VectorsComplex128[cID] = nil
	}
	if cID < len(g.VectorsInt8) {
		atomic.SwapUint64(&g.VectorsInt8[cID], 0)
	}
	if cID < len(g.VectorsInt16) {
		atomic.SwapUint64(&g.VectorsInt16[cID], 0)
	}
	if cID < len(g.VectorsUint16) {
		atomic.SwapUint64(&g.VectorsUint16[cID], 0)
	}
	if cID < len(g.VectorsF16) {
		atomic.SwapUint64(&g.VectorsF16[cID], 0)
	}
	if cID < len(g.VectorsInt32) {
		atomic.SwapUint64(&g.VectorsInt32[cID], 0)
	}
	if cID < len(g.VectorsUint32) {
		atomic.SwapUint64(&g.VectorsUint32[cID], 0)
	}
	if cID < len(g.VectorsInt64) {
		atomic.SwapUint64(&g.VectorsInt64[cID], 0)
	}
	if cID < len(g.VectorsUint64) {
		atomic.SwapUint64(&g.VectorsUint64[cID], 0)
	}
}

// ReleaseNeighborsChunk releases neighbor storage for a specific layer and chunk.
func (g *GraphData) ReleaseNeighborsChunk(layer, cID int) {
	if layer < len(g.Neighbors) && cID < len(g.Neighbors[layer]) && g.Uint32Arena != nil {
		offset := atomic.SwapUint64(&g.Neighbors[layer][cID], 0)
		if offset != 0 {
			g.releaseArenaMemory(g.Uint32Arena.Slab(), offset, uint32(ChunkSize*MaxNeighbors)*4) // #nosec G115
		}
	}
}

// ReleaseFloat32Chunk releases monolithic Float32Arena vector storage for a specific chunk.
func (g *GraphData) ReleaseFloat32Chunk(cID int) {
	if cID < len(g.VectorsF32) && g.Float32Arena != nil {
		offset := atomic.SwapUint64(&g.VectorsF32[cID], 0)
		if offset != 0 {
			paddedDims := g.GetPaddedDimsForType(VectorTypeFloat32)
			g.releaseArenaMemory(g.Float32Arena.Slab(), offset, uint32(ChunkSize*paddedDims)*4) // #nosec G115
		}
	}
}

func (g *GraphData) releaseArenaMemory(s *memory.SlabArena, offset uint64, size uint32) {
	if s == nil {
		return
	}
	data := s.Get(offset, size)
	if len(data) > 0 {
		// Use Madvise to tell the OS we don't need these physical pages anymore.
		// This is safer than Munmap because pointers/offsets remain valid (but point to zeroed pages).
		_ = memory.Madvise(data, memory.MadvDontNeed)
	}
}

func (g *GraphData) EnsureChunk(cID, cOff, dims int) error {
	if g.Dims == 0 && dims > 0 {
		g.Dims = dims
	}
	// 1. Ensure Vectors (Float32 / Unknown)
	if !g.SharedVectorSpace && (g.Type == VectorTypeFloat32 || g.Type == VectorTypeUnknown) {
		paddedDims := g.GetPaddedDimsForType(VectorTypeFloat32)
		if cID < len(g.VectorsF32) && atomic.LoadUint64(&g.VectorsF32[cID]) == 0 && dims > 0 && paddedDims > 0 {
			slabSize := ChunkSize*paddedDims*4 + 64
			if slabSize < 1024*1024 {
				slabSize = 1024 * 1024
			}
			initArenaSafe(&g.Float32Arena, slabSize, g.Allocator)
			ref, err := g.Float32Arena.AllocSlice(ChunkSize * paddedDims)
			if err != nil {
				return err
			}
			atomic.StoreUint64(&g.VectorsF32[cID], ref.Offset)
		}
	}

	// 2. Ensure SQ8 if enabled
	if g.SQ8Enabled {
		paddedDims := (dims + 63) & ^63
		if cID < len(g.VectorsSQ8) && atomic.LoadUint64(&g.VectorsSQ8[cID]) == 0 && dims > 0 && paddedDims > 0 {
			slabSize := ChunkSize*paddedDims + 64
			if slabSize < 1024*1024 {
				slabSize = 1024 * 1024
			}
			initArenaSafe(&g.Uint8Arena, slabSize, g.Allocator)
			ref, err := g.Uint8Arena.AllocSliceDirty(ChunkSize * paddedDims)
			if err != nil {
				return err
			}
			atomic.StoreUint64(&g.VectorsSQ8[cID], ref.Offset)
		}
	}

	// 3. Ensure PQ if enabled
	if g.PQEnabled && g.PQM > 0 {
		numWordsPerNode := (g.PQM + 7) / 8
		if cID < len(g.VectorsPQ) && atomic.LoadUint64(&g.VectorsPQ[cID]) == 0 && dims > 0 && numWordsPerNode > 0 {
			slabSize := ChunkSize*numWordsPerNode*8 + 64
			if slabSize < 1024*1024 {
				slabSize = 1024 * 1024
			}
			initArenaSafe(&g.Uint64Arena, slabSize, g.Allocator)
			ref, err := g.Uint64Arena.AllocSliceDirty(ChunkSize * numWordsPerNode)
			if err != nil {
				return err
			}
			atomic.StoreUint64(&g.VectorsPQ[cID], ref.Offset)
		}
	}

	// 4. Levels are pre-allocated in GrowMetadataSlices

	// Optimization: Ensure Neighbors, Counts, Versions for the requested chunk index.
	// Only layer 0 gets neighbor pre-allocation (always needed).
	// Upper layers use PackedNeighbors/TopLayerManager — skipping their neighbor
	// pre-allocation saves ~14 GB of off-heap tracked memory at 500k nodes.
	for l := 0; l < ArrowMaxLayers; l++ {
		if len(g.Neighbors) <= l {
			panic(fmt.Sprintf("Neighbors slice too small: %d <= %d", len(g.Neighbors), l))
		}
		if len(g.Neighbors[l]) <= cID {
			panic(fmt.Sprintf("Neighbors[%d] slice too small: %d <= %d (capacity: %d)", l, len(g.Neighbors[l]), cID, g.Capacity))
		}

		if l == 0 {
			if atomic.LoadUint64(&g.Neighbors[l][cID]) == 0 {
				neighborSlabSize := ChunkSize*MaxNeighbors*4 + 64
				if neighborSlabSize < 1024*1024 {
					neighborSlabSize = 1024 * 1024
				}
				initArenaSafe(&g.Uint32Arena, neighborSlabSize, g.Allocator)
				ref, err := g.Uint32Arena.AllocSlice(ChunkSize * MaxNeighbors)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.Neighbors[l][cID], ref.Offset)
			}
		}

		if atomic.LoadUint64(&g.Counts[l][cID]) == 0 {
			countSlabSize := ChunkSize*4 + 64
			if countSlabSize < 1024*1024 {
				countSlabSize = 1024 * 1024
			}
			initArenaSafe(&g.Int32Arena, countSlabSize, g.Allocator)
			ref, err := g.Int32Arena.AllocSlice(ChunkSize)
			if err != nil {
				return err
			}
			atomic.StoreUint64(&g.Counts[l][cID], ref.Offset)
		}

		if atomic.LoadUint64(&g.Versions[l][cID]) == 0 {
			versionSlabSize := ChunkSize*4 + 64
			if versionSlabSize < 1024*1024 {
				versionSlabSize = 1024 * 1024
			}
			initArenaSafe(&g.Uint32Arena, versionSlabSize, g.Allocator)
			ref, err := g.Uint32Arena.AllocSlice(ChunkSize)
			if err != nil {
				return err
			}
			atomic.StoreUint64(&g.Versions[l][cID], ref.Offset)
		}
	}

	// Ensure Float64 - use arena for off-heap allocation
	if !g.SharedVectorSpace && g.Type == VectorTypeFloat64 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeFloat64)
		if paddedDims > 0 {
			if cID < len(g.VectorsFloat64Offsets) && atomic.LoadUint64(&g.VectorsFloat64Offsets[cID]) == 0 {

				slabSize := ChunkSize*paddedDims*8 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				initArenaSafe(&g.Float64Arena, slabSize, g.Allocator)

				ref, err := g.Float64Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsFloat64Offsets[cID], ref.Offset)
			}
		}
	}

	// Ensure Complex64 - use arena for off-heap allocation
	if !g.SharedVectorSpace && g.Type == VectorTypeComplex64 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeComplex64)
		if paddedDims > 0 {
			if cID < len(g.VectorsComplex64Offsets) && atomic.LoadUint64(&g.VectorsComplex64Offsets[cID]) == 0 {

				slabSize := ChunkSize*paddedDims*8 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				initArenaSafe(&g.Complex64Arena, slabSize, g.Allocator)

				ref, err := g.Complex64Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsComplex64Offsets[cID], ref.Offset)
			}
		}
	}

	// Ensure Complex128 - use arena for off-heap allocation
	if !g.SharedVectorSpace && g.Type == VectorTypeComplex128 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeComplex128)
		if paddedDims > 0 {
			if cID < len(g.VectorsComplex128Offsets) && atomic.LoadUint64(&g.VectorsComplex128Offsets[cID]) == 0 {

				slabSize := ChunkSize*paddedDims*16 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				initArenaSafe(&g.Complex128Arena, slabSize, g.Allocator)

				ref, err := g.Complex128Arena.AllocSliceAligned(ChunkSize*paddedDims, 64)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsComplex128Offsets[cID], ref.Offset)
			}
		}
	}

	// Ensure Int64 - use arena for off-heap allocation
	if g.Type == VectorTypeInt64 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeInt64)
		if paddedDims > 0 {
			if cID < len(g.VectorsInt64) && atomic.LoadUint64(&g.VectorsInt64[cID]) == 0 {

				slabSize := ChunkSize*paddedDims*8 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				initArenaSafe(&g.Int64Arena, slabSize, g.Allocator)

				ref, err := g.Int64Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsInt64[cID], ref.Offset)
			}
		}
	}

	// Ensure Uint64 - use arena for off-heap allocation
	if g.Type == VectorTypeUint64 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeUint64)
		if paddedDims > 0 {
			if cID < len(g.VectorsUint64) && atomic.LoadUint64(&g.VectorsUint64[cID]) == 0 {

				slabSize := ChunkSize*paddedDims*8 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				initArenaSafe(&g.Uint64Arena, slabSize, g.Allocator)

				ref, err := g.Uint64Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsUint64[cID], ref.Offset)
			}
		}
	}

	// Ensure Int32
	if g.Type == VectorTypeInt32 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeInt32)
		if paddedDims > 0 {
			if cID < len(g.VectorsInt32) && atomic.LoadUint64(&g.VectorsInt32[cID]) == 0 {

				slabSize := ChunkSize*paddedDims*4 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				initArenaSafe(&g.Int32Arena, slabSize, g.Allocator)

				ref, err := g.Int32Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsInt32[cID], ref.Offset)
			}
		}
	}

	// Ensure Uint32
	if g.Type == VectorTypeUint32 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeUint32)
		if paddedDims > 0 {
			if cID < len(g.VectorsUint32) && atomic.LoadUint64(&g.VectorsUint32[cID]) == 0 {

				slabSize := ChunkSize*paddedDims*4 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				initArenaSafe(&g.Uint32Arena, slabSize, g.Allocator)

				ref, err := g.Uint32Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsUint32[cID], ref.Offset)
			}
		}
	}

	// Ensure Int16 - use arena for off-heap allocation
	if g.Type == VectorTypeInt16 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeInt16)
		if paddedDims > 0 {
			if cID < len(g.VectorsInt16) && atomic.LoadUint64(&g.VectorsInt16[cID]) == 0 {

				slabSize := ChunkSize*paddedDims*2 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				initArenaSafe(&g.Int16Arena, slabSize, g.Allocator)

				ref, err := g.Int16Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsInt16[cID], ref.Offset)
			}
		}
	}

	// Ensure TQ if enabled
	if g.TurboQuantEnabled {
		stride := g.PackedSize()
		if stride > 0 {
			for len(g.VectorsTQ) <= cID {

				slabSize := ChunkSize*stride + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				initArenaSafe(&g.Uint8Arena, slabSize, g.Allocator)

				ref, err := g.Uint8Arena.AllocSlice(ChunkSize * stride)
				if err != nil {
					return err
				}
				g.VectorsTQ = append(g.VectorsTQ, ref.Offset)
			}
		}
	}

	// Ensure Uint16 - use arena for off-heap allocation
	if g.Type == VectorTypeUint16 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeUint16)
		if paddedDims > 0 {
			if cID < len(g.VectorsUint16) && atomic.LoadUint64(&g.VectorsUint16[cID]) == 0 {

				slabSize := ChunkSize*paddedDims*2 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				initArenaSafe(&g.Uint16Arena, slabSize, g.Allocator)

				ref, err := g.Uint16Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsUint16[cID], ref.Offset)
			}
		}
	}

	// Ensure Int8/Uint8
	if g.Type == VectorTypeInt8 || g.Type == VectorTypeUint8 {
		paddedDims := g.GetPaddedDimsForType(g.Type)
		if paddedDims > 0 {
			if cID < len(g.VectorsInt8) && atomic.LoadUint64(&g.VectorsInt8[cID]) == 0 {

				slabSize := ChunkSize*paddedDims + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				initArenaSafe(&g.Int8Arena, slabSize, g.Allocator)

				ref, err := g.Int8Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsInt8[cID], ref.Offset)
			}
		}
	}

	// Ensure BQ if enabled
	if g.BQEnabled {
		paddedDims := (dims + 63) & ^63
		numWords := paddedDims / 64
		if cID < len(g.VectorsBQ) && atomic.LoadUint64(&g.VectorsBQ[cID]) == 0 && numWords > 0 {

			slabSize := ChunkSize*numWords*8 + 64
			if slabSize < 1024*1024 {
				slabSize = 1024 * 1024
			}
			initArenaSafe(&g.Uint64Arena, slabSize, g.Allocator)

			ref, err := g.Uint64Arena.AllocSlice(ChunkSize * numWords)
			if err != nil {
				return err
			}
			atomic.StoreUint64(&g.VectorsBQ[cID], ref.Offset)
		}
	}

	// Ensure F16
	if g.Type == VectorTypeFloat16 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeFloat16)
		if paddedDims > 0 {
			if cID < len(g.VectorsF16) && atomic.LoadUint64(&g.VectorsF16[cID]) == 0 {

				slabSize := ChunkSize*paddedDims*2 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				initArenaSafe(&g.Float16Arena, slabSize, g.Allocator)

				ref, err := g.Float16Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsF16[cID], ref.Offset)
			}
		}
	}

	return nil
}

func (g *GraphData) ensureLayerNeighborsChunk(layer, cID int) error {
	if layer < 0 || layer >= len(g.Neighbors) {
		return fmt.Errorf("layer %d out of range", layer)
	}
	if cID < 0 || cID >= len(g.Neighbors[layer]) {
		return fmt.Errorf("chunk %d out of range for layer %d", cID, layer)
	}
	if atomic.LoadUint64(&g.Neighbors[layer][cID]) != 0 {
		return nil
	}
	neighborSlabSize := ChunkSize*MaxNeighbors*4 + 64
	if neighborSlabSize < 1024*1024 {
		neighborSlabSize = 1024 * 1024
	}
	initArenaSafe(&g.Uint32Arena, neighborSlabSize, g.Allocator)
	ref, err := g.Uint32Arena.AllocSlice(ChunkSize * MaxNeighbors)
	if err != nil {
		return err
	}
	atomic.StoreUint64(&g.Neighbors[layer][cID], ref.Offset)
	return nil
}

func (g *GraphData) GrowMetadataSlices(numChunks int) {
	if numChunks <= 0 {
		return
	}

	// 1. Topology Chunks (Layered)
	if len(g.Neighbors) == 0 {
		const ArrowMaxLayers = 16 // Consistent with types.go
		g.Neighbors = make([][]uint64, ArrowMaxLayers)
		g.Counts = make([][]uint64, ArrowMaxLayers)
		g.Versions = make([][]uint64, ArrowMaxLayers)
	}

	for l := range g.Neighbors {
		if len(g.Neighbors[l]) < numChunks {
			newN := make([]uint64, numChunks)
			copy(newN, g.Neighbors[l])
			g.Neighbors[l] = newN
		}
		if len(g.Counts[l]) < numChunks {
			newC := make([]uint64, numChunks)
			copy(newC, g.Counts[l])
			g.Counts[l] = newC
		}
		if len(g.Versions[l]) < numChunks {
			newV := make([]uint64, numChunks)
			copy(newV, g.Versions[l])
			g.Versions[l] = newV
		}
	}

	// 2. Levels
	if len(g.Levels) < numChunks {
		newL := make([][]uint32, numChunks)
		copy(newL, g.Levels)
		for i := len(g.Levels); i < numChunks; i++ {
			newL[i] = make([]uint32, ChunkSize)
		}
		g.Levels = newL
	}

	// 3. Vector arrays
	growOffsetSlice := func(src []uint64) []uint64 {
		if len(src) >= numChunks {
			return src
		}
		newS := make([]uint64, numChunks)
		copy(newS, src)
		return newS
	}

	if !g.SharedVectorSpace {
		if g.Type == VectorTypeFloat32 || g.Type == VectorTypeUnknown {
			g.VectorsF32 = growOffsetSlice(g.VectorsF32)
		}
		if g.SQ8Enabled {
			g.VectorsSQ8 = growOffsetSlice(g.VectorsSQ8)
		}
		if g.PQEnabled {
			g.VectorsPQ = growOffsetSlice(g.VectorsPQ)
		}
		if g.BQEnabled {
			g.VectorsBQ = growOffsetSlice(g.VectorsBQ)
		}
		if g.TurboQuantEnabled {
			g.VectorsTQ = growOffsetSlice(g.VectorsTQ)
		}
		if g.Type == VectorTypeFloat16 {
			g.VectorsF16 = growOffsetSlice(g.VectorsF16)
		}
		if g.Type == VectorTypeInt8 || g.Type == VectorTypeUint8 {
			g.VectorsInt8 = growOffsetSlice(g.VectorsInt8)
		}
		if g.Type == VectorTypeInt16 {
			g.VectorsInt16 = growOffsetSlice(g.VectorsInt16)
		}
		if g.Type == VectorTypeUint16 {
			g.VectorsUint16 = growOffsetSlice(g.VectorsUint16)
		}
		if g.Type == VectorTypeInt32 {
			g.VectorsInt32 = growOffsetSlice(g.VectorsInt32)
		}
		if g.Type == VectorTypeUint32 {
			g.VectorsUint32 = growOffsetSlice(g.VectorsUint32)
		}
		if g.Type == VectorTypeInt64 {
			g.VectorsInt64 = growOffsetSlice(g.VectorsInt64)
		}
		if g.Type == VectorTypeUint64 {
			g.VectorsUint64 = growOffsetSlice(g.VectorsUint64)
		}
		if g.Type == VectorTypeFloat64 {
			g.VectorsFloat64Offsets = growOffsetSlice(g.VectorsFloat64Offsets)
		}
		if g.Type == VectorTypeComplex64 {
			g.VectorsComplex64Offsets = growOffsetSlice(g.VectorsComplex64Offsets)
		}
		if g.Type == VectorTypeComplex128 {
			g.VectorsComplex128Offsets = growOffsetSlice(g.VectorsComplex128Offsets)
		}
	}

	// Allocate Complex128Magnitudes flat array indexed by global id.
	// Size: ChunkSize * numChunks float64 values.
	if g.Type == VectorTypeComplex128 {
		needed := numChunks * ChunkSize
		if len(g.Complex128Magnitudes) < needed {
			newM := make([]float64, needed)
			copy(newM, g.Complex128Magnitudes)
			g.Complex128Magnitudes = newM
		}
	}

	if len(g.Vectors) < numChunks {
		newV := make([][]float32, numChunks)
		copy(newV, g.Vectors)
		g.Vectors = newV
	}
	if len(g.VectorsFloat64) < numChunks {
		newV := make([][]float64, numChunks)
		copy(newV, g.VectorsFloat64)
		g.VectorsFloat64 = newV
	}
	if len(g.VectorsComplex64) < numChunks {
		newV := make([][]complex64, numChunks)
		copy(newV, g.VectorsComplex64)
		g.VectorsComplex64 = newV
	}
	if len(g.VectorsComplex128) < numChunks {
		newV := make([][]complex128, numChunks)
		copy(newV, g.VectorsComplex128)
		g.VectorsComplex128 = newV
	}
}
