package types

import (
	"github.com/23skdu/longbow/internal/memory"
	"sync/atomic"
)

func (g *GraphData) PreAllocate(capacity int) error {
	if capacity <= 0 || g.Dims <= 0 {
		return nil
	}

	numChunks := (capacity + ChunkSize - 1) / ChunkSize
	if numChunks <= 0 {
		numChunks = 1
	}
	g.GrowMetadataSlices(numChunks)

	// Helper helper function to calculate safe power-of-2 slab size capped at 64MB
	getSafeSlabSize := func(requiredSize int) int {
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if slabSize > 64*1024*1024 {
			slabSize = 64 * 1024 * 1024
		}
		return slabSize
	}

	// Pre-allocate Float32 arena chunks
	if !g.SharedVectorSpace && (g.Type == VectorTypeFloat32 || g.Type == VectorTypeUnknown) {
		paddedDims := g.GetPaddedDimsForType(VectorTypeFloat32)
		requiredSize := numChunks * ChunkSize * paddedDims * 4
		slabSize := getSafeSlabSize(requiredSize)

		initArenaSafe(&g.Float32Arena, slabSize, g.Allocator)

		// Pre-allocate all chunks
		for i := 0; i < numChunks; i++ {
			if atomic.LoadUint64(&g.VectorsF32[i]) == 0 {
				ref, err := g.Float32Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsF32[i], ref.Offset)
			}
		}
	}

	// Pre-allocate Float64 arena chunks
	if !g.SharedVectorSpace && g.Type == VectorTypeFloat64 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeFloat64)
		requiredSize := numChunks * ChunkSize * paddedDims * 8
		slabSize := getSafeSlabSize(requiredSize)

		initArenaSafe(&g.Float64Arena, slabSize, g.Allocator)

		for i := 0; i < numChunks; i++ {
			if atomic.LoadUint64(&g.VectorsFloat64Offsets[i]) == 0 {
				ref, err := g.Float64Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsFloat64Offsets[i], ref.Offset)
			}
		}
	}

	// Pre-allocate TurboQuant arena chunks
	if g.TurboQuantEnabled {
		stride := g.PackedSize()
		requiredSize := numChunks * ChunkSize * stride
		slabSize := getSafeSlabSize(requiredSize)

		initArenaSafe(&g.Uint8Arena, slabSize, g.Allocator)

		for i := 0; i < numChunks; i++ {
			if atomic.LoadUint64(&g.VectorsTQ[i]) == 0 {
				ref, err := g.Uint8Arena.AllocSliceDirty(ChunkSize * stride)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsTQ[i], ref.Offset)
			}
		}
	}

	// Pre-allocate Complex64 arena chunks
	if g.Type == VectorTypeComplex64 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeComplex64)
		requiredSize := numChunks * ChunkSize * paddedDims * 8
		slabSize := getSafeSlabSize(requiredSize)

		initArenaSafe(&g.Complex64Arena, slabSize, g.Allocator)

		for i := 0; i < numChunks; i++ {
			if atomic.LoadUint64(&g.VectorsComplex64Offsets[i]) == 0 {
				ref, err := g.Complex64Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsComplex64Offsets[i], ref.Offset)
			}
		}
	}

	// Pre-allocate Complex128 arena chunks
	if g.Type == VectorTypeComplex128 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeComplex128)
		requiredSize := numChunks * ChunkSize * paddedDims * 16
		slabSize := getSafeSlabSize(requiredSize)

		initArenaSafe(&g.Complex128Arena, slabSize, g.Allocator)

		for i := 0; i < numChunks; i++ {
			if atomic.LoadUint64(&g.VectorsComplex128Offsets[i]) == 0 {
				ref, err := g.Complex128Arena.AllocSliceAligned(ChunkSize*paddedDims, 64)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsComplex128Offsets[i], ref.Offset)
			}
		}
	}

	// Pre-allocate SQ8 arena chunks
	if g.SQ8Enabled {
		paddedDims := (g.Dims + 63) & ^63
		requiredSize := numChunks * ChunkSize * paddedDims
		slabSize := getSafeSlabSize(requiredSize)

		initArenaSafe(&g.Uint8Arena, slabSize, g.Allocator)

		for i := 0; i < numChunks; i++ {
			if atomic.LoadUint64(&g.VectorsSQ8[i]) == 0 {
				ref, err := g.Uint8Arena.AllocSliceDirty(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsSQ8[i], ref.Offset)
			}
		}
	}

	// Pre-allocate PQ arena chunks
	if g.PQEnabled && g.PQM > 0 {
		numWordsPerNode := (g.PQM + 7) / 8
		numWords := ChunkSize * numWordsPerNode
		requiredSize := numChunks * numWords * 8
		slabSize := getSafeSlabSize(requiredSize)

		initArenaSafe(&g.Uint64Arena, slabSize, g.Allocator)

		for i := 0; i < numChunks; i++ {
			if atomic.LoadUint64(&g.VectorsPQ[i]) == 0 {
				ref, err := g.Uint64Arena.AllocSliceDirty(numWords)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsPQ[i], ref.Offset)
			}
		}
	}

	// Pre-allocate BQ arena chunks
	if g.BQEnabled {
		paddedDims := (g.Dims + 63) & ^63
		numWordsPerNode := paddedDims / 64
		numWords := ChunkSize * numWordsPerNode
		requiredSize := numChunks * numWords * 8
		slabSize := getSafeSlabSize(requiredSize)

		initArenaSafe(&g.Uint64Arena, slabSize, g.Allocator)

		for i := 0; i < numChunks; i++ {
			if atomic.LoadUint64(&g.VectorsBQ[i]) == 0 {
				ref, err := g.Uint64Arena.AllocSliceDirty(numWords)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsBQ[i], ref.Offset)
			}
		}
	}

	// Pre-allocate Int8 arena chunks
	if g.Type == VectorTypeInt8 || g.Type == VectorTypeUint8 {
		paddedDims := g.GetPaddedDimsForType(g.Type)
		requiredSize := numChunks * ChunkSize * paddedDims
		slabSize := getSafeSlabSize(requiredSize)

		initArenaSafe(&g.Int8Arena, slabSize, g.Allocator)

		for i := 0; i < numChunks; i++ {
			if atomic.LoadUint64(&g.VectorsInt8[i]) == 0 {
				ref, err := g.Int8Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsInt8[i], ref.Offset)
			}
		}
	}

	// Pre-allocate Int64 arena chunks
	if g.Type == VectorTypeInt64 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeInt64)
		requiredSize := numChunks * ChunkSize * paddedDims * 8
		slabSize := getSafeSlabSize(requiredSize)

		initArenaSafe(&g.Int64Arena, slabSize, g.Allocator)

		for i := 0; i < numChunks; i++ {
			if atomic.LoadUint64(&g.VectorsInt64[i]) == 0 {
				ref, err := g.Int64Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsInt64[i], ref.Offset)
			}
		}
	}

	// Pre-allocate Uint64 arena chunks
	if g.Type == VectorTypeUint64 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeUint64)
		requiredSize := numChunks * ChunkSize * paddedDims * 8
		slabSize := getSafeSlabSize(requiredSize)

		initArenaSafe(&g.Uint64Arena, slabSize, g.Allocator)

		for i := 0; i < numChunks; i++ {
			if atomic.LoadUint64(&g.VectorsUint64[i]) == 0 {
				ref, err := g.Uint64Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsUint64[i], ref.Offset)
			}
		}
	}

	// Pre-allocate Int32 arena chunks
	if g.Type == VectorTypeInt32 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeInt32)
		requiredSize := numChunks * ChunkSize * paddedDims * 4
		slabSize := getSafeSlabSize(requiredSize)

		initArenaSafe(&g.Int32Arena, slabSize, g.Allocator)

		for i := 0; i < numChunks; i++ {
			if atomic.LoadUint64(&g.VectorsInt32[i]) == 0 {
				ref, err := g.Int32Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsInt32[i], ref.Offset)
			}
		}
	}

	// Pre-allocate Uint32 arena chunks
	if g.Type == VectorTypeUint32 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeUint32)
		requiredSize := numChunks * ChunkSize * paddedDims * 4
		slabSize := getSafeSlabSize(requiredSize)

		initArenaSafe(&g.Uint32Arena, slabSize, g.Allocator)

		for i := 0; i < numChunks; i++ {
			if atomic.LoadUint64(&g.VectorsUint32[i]) == 0 {
				ref, err := g.Uint32Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsUint32[i], ref.Offset)
			}
		}
	}

	// Pre-allocate Int16 arena chunks
	if g.Type == VectorTypeInt16 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeInt16)
		requiredSize := numChunks * ChunkSize * paddedDims * 2
		slabSize := getSafeSlabSize(requiredSize)

		initArenaSafe(&g.Int16Arena, slabSize, g.Allocator)

		for i := 0; i < numChunks; i++ {
			if atomic.LoadUint64(&g.VectorsInt16[i]) == 0 {
				ref, err := g.Int16Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsInt16[i], ref.Offset)
			}
		}
	}

	// Pre-allocate Uint16 arena chunks
	if g.Type == VectorTypeUint16 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeUint16)
		requiredSize := numChunks * ChunkSize * paddedDims * 2
		slabSize := getSafeSlabSize(requiredSize)

		initArenaSafe(&g.Uint16Arena, slabSize, g.Allocator)

		for i := 0; i < numChunks; i++ {
			if atomic.LoadUint64(&g.VectorsUint16[i]) == 0 {
				ref, err := g.Uint16Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.VectorsUint16[i], ref.Offset)
			}
		}
	}

	// Pre-allocate Float16 arena chunks
	if g.Type == VectorTypeFloat16 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeFloat16)
		requiredSize := numChunks * ChunkSize * paddedDims * 2
		slabSize := getSafeSlabSize(requiredSize)

		initArenaSafe(&g.Float16Arena, slabSize, g.Allocator)

		for i := len(g.VectorsF16); i < numChunks; i++ {
			ref, err := g.Float16Arena.AllocSlice(ChunkSize * paddedDims)
			if err != nil {
				return err
			}
			g.VectorsF16 = append(g.VectorsF16, ref.Offset)
		}
	}

	// Pre-allocate Levels for all chunks
	if len(g.Levels) < numChunks {
		for i := len(g.Levels); i < numChunks; i++ {
			g.Levels = append(g.Levels, make([]uint32, ChunkSize))
		}
	}

	// Pre-allocate Neighbors, Counts, Versions for all layers
	if !g.SharedVectorSpace && len(g.VectorsF32) < numChunks {
		for i := len(g.VectorsF32); i < numChunks; i++ {
			g.VectorsF32 = append(g.VectorsF32, 0)
			if len(g.Vectors) < numChunks {
				g.Vectors = append(g.Vectors, nil)
			}
		}
	}

	if len(g.Neighbors) == 0 {
		g.Neighbors = make([][]uint64, ArrowMaxLayers)
		g.Counts = make([][]uint64, ArrowMaxLayers)
		g.Versions = make([][]uint64, ArrowMaxLayers)
	}
	// Expand offset slices for all layers (needed for indexing) but defer actual
	// arena allocation to EnsureChunk. This avoids pre-allocating ~978 MB of old-style
	// neighbor storage at layer 0 when PackedNeighbors handles the hot path.
	for l := 0; l < ArrowMaxLayers; l++ {
		if len(g.Neighbors[l]) < numChunks {
			delta := numChunks - len(g.Neighbors[l])
			g.Neighbors[l] = append(g.Neighbors[l], make([]uint64, delta)...)
			g.Counts[l] = append(g.Counts[l], make([]uint64, delta)...)
			g.Versions[l] = append(g.Versions[l], make([]uint64, delta)...)
		}
	}

	g.Capacity = capacity

	return nil
}

// NewGraphData creates a new GraphData instance.

// This is a helper for legacy tests.
// NewGraphData creates a new GraphData instance.

func (g *GraphData) RelocateToOffHeap() error {
	alloc := memory.NewOffHeapAllocator()

	// 1. Relocate Slab Arenas
	arenas := []*memory.SlabArena{}
	if g.Float32Arena != nil {
		arenas = append(arenas, g.Float32Arena.Slab())
	}
	if g.Float64Arena != nil {
		arenas = append(arenas, g.Float64Arena.Slab())
	}
	if g.Uint8Arena != nil {
		arenas = append(arenas, g.Uint8Arena.Slab())
	}
	if g.Uint16Arena != nil {
		arenas = append(arenas, g.Uint16Arena.Slab())
	}
	if g.Uint32Arena != nil {
		arenas = append(arenas, g.Uint32Arena.Slab())
	}
	if g.Uint64Arena != nil {
		arenas = append(arenas, g.Uint64Arena.Slab())
	}
	if g.Int8Arena != nil {
		arenas = append(arenas, g.Int8Arena.Slab())
	}
	if g.Int16Arena != nil {
		arenas = append(arenas, g.Int16Arena.Slab())
	}
	if g.Int32Arena != nil {
		arenas = append(arenas, g.Int32Arena.Slab())
	}
	if g.Int64Arena != nil {
		arenas = append(arenas, g.Int64Arena.Slab())
	}
	if g.Float16Arena != nil {
		arenas = append(arenas, g.Float16Arena.Slab())
	}
	if g.Complex64Arena != nil {
		arenas = append(arenas, g.Complex64Arena.Slab())
	}
	if g.Complex128Arena != nil {
		arenas = append(arenas, g.Complex128Arena.Slab())
	}

	for _, a := range arenas {
		if err := a.ConvertToOffHeap(alloc); err != nil {
			return err
		}
	}

	// 2. Relocate PackedAdjacency Chunks
	for _, pa := range g.PackedNeighbors {
		if pa == nil {
			continue
		}
		if adj, ok := pa.(interface {
			RelocateToOffHeap(*memory.OffHeapAllocator)
		}); ok {
			adj.RelocateToOffHeap(alloc)
		}
	}

	return nil
}
