package types

import (
	"fmt"
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// GraphData holds the vector data and graph topology.
// It effectively implements the component storage for ArrowHNSW.
type GraphData struct {
	// Metadata
	Capacity      int
	Dims          int
	Type          VectorDataType
	SQ8Enabled    bool
	SQ8Ready      uint32 // 0=not ready, 1=ready
	BQEnabled     bool
	PQEnabled     bool
	PQM           int
	GlobalVersion uint64 // For cache validation
	BackingGraph  any    // interface{} to avoid import cycle (likely *DiskGraph)

	// Vectors (primary storage, usually float32)
	Vectors [][]float32

	// VectorsF32 stores arena offsets for Float32 vectors (off-heap, GC-free)
	VectorsF32 []uint64

	// VectorsPQ for quantized vectors
	VectorsPQ []uint64

	// VectorsInt8 for raw int8 vectors
	VectorsInt8 []uint64

	// VectorsInt16 for raw int16 vectors (off-heap, GC-free)
	VectorsInt16 []uint64

	// VectorsUint16 for raw uint16 vectors (off-heap, GC-free)
	VectorsUint16 []uint64

	// VectorsF16 for half-precision
	VectorsF16 []uint64

	// VectorsBQ for binary quantized vectors
	VectorsBQ []uint64

	// VectorsSQ8 for scalar quantized vectors
	VectorsSQ8 []uint64

	// VectorsTQ for TurboQuant compressed vectors
	VectorsTQ []uint64

	// VectorsFloat64
	VectorsFloat64 [][]float64

	// VectorsComplex64
	VectorsComplex64 [][]complex64

	// VectorsComplex128
	VectorsComplex128 [][]complex128

	// VectorsInt64 stores arena offsets for Int64 vectors (off-heap, GC-free)
	VectorsInt64 []uint64

	// VectorsUint64 stores arena offsets for Uint64 vectors (off-heap, GC-free)
	VectorsUint64 []uint64

	// VectorsInt32 stores arena offsets for Int32 vectors (off-heap, GC-free)
	VectorsInt32 []uint64

	// VectorsUint32 stores arena offsets for Uint32 vectors (off-heap, GC-free)
	VectorsUint32 []uint64

	// VectorsFloat64Offsets stores arena offsets for Float64 vectors (off-heap, GC-free)
	VectorsFloat64Offsets []uint64

	// VectorsComplex64Offsets stores arena offsets for Complex64 vectors (off-heap, GC-free)
	VectorsComplex64Offsets []uint64

	// VectorsComplex128Offsets stores arena offsets for Complex128 vectors (off-heap, GC-free)
	VectorsComplex128Offsets []uint64

	// Neighbors (Layer -> Chunk -> Data)
	Neighbors [][][]uint32

	// Levels (Chunk -> Data)
	Levels [][]uint8

	// Versions (Layer -> Chunk -> Data)
	Versions [][][]uint32

	// Counts (Layer -> Chunk -> Data)
	Counts [][][]int32

	// Memory Arenas
	Float32Arena    *memory.TypedArena[float32]
	Float64Arena    *memory.TypedArena[float64]
	Uint8Arena      *memory.TypedArena[uint8]
	Uint16Arena     *memory.TypedArena[uint16]
	Uint32Arena     *memory.TypedArena[uint32]
	Uint64Arena     *memory.TypedArena[uint64]
	Int8Arena       *memory.TypedArena[int8]
	Int16Arena      *memory.TypedArena[int16]
	Int32Arena      *memory.TypedArena[int32]
	Int64Arena      *memory.TypedArena[int64]
	Float16Arena    *memory.TypedArena[float16.Num]
	Complex64Arena  *memory.TypedArena[complex64]
	Complex128Arena *memory.TypedArena[complex128]

	// PackedNeighbors
	PackedNeighbors []PackedNeighbors

	TurboQuantEnabled bool
	TurboQuantBits    int
}

type graphFallback interface {
	GetNeighbors(layer int, id uint32, buf []uint32) []uint32
	GetVector(id uint32) (any, error)
}

// PackedNeighbors interface for graph adjacency management
type PackedNeighbors interface {
	GetNeighbors(id uint32) ([]uint32, bool)
	SetNeighbors(id uint32, neighbors []uint32) error
	GetNeighborsF16(id uint32) ([]uint32, []float16.Num, bool)
	SetNeighborsF16(id uint32, neighbors []uint32, dists []float16.Num) error
}

// GetNodeCount returns the current capacity of the graph (number of addressable nodes).
func (g *GraphData) GetNodeCount() int {
	return g.Capacity
}

// NeedsChunk returns true if the given chunk ID requires allocation.
func (g *GraphData) NeedsChunk(cID int) bool {
	if cID >= len(g.Levels) {
		return true
	}
	if g.Levels[cID] == nil {
		return true
	}
	return false
}

// GetVectorsChunk returns the vector chunk for the given ID.
func (g *GraphData) GetVectorsChunk(chunkID int) []float32 {
	// Try arena first (off-heap, GC-free)
	if g.Float32Arena != nil && chunkID < len(g.VectorsF32) {
		return g.Float32Arena.Get(memory.SliceRef{Offset: g.VectorsF32[chunkID], Len: uint32(ChunkSize * g.Dims), Cap: uint32(ChunkSize * g.Dims)})
	}
	// Fallback to legacy slice
	if chunkID < len(g.Vectors) {
		return g.Vectors[chunkID]
	}
	return nil
}

func (g *GraphData) PackedSize() int {
	if g.Dims <= 0 {
		return 0
	}
	p2 := int(1 << uint(math.Ceil(math.Log2(float64(g.Dims)))))
	angleBytes := ((p2-1)*g.TurboQuantBits + 7) / 8
	bitBytes := (p2 + 7) / 8
	return 4 + angleBytes + bitBytes
}

func (g *GraphData) GetVectorsTQChunk(chunkID int) []byte {
	if chunkID < len(g.VectorsTQ) && g.Uint8Arena != nil {
		stride := g.PackedSize()
		return g.Uint8Arena.Get(memory.SliceRef{Offset: g.VectorsTQ[chunkID], Len: uint32(ChunkSize * stride), Cap: uint32(ChunkSize * stride)})
	}
	return nil
}

func (g *GraphData) GetVectorsFloat64Chunk(chunkID int) []float64 {
	if chunkID < len(g.VectorsFloat64Offsets) && g.Float64Arena != nil {
		return g.Float64Arena.Get(memory.SliceRef{Offset: g.VectorsFloat64Offsets[chunkID], Len: uint32(ChunkSize * g.Dims), Cap: uint32(ChunkSize * g.Dims)})
	}
	if chunkID < len(g.VectorsFloat64) {
		return g.VectorsFloat64[chunkID]
	}
	return nil
}

func (g *GraphData) GetVectorsComplex64Chunk(chunkID int) []complex64 {
	if chunkID < len(g.VectorsComplex64Offsets) && g.Complex64Arena != nil {
		return g.Complex64Arena.Get(memory.SliceRef{Offset: g.VectorsComplex64Offsets[chunkID], Len: uint32(ChunkSize * g.Dims), Cap: uint32(ChunkSize * g.Dims)})
	}
	if chunkID < len(g.VectorsComplex64) {
		return g.VectorsComplex64[chunkID]
	}
	return nil
}

func (g *GraphData) GetVectorsComplex128Chunk(chunkID int) []complex128 {
	if chunkID < len(g.VectorsComplex128Offsets) && g.Complex128Arena != nil {
		return g.Complex128Arena.Get(memory.SliceRef{Offset: g.VectorsComplex128Offsets[chunkID], Len: uint32(ChunkSize * g.Dims), Cap: uint32(ChunkSize * g.Dims)})
	}
	if chunkID < len(g.VectorsComplex128) {
		return g.VectorsComplex128[chunkID]
	}
	return nil
}

func (g *GraphData) GetVectorsInt64Chunk(chunkID int) []int64 {
	if chunkID < len(g.VectorsInt64) && g.Int64Arena != nil {
		return g.Int64Arena.Get(memory.SliceRef{Offset: g.VectorsInt64[chunkID], Len: uint32(ChunkSize * g.Dims), Cap: uint32(ChunkSize * g.Dims)})
	}
	return nil
}

func (g *GraphData) GetVectorsUint64Chunk(chunkID int) []uint64 {
	if chunkID < len(g.VectorsUint64) && g.Uint64Arena != nil {
		return g.Uint64Arena.Get(memory.SliceRef{Offset: g.VectorsUint64[chunkID], Len: uint32(ChunkSize * g.Dims), Cap: uint32(ChunkSize * g.Dims)})
	}
	return nil
}

func (g *GraphData) GetVectorsInt32Chunk(chunkID int) []int32 {
	if chunkID < len(g.VectorsInt32) && g.Int32Arena != nil {
		return g.Int32Arena.Get(memory.SliceRef{Offset: g.VectorsInt32[chunkID], Len: uint32(ChunkSize * g.Dims), Cap: uint32(ChunkSize * g.Dims)})
	}
	return nil
}

func (g *GraphData) GetVectorsUint32Chunk(chunkID int) []uint32 {
	if chunkID < len(g.VectorsUint32) && g.Uint32Arena != nil {
		return g.Uint32Arena.Get(memory.SliceRef{Offset: g.VectorsUint32[chunkID], Len: uint32(ChunkSize * g.Dims), Cap: uint32(ChunkSize * g.Dims)})
	}
	return nil
}

func (g *GraphData) GetPaddedDims() int {
	return g.GetPaddedDimsForType(g.Type)
}

func (g *GraphData) GetPaddedDimsForType(dt VectorDataType) int {
	switch dt {
	case VectorTypeFloat32, VectorTypeInt32, VectorTypeUint32:
		// 4 bytes per element. Cache line = 64 bytes = 16 elements.
		return (g.Dims + 15) & ^15
	case VectorTypeInt8, VectorTypeUint8:
		// 1 byte per element. Cache line = 64 bytes = 64 elements.
		return (g.Dims + 63) & ^63
	case VectorTypeFloat16, VectorTypeInt16, VectorTypeUint16:
		// 2 bytes per element. Cache line = 64 bytes = 32 elements.
		return (g.Dims + 31) & ^31
	case VectorTypeFloat64, VectorTypeInt64, VectorTypeUint64:
		// 8 bytes per element. Cache line = 64 bytes = 8 elements.
		return (g.Dims + 7) & ^7
	case VectorTypeComplex64:
		// 8 bytes per element (2x float32). Cache line = 64 bytes = 8 elements.
		return (g.Dims + 7) & ^7
	case VectorTypeComplex128:
		// 16 bytes per element (2x float64). Cache line = 64 bytes = 4 elements.
		return (g.Dims + 3) & ^3
	default:
		return g.Dims
	}
}

func (g *GraphData) GetVectorsSQ8Chunk(chunkID int) []byte {
	if chunkID < len(g.VectorsSQ8) && g.Uint8Arena != nil {
		paddedDims := (g.Dims + 63) & ^63
		return g.Uint8Arena.Get(memory.SliceRef{Offset: g.VectorsSQ8[chunkID], Len: uint32(ChunkSize * paddedDims), Cap: uint32(ChunkSize * paddedDims)})
	}
	return nil
}

func (g *GraphData) GetVectorsBQChunk(chunkID int) []uint64 {
	if g.Uint64Arena != nil && chunkID < len(g.VectorsBQ) {
		paddedDims := (g.Dims + 63) & ^63
		numWordsPerNode := paddedDims / 64
		chunkLen := ChunkSize * numWordsPerNode
		if chunkLen == 0 {
			return nil
		}

		return g.Uint64Arena.Get(memory.SliceRef{
			Offset: g.VectorsBQ[chunkID],
			Len:    uint32(chunkLen),
			Cap:    uint32(chunkLen),
		})
	}
	return nil
}

// GetVectorsPQChunk returns the PQ vectors chunk for the given ID.
func (g *GraphData) GetVectorsPQChunk(chunkID int) []byte {
	if g.Uint64Arena != nil && chunkID < len(g.VectorsPQ) && g.PQM > 0 {
		numWordsPerNode := (g.PQM + 7) / 8
		numWords := ChunkSize * numWordsPerNode

		chunk := g.Uint64Arena.Get(memory.SliceRef{
			Offset: g.VectorsPQ[chunkID],
			Len:    uint32(numWords),
			Cap:    uint32(numWords),
		})

		if len(chunk) == 0 {
			return nil
		}

		// Cast uint64 to byte slice
		ptr := unsafe.Pointer(&chunk[0])
		return unsafe.Slice((*byte)(ptr), numWords*8)
	}
	return nil
}

func (g *GraphData) SetVectorPQ(id uint32, code []byte) error {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	if g.Uint64Arena != nil && cID < len(g.VectorsPQ) {
		m := g.PQM
		if len(code) != m {
			return fmt.Errorf("PQ code length mismatch: expected %d, got %d", m, len(code))
		}

		numWordsPerNode := (m + 7) / 8
		numWords := ChunkSize * numWordsPerNode

		chunk := g.Uint64Arena.Get(memory.SliceRef{
			Offset: g.VectorsPQ[cID],
			Len:    uint32(numWords),
			Cap:    uint32(numWords),
		})

		if len(chunk) == 0 {
			return fmt.Errorf("PQ chunk is empty")
		}

		// Cast uint64 to byte slice
		ptr := unsafe.Pointer(&chunk[0])
		byteChunk := unsafe.Slice((*byte)(ptr), numWords*8)

		start := cOff * m
		if start+m <= len(byteChunk) {
			copy(byteChunk[start:start+m], code)
			return nil
		}
	}
	return fmt.Errorf("failed to set PQ vector for id %d", id)
}

func (g *GraphData) GetCountsChunk(layer, chunkID int) []int32 {
	if layer < len(g.Counts) && chunkID < len(g.Counts[layer]) {
		return g.Counts[layer][chunkID]
	}
	return nil
}

func (g *GraphData) GetNeighborsChunk(layer, chunkID int) []uint32 {
	if layer < len(g.Neighbors) && chunkID < len(g.Neighbors[layer]) {
		return g.Neighbors[layer][chunkID]
	}
	return nil
}

func (g *GraphData) GetVersionsChunk(layer, chunkID int) []uint32 {
	if layer < len(g.Versions) && chunkID < len(g.Versions[layer]) {
		return g.Versions[layer][chunkID]
	}
	return nil
}

func (g *GraphData) GetVectorsInt8Chunk(chunkID int) []int8 {
	if chunkID < len(g.VectorsInt8) && g.Int8Arena != nil {
		return g.Int8Arena.Get(memory.SliceRef{Offset: g.VectorsInt8[chunkID], Len: uint32(ChunkSize * g.Dims), Cap: uint32(ChunkSize * g.Dims)})
	}
	return nil
}

func (g *GraphData) GetVectorsInt16Chunk(chunkID int) []int16 {
	if chunkID < len(g.VectorsInt16) && g.Int16Arena != nil {
		return g.Int16Arena.Get(memory.SliceRef{Offset: g.VectorsInt16[chunkID], Len: uint32(ChunkSize * g.Dims), Cap: uint32(ChunkSize * g.Dims)})
	}
	return nil
}

func (g *GraphData) GetVectorsUint16Chunk(chunkID int) []uint16 {
	if chunkID < len(g.VectorsUint16) && g.Uint16Arena != nil {
		return g.Uint16Arena.Get(memory.SliceRef{Offset: g.VectorsUint16[chunkID], Len: uint32(ChunkSize * g.Dims), Cap: uint32(ChunkSize * g.Dims)})
	}
	return nil
}

func (g *GraphData) EnsureChunk(cID, cOff, dims int) error {
	// Ensure Vectors (Float32 is default/primary) - use arena for off-heap allocation
	if g.Type == VectorTypeFloat32 || g.Type == VectorTypeUnknown {
		for len(g.VectorsF32) <= cID {
			// If dims is 0, allocate a placeholder (will be reallocated when actual dims known)
			if dims == 0 {
				g.VectorsF32 = append(g.VectorsF32, 0)
				g.Vectors = append(g.Vectors, nil)
				continue
			}
			// Create arena on first need
			// Create arena if needed - always size for the required chunk + alignment buffer
			requiredChunkSize := ChunkSize * dims * 4
			if g.Float32Arena == nil {
				slabSize := requiredChunkSize + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				g.Float32Arena = memory.NewTypedArena[float32](memory.NewSlabArena(slabSize))
			}
			ref, err := g.Float32Arena.AllocSliceDirty(ChunkSize * dims)
			if err != nil {
				return err
			}
			g.VectorsF32 = append(g.VectorsF32, ref.Offset)
			// Also append nil to Vectors for backward compatibility
			g.Vectors = append(g.Vectors, nil)
		}
	}

	// Ensure Levels
	for len(g.Levels) <= cID {
		g.Levels = append(g.Levels, nil)
	}
	if g.Levels[cID] == nil {
		g.Levels[cID] = make([]uint8, ChunkSize)
	}

	// Ensure Neighbors, Counts, Versions for all layers
	if len(g.Neighbors) == 0 {
		g.Neighbors = make([][][]uint32, ArrowMaxLayers)
		g.Counts = make([][][]int32, ArrowMaxLayers)
		g.Versions = make([][][]uint32, ArrowMaxLayers)
	}
	for l := 0; l < ArrowMaxLayers; l++ {
		// Ensure Neighbors
		for len(g.Neighbors[l]) <= cID {
			g.Neighbors[l] = append(g.Neighbors[l], nil)
		}
		if g.Neighbors[l][cID] == nil {
			g.Neighbors[l][cID] = make([]uint32, ChunkSize*MaxNeighbors)
		}
		// Ensure Counts
		for len(g.Counts[l]) <= cID {
			g.Counts[l] = append(g.Counts[l], nil)
		}
		if g.Counts[l][cID] == nil {
			g.Counts[l][cID] = make([]int32, ChunkSize)
		}
		// Ensure Versions
		for len(g.Versions[l]) <= cID {
			g.Versions[l] = append(g.Versions[l], nil)
		}
		if g.Versions[l][cID] == nil {
			g.Versions[l][cID] = make([]uint32, ChunkSize)
		}
	}

	// Ensure SQ8 if enabled
	if g.SQ8Enabled {
		for len(g.VectorsSQ8) <= cID {
			// Allocate in arena
			if g.Uint8Arena == nil {
				// Create a slab arena with reasonable size (e.g. 1MB or fits 1 chunk)
				// SQ8 requires padding to 64 bytes
				paddedDims := (dims + 63) & ^63
				slabSize := ChunkSize*paddedDims + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				slabArena := memory.NewSlabArena(slabSize)
				g.Uint8Arena = memory.NewTypedArena[uint8](slabArena)
			}
			paddedDims := (dims + 63) & ^63
			ref, err := g.Uint8Arena.AllocSliceDirty(ChunkSize * paddedDims)
			if err != nil {
				return err
			}
			g.VectorsSQ8 = append(g.VectorsSQ8, ref.Offset)
		}
	}

	// Ensure Float64 - use arena for off-heap allocation
	if g.Type == VectorTypeFloat64 {
		for len(g.VectorsFloat64Offsets) <= cID {
			if g.Float64Arena == nil {
				slabSize := ChunkSize*dims*8 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				g.Float64Arena = memory.NewTypedArena[float64](memory.NewSlabArena(slabSize))
			}
			ref, err := g.Float64Arena.AllocSliceDirty(ChunkSize * dims)
			if err != nil {
				return err
			}
			g.VectorsFloat64Offsets = append(g.VectorsFloat64Offsets, ref.Offset)
		}
	}

	// Ensure Complex64 - use arena for off-heap allocation
	if g.Type == VectorTypeComplex64 {
		for len(g.VectorsComplex64Offsets) <= cID {
			if g.Complex64Arena == nil {
				slabSize := ChunkSize*dims*8 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				g.Complex64Arena = memory.NewTypedArena[complex64](memory.NewSlabArena(slabSize))
			}
			ref, err := g.Complex64Arena.AllocSliceDirty(ChunkSize * dims)
			if err != nil {
				return err
			}
			g.VectorsComplex64Offsets = append(g.VectorsComplex64Offsets, ref.Offset)
		}
	}

	// Ensure Complex128 - use arena for off-heap allocation
	if g.Type == VectorTypeComplex128 {
		for len(g.VectorsComplex128Offsets) <= cID {
			if g.Complex128Arena == nil {
				slabSize := ChunkSize*dims*16 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				g.Complex128Arena = memory.NewTypedArena[complex128](memory.NewSlabArena(slabSize))
			}
			ref, err := g.Complex128Arena.AllocSliceDirty(ChunkSize * dims)
			if err != nil {
				return err
			}
			g.VectorsComplex128Offsets = append(g.VectorsComplex128Offsets, ref.Offset)
		}
	}

	// Ensure Int64 - use arena for off-heap allocation
	if g.Type == VectorTypeInt64 {
		for len(g.VectorsInt64) <= cID {
			// Create arena if needed
			if g.Int64Arena == nil {
				slabSize := ChunkSize*dims*8 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				g.Int64Arena = memory.NewTypedArena[int64](memory.NewSlabArena(slabSize))
			}
			ref, err := g.Int64Arena.AllocSliceDirty(ChunkSize * dims)
			if err != nil {
				return err
			}
			g.VectorsInt64 = append(g.VectorsInt64, ref.Offset)
		}
	}

	// Ensure Uint64 - use arena for off-heap allocation
	if g.Type == VectorTypeUint64 {
		for len(g.VectorsUint64) <= cID {
			if g.Uint64Arena == nil {
				slabSize := ChunkSize*dims*8 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				g.Uint64Arena = memory.NewTypedArena[uint64](memory.NewSlabArena(slabSize))
			}
			ref, err := g.Uint64Arena.AllocSliceDirty(ChunkSize * dims)
			if err != nil {
				return err
			}
			g.VectorsUint64 = append(g.VectorsUint64, ref.Offset)
		}
	}

	// Ensure Int32 - use arena for off-heap allocation
	if g.Type == VectorTypeInt32 {
		for len(g.VectorsInt32) <= cID {
			if g.Int32Arena == nil {
				slabSize := ChunkSize*dims*4 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				g.Int32Arena = memory.NewTypedArena[int32](memory.NewSlabArena(slabSize))
			}
			ref, err := g.Int32Arena.AllocSliceDirty(ChunkSize * dims)
			if err != nil {
				return err
			}
			g.VectorsInt32 = append(g.VectorsInt32, ref.Offset)
		}
	}

	// Ensure Uint32 - use arena for off-heap allocation
	if g.Type == VectorTypeUint32 {
		for len(g.VectorsUint32) <= cID {
			if g.Uint32Arena == nil {
				slabSize := ChunkSize*dims*4 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				g.Uint32Arena = memory.NewTypedArena[uint32](memory.NewSlabArena(slabSize))
			}
			ref, err := g.Uint32Arena.AllocSliceDirty(ChunkSize * dims)
			if err != nil {
				return err
			}
			g.VectorsUint32 = append(g.VectorsUint32, ref.Offset)
		}
	}

	// Ensure Int16 - use arena for off-heap allocation
	if g.Type == VectorTypeInt16 {
		for len(g.VectorsInt16) <= cID {
			if g.Int16Arena == nil {
				slabSize := ChunkSize*dims*2 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				g.Int16Arena = memory.NewTypedArena[int16](memory.NewSlabArena(slabSize))
			}
			ref, err := g.Int16Arena.AllocSliceDirty(ChunkSize * dims)
			if err != nil {
				return err
			}
			g.VectorsInt16 = append(g.VectorsInt16, ref.Offset)
		}
	}

	// Ensure TQ if enabled
	if g.TurboQuantEnabled {
		for len(g.VectorsTQ) <= cID {
			stride := g.PackedSize()
			if g.Uint8Arena == nil {
				slabSize := ChunkSize*stride + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				g.Uint8Arena = memory.NewTypedArena[uint8](memory.NewSlabArena(slabSize))
			}
			ref, err := g.Uint8Arena.AllocSliceDirty(ChunkSize * stride)
			if err != nil {
				return err
			}
			g.VectorsTQ = append(g.VectorsTQ, ref.Offset)
		}
	}

	// Ensure Uint16 - use arena for off-heap allocation
	if g.Type == VectorTypeUint16 {
		for len(g.VectorsUint16) <= cID {
			if g.Uint16Arena == nil {
				slabSize := ChunkSize*dims*2 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				g.Uint16Arena = memory.NewTypedArena[uint16](memory.NewSlabArena(slabSize))
			}
			ref, err := g.Uint16Arena.AllocSliceDirty(ChunkSize * dims)
			if err != nil {
				return err
			}
			g.VectorsUint16 = append(g.VectorsUint16, ref.Offset)
		}
	}

	// Ensure Int8/Uint8
	if g.Type == VectorTypeInt8 || g.Type == VectorTypeUint8 {
		for len(g.VectorsInt8) <= cID {
			if g.Int8Arena == nil {
				slabSize := ChunkSize*dims + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				g.Int8Arena = memory.NewTypedArena[int8](memory.NewSlabArena(slabSize))
			}
			ref, err := g.Int8Arena.AllocSliceDirty(ChunkSize * dims)
			if err != nil {
				return err
			}
			g.VectorsInt8 = append(g.VectorsInt8, ref.Offset)
		}
	}

	// Ensure BQ if enabled
	if g.BQEnabled {
		for len(g.VectorsBQ) <= cID {
			if g.Uint64Arena == nil {
				paddedDims := (dims + 63) & ^63
				numWords := paddedDims / 64
				slabSize := ChunkSize*numWords*8 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				g.Uint64Arena = memory.NewTypedArena[uint64](memory.NewSlabArena(slabSize))
			}
			paddedDims := (dims + 63) & ^63
			numWords := paddedDims / 64
			ref, err := g.Uint64Arena.AllocSliceDirty(ChunkSize * numWords)
			if err != nil {
				return err
			}
			g.VectorsBQ = append(g.VectorsBQ, ref.Offset)
		}
	}

	// Ensure PQ if enabled
	if g.PQEnabled && g.PQM > 0 {
		for len(g.VectorsPQ) <= cID {
			if g.Uint64Arena == nil {
				// Allocate based on PQM
				numWords := (g.PQM + 7) / 8
				slabSize := ChunkSize*numWords*8 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				g.Uint64Arena = memory.NewTypedArena[uint64](memory.NewSlabArena(slabSize))
			}
			numWords := (g.PQM + 7) / 8
			ref, err := g.Uint64Arena.AllocSliceDirty(ChunkSize * numWords)
			if err != nil {
				return err
			}
			g.VectorsPQ = append(g.VectorsPQ, ref.Offset)
		}
	}

	// Ensure F16
	if g.Type == VectorTypeFloat16 {
		for len(g.VectorsF16) <= cID {
			if g.Float16Arena == nil {
				slabSize := ChunkSize*dims*2 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				g.Float16Arena = memory.NewTypedArena[float16.Num](memory.NewSlabArena(slabSize))
			}
			ref, err := g.Float16Arena.AllocSliceDirty(ChunkSize * dims)
			if err != nil {
				return err
			}
			g.VectorsF16 = append(g.VectorsF16, ref.Offset)
		}
	}

	return nil
}

func (g *GraphData) SetNeighbors(id uint32, neighbors []uint32) error {
	// Only support layer 0 for generic SetNeighbors on GraphData for now
	// To support multiple layers, we'd need layer argument.
	// Assuming this is used for simple tests or base layer.
	// But wait, HNSW is multi-layer.
	// The interface might be legacy or for specialized use.
	// Let's assume layer 0.

	layer := 0
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	// Ensure chunk exists (might fail if not pre-allocated)
	// But we should try to get it.
	countsChunk := g.GetCountsChunk(layer, cID)
	neighborsChunk := g.GetNeighborsChunk(layer, cID)
	versionsChunk := g.GetVersionsChunk(layer, cID)

	if countsChunk == nil || neighborsChunk == nil {
		// Try to ensure?
		if err := g.EnsureChunk(cID, 0, g.Dims); err != nil {
			return err
		}
		countsChunk = g.GetCountsChunk(layer, cID)
		neighborsChunk = g.GetNeighborsChunk(layer, cID)
		versionsChunk = g.GetVersionsChunk(layer, cID)
		if countsChunk == nil || neighborsChunk == nil {
			return fmt.Errorf("failed to allocate chunk for SetNeighbors")
		}
	}

	if len(neighbors) > MaxNeighbors {
		// Truncate or error?
		neighbors = neighbors[:MaxNeighbors]
	}

	if versionsChunk != nil {
		atomic.AddUint32(&versionsChunk[cOff], 1)
	}

	baseIdx := cOff * MaxNeighbors

	// Write neighbors
	for i, n := range neighbors {
		neighborsChunk[baseIdx+i] = n
	}

	// Update count
	if len(neighbors) > math.MaxInt32 {
		panic("too many neighbors")
	}
	countsChunk[cOff] = int32(len(neighbors)) // #nosec G115

	if versionsChunk != nil {
		atomic.AddUint32(&versionsChunk[cOff], 1)
	}

	// Increment global version
	atomic.AddUint64(&g.GlobalVersion, 1)

	return nil
}

func (g *GraphData) GetVectorsF16Chunk(chunkID int) []float16.Num {
	if chunkID < len(g.VectorsF16) && g.Float16Arena != nil {
		return g.Float16Arena.Get(memory.SliceRef{Offset: g.VectorsF16[chunkID], Len: uint32(ChunkSize * g.Dims), Cap: uint32(ChunkSize * g.Dims)})
	}
	return nil
}

// GetVector returns the vector for the given ID.
func (g *GraphData) GetVector(id uint32) (any, error) {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	// Based on type, get the appropriate chunk
	// Only supporting float32 and float16 for now in this generic method
	// for simplicity, as they are the primary types used in tests.
	if g.Uint8Arena != nil && len(g.VectorsSQ8) > cID && (g.SQ8Enabled || atomic.LoadUint32(&g.SQ8Ready) == 1) {
		chunk := g.GetVectorsSQ8Chunk(cID)
		if chunk != nil {
			paddedDims := (g.Dims + 63) & ^63
			start := cOff * paddedDims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	if g.Int8Arena != nil && len(g.VectorsInt8) > cID {
		chunk := g.GetVectorsInt8Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	if len(g.Vectors) > cID {
		chunk := g.GetVectorsChunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	if len(g.VectorsComplex64) > cID {
		chunk := g.GetVectorsComplex64Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	if len(g.VectorsComplex128) > cID {
		chunk := g.GetVectorsComplex128Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	if len(g.VectorsInt64) > cID {
		chunk := g.GetVectorsInt64Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	if len(g.VectorsUint64) > cID {
		chunk := g.GetVectorsUint64Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	if len(g.VectorsInt32) > cID {
		chunk := g.GetVectorsInt32Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	if len(g.VectorsUint32) > cID {
		chunk := g.GetVectorsUint32Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	if len(g.VectorsInt16) > cID {
		chunk := g.GetVectorsInt16Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	if len(g.VectorsUint16) > cID {
		chunk := g.GetVectorsUint16Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	if len(g.VectorsF16) > cID {
		chunk := g.GetVectorsF16Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	if len(g.VectorsFloat64) > cID || len(g.VectorsFloat64Offsets) > cID {
		chunk := g.GetVectorsFloat64Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	if len(g.VectorsComplex64) > cID || len(g.VectorsComplex64Offsets) > cID {
		chunk := g.GetVectorsComplex64Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	if len(g.VectorsComplex128) > cID || len(g.VectorsComplex128Offsets) > cID {
		chunk := g.GetVectorsComplex128Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	if g.BackingGraph != nil {
		if bg, ok := g.BackingGraph.(graphFallback); ok {
			return bg.GetVector(id)
		}
	}

	return nil, nil
}

func (g *GraphData) SetVector(id uint32, vec any) error {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	switch v := vec.(type) {
	case []float32:
		if g.Type == VectorTypeFloat16 {
			// Convert to Float16
			chunk := g.GetVectorsF16Chunk(cID)
			if chunk != nil {
				start := cOff * g.Dims
				if start+len(v) <= len(chunk) {
					for i, val := range v {
						chunk[start+i] = float16.New(val)
					}
				}
			}
			return nil
		}
		chunk := g.GetVectorsChunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []float16.Num:
		chunk := g.GetVectorsF16Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []float64:
		chunk := g.GetVectorsFloat64Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []complex64:
		chunk := g.GetVectorsComplex64Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []complex128:
		chunk := g.GetVectorsComplex128Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []int8:
		chunk := g.GetVectorsInt8Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []int16:
		chunk := g.GetVectorsInt16Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []uint16:
		chunk := g.GetVectorsUint16Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []int64:
		chunk := g.GetVectorsInt64Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []uint64:
		chunk := g.GetVectorsUint64Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []int32:
		chunk := g.GetVectorsInt32Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []uint32:
		chunk := g.GetVectorsUint32Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []uint8: // same as []byte
		if g.Type == VectorTypeUint8 && g.Int8Arena != nil {
			chunk := g.GetVectorsInt8Chunk(cID)
			if chunk != nil {
				start := cOff * g.Dims
				if start+len(v) <= len(chunk) {
					v8 := *(*[]int8)(unsafe.Pointer(&v))
					copy(chunk[start:start+len(v)], v8)
				}
			}
			return nil
		}
		chunk := g.GetVectorsSQ8Chunk(cID)
		if chunk != nil {
			paddedDims := (g.Dims + 63) & ^63
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	}
	return nil
}

// SetVectorsBatch sets multiple vectors in the same chunk efficiently.
// This is optimized for bulk insertion where vectors belong to the same chunk.
func (g *GraphData) SetVectorsBatch(startID uint32, vecs [][]float32) error {
	if len(vecs) == 0 {
		return nil
	}

	// Get chunk info for first vector
	startChunk := int(startID) / ChunkSize
	chunk := g.GetVectorsChunk(startChunk)
	if chunk == nil {
		return fmt.Errorf("chunk %d not found", startChunk)
	}

	dims := g.Dims
	if dims == 0 {
		return fmt.Errorf("dimensions not set")
	}

	// Batch copy all vectors to chunk
	for i, vec := range vecs {
		id := startID + uint32(i)
		cID := int(id) / ChunkSize
		cOff := int(id) % ChunkSize

		// Ensure we're in the same chunk
		if cID != startChunk {
			// Different chunk - use regular SetVector
			if err := g.SetVector(id, vec); err != nil {
				return err
			}
			continue
		}

		start := cOff * dims
		if start+len(vec) <= len(chunk) {
			copy(chunk[start:start+len(vec)], vec)
		}
	}

	return nil
}

func (g *GraphData) DiskStore() any {
	return g.BackingGraph
}

func (g *GraphData) PQDims() int {
	return 0
}

func (g *GraphData) GetVectorPQ(id uint32) []byte {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	chunk := g.GetVectorsPQChunk(cID)
	if chunk == nil {
		return nil
	}

	m := g.PQM
	if m == 0 {
		return nil
	}

	start := cOff * m
	if start+m <= len(chunk) {
		return chunk[start : start+m]
	}
	return nil
}

func (g *GraphData) GetVectorBQ(id uint32) ([]uint64, error) {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	if g.Uint64Arena != nil && cID < len(g.VectorsBQ) {
		paddedDims := (g.Dims + 63) & ^63
		numWords := paddedDims / 64
		chunkLen := ChunkSize * numWords

		chunk := g.Uint64Arena.Get(memory.SliceRef{
			Offset: g.VectorsBQ[cID],
			Len:    uint32(chunkLen),
			Cap:    uint32(chunkLen),
		})

		start := cOff * numWords
		if start+numWords <= len(chunk) {
			return chunk[start : start+numWords], nil
		}
	}
	return nil, fmt.Errorf("BQ vector not found for id %d", id)
}

func (g *GraphData) SetVectorBQ(id uint32, vec []uint64) error {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	if g.Uint64Arena != nil && cID < len(g.VectorsBQ) {
		paddedDims := (g.Dims + 63) & ^63
		numWords := paddedDims / 64
		chunkLen := ChunkSize * numWords

		chunk := g.Uint64Arena.Get(memory.SliceRef{
			Offset: g.VectorsBQ[cID],
			Len:    uint32(chunkLen),
			Cap:    uint32(chunkLen),
		})

		start := cOff * numWords
		if start+len(vec) <= len(chunk) {
			copy(chunk[start:start+len(vec)], vec)
			return nil
		}
	}
	return fmt.Errorf("failed to set BQ vector for id %d", id)
}

// GetNeighbors returns the neighbors for a given node at a level.
func (g *GraphData) GetNeighbors(layer int, id uint32, buf []uint32) []uint32 {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	counts := g.GetCountsChunk(layer, cID)
	neighbors := g.GetNeighborsChunk(layer, cID)
	versions := g.GetVersionsChunk(layer, cID)

	if counts == nil || neighbors == nil {
		if g.BackingGraph != nil {
			if bg, ok := g.BackingGraph.(graphFallback); ok {
				return bg.GetNeighbors(layer, id, buf)
			}
		}
		return nil
	}

	countAddr := &counts[cOff]
	base := cOff * MaxNeighbors

	// Seqlock read loop
	for attempts := 0; attempts < 100; attempts++ {
		var v1 uint32
		if versions != nil {
			v1 = atomic.LoadUint32(&versions[cOff])
			if v1&NodeLockMask != 0 {
				// Writer is active/locked, spin
				continue
			}
		}

		count := int(atomic.LoadInt32(countAddr))
		if count == 0 {
			if g.BackingGraph != nil {
				if bg, ok := g.BackingGraph.(graphFallback); ok {
					return bg.GetNeighbors(layer, id, buf)
				}
			}
			return nil
		}
		if base+count > len(neighbors) {
			return nil
		}

		var res []uint32
		if buf != nil && cap(buf) >= count {
			res = buf[:count]
		} else {
			res = make([]uint32, count)
		}

		// Atomic copy to satisfy race detector and coordinate with seqlock
		for i := 0; i < count; i++ {
			res[i] = atomic.LoadUint32(&neighbors[base+i])
		}

		if versions != nil {
			v2 := atomic.LoadUint32(&versions[cOff])
			if v1 == v2 {
				return res
			}
			// Version changed during read, retry
			continue
		}
		return res
	}

	return nil
}

// LockNode acquires a per-node spinlock.
func (g *GraphData) LockNode(layer int, id uint32) uint32 {
	versions := g.GetVersionsChunk(layer, int(id)/ChunkSize)
	if versions == nil {
		return 0
	}
	verAddr := &versions[int(id)%ChunkSize]

	for {
		v := atomic.LoadUint32(verAddr)
		if v&NodeLockMask == 0 {
			if atomic.CompareAndSwapUint32(verAddr, v, v|NodeLockMask) {
				return v // Return old version for Unlock
			}
		}
		// Spin
		for i := 0; i < 10; i++ {
			// Relaxed spin
		}
	}
}

// UnlockNode releases the per-node spinlock and increments the version.
func (g *GraphData) UnlockNode(layer int, id, oldVersion uint32) {
	versions := g.GetVersionsChunk(layer, int(id)/ChunkSize)
	if versions == nil {
		return
	}
	verAddr := &versions[int(id)%ChunkSize]
	// Increment version and clear lock bit
	newVersion := (oldVersion + 1) & (NodeLockMask - 1)
	atomic.StoreUint32(verAddr, newVersion)
}

// TryLockNode attempts to acquire the lock once.
func (g *GraphData) TryLockNode(layer int, id uint32) (uint32, bool) {
	versions := g.GetVersionsChunk(layer, int(id)/ChunkSize)
	if versions == nil {
		return 0, false
	}
	verAddr := &versions[int(id)%ChunkSize]

	v := atomic.LoadUint32(verAddr)
	if v&NodeLockMask == 0 {
		if atomic.CompareAndSwapUint32(verAddr, v, v|NodeLockMask) {
			return v, true
		}
	}
	return 0, false
}

func (g *GraphData) GetVectorSQ8(id uint32) []byte {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	if g.Uint8Arena != nil && len(g.VectorsSQ8) > cID {
		chunk := g.GetVectorsSQ8Chunk(cID)
		if chunk != nil {
			paddedDims := (g.Dims + 63) & ^63
			start := cOff * paddedDims
			if start+g.Dims <= len(chunk) {
				// Return a copy to be safe, or just the slice?
				// Disk writer wants to write it, so slice is fine.
				return chunk[start : start+g.Dims]
			}
		}
	}
	return nil
}

// GetLevelsChunk returns the level chunk for the given ID.
func (g *GraphData) GetLevelsChunk(chunkID int) []uint8 {
	if chunkID < len(g.Levels) {
		return g.Levels[chunkID]
	}
	return nil
}

// Clone creates a shallow copy of the GraphData with deep copies of the structure slices.
// This allows concurrent readers to safely access the old structure while a new one is being built (COW).
func (g *GraphData) Clone() *GraphData {
	newG := &GraphData{}

	// Metadata - use atomic loads for fields that might be modified concurrently
	newG.Capacity = g.Capacity
	newG.Dims = g.Dims
	newG.Type = g.Type
	newG.SQ8Enabled = g.SQ8Enabled
	newG.SQ8Ready = atomic.LoadUint32(&g.SQ8Ready)
	newG.BQEnabled = g.BQEnabled
	newG.PQEnabled = g.PQEnabled
	newG.PQM = g.PQM
	newG.GlobalVersion = atomic.LoadUint64(&g.GlobalVersion)
	newG.BackingGraph = g.BackingGraph
	newG.TurboQuantEnabled = g.TurboQuantEnabled
	newG.TurboQuantBits = g.TurboQuantBits

	// Slabs/Arenas (Shallow copy pointers)
	newG.Float32Arena = g.Float32Arena
	newG.Float64Arena = g.Float64Arena
	newG.Uint8Arena = g.Uint8Arena
	newG.Uint16Arena = g.Uint16Arena
	newG.Uint32Arena = g.Uint32Arena
	newG.Uint64Arena = g.Uint64Arena
	newG.Int8Arena = g.Int8Arena
	newG.Int16Arena = g.Int16Arena
	newG.Int32Arena = g.Int32Arena
	newG.Int64Arena = g.Int64Arena
	newG.Float16Arena = g.Float16Arena
	newG.Complex64Arena = g.Complex64Arena
	newG.Complex128Arena = g.Complex128Arena

	// Deep copy Vectors (Slice of slices)
	if g.Vectors != nil {
		newG.Vectors = make([][]float32, len(g.Vectors))
		copy(newG.Vectors, g.Vectors)
	}

	// Deep copy Neighbors (Layer -> Chunk)
	if g.Neighbors != nil {
		newG.Neighbors = make([][][]uint32, len(g.Neighbors))
		for l := range g.Neighbors {
			if g.Neighbors[l] != nil {
				newG.Neighbors[l] = make([][]uint32, len(g.Neighbors[l]))
				copy(newG.Neighbors[l], g.Neighbors[l])
			}
		}
	}

	// Deep copy Counts (Layer -> Chunk)
	if g.Counts != nil {
		newG.Counts = make([][][]int32, len(g.Counts))
		for l := range g.Counts {
			if g.Counts[l] != nil {
				newG.Counts[l] = make([][]int32, len(g.Counts[l]))
				copy(newG.Counts[l], g.Counts[l])
			}
		}
	}

	// Deep copy Versions (Layer -> Chunk)
	if g.Versions != nil {
		newG.Versions = make([][][]uint32, len(g.Versions))
		for l := range g.Versions {
			if g.Versions[l] != nil {
				newG.Versions[l] = make([][]uint32, len(g.Versions[l]))
				copy(newG.Versions[l], g.Versions[l])
			}
		}
	}

	// Deep copy Levels (Chunk)
	if g.Levels != nil {
		newG.Levels = make([][]uint8, len(g.Levels))
		copy(newG.Levels, g.Levels)
	}

	// Deep copy vector offset slices
	if g.VectorsF32 != nil {
		newG.VectorsF32 = make([]uint64, len(g.VectorsF32))
		copy(newG.VectorsF32, g.VectorsF32)
	}
	if g.VectorsBQ != nil {
		newG.VectorsBQ = make([]uint64, len(g.VectorsBQ))
		copy(newG.VectorsBQ, g.VectorsBQ)
	}
	if g.VectorsPQ != nil {
		newG.VectorsPQ = make([]uint64, len(g.VectorsPQ))
		copy(newG.VectorsPQ, g.VectorsPQ)
	}
	if g.VectorsF16 != nil {
		newG.VectorsF16 = make([]uint64, len(g.VectorsF16))
		copy(newG.VectorsF16, g.VectorsF16)
	}
	if g.VectorsSQ8 != nil {
		newG.VectorsSQ8 = make([]uint64, len(g.VectorsSQ8))
		copy(newG.VectorsSQ8, g.VectorsSQ8)
	}
	if g.VectorsTQ != nil {
		newG.VectorsTQ = make([]uint64, len(g.VectorsTQ))
		copy(newG.VectorsTQ, g.VectorsTQ)
	}
	if g.VectorsInt8 != nil {
		newG.VectorsInt8 = make([]uint64, len(g.VectorsInt8))
		copy(newG.VectorsInt8, g.VectorsInt8)
	}
	if g.VectorsInt16 != nil {
		newG.VectorsInt16 = make([]uint64, len(g.VectorsInt16))
		copy(newG.VectorsInt16, g.VectorsInt16)
	}
	if g.VectorsUint16 != nil {
		newG.VectorsUint16 = make([]uint64, len(g.VectorsUint16))
		copy(newG.VectorsUint16, g.VectorsUint16)
	}
	if g.VectorsInt64 != nil {
		newG.VectorsInt64 = make([]uint64, len(g.VectorsInt64))
		copy(newG.VectorsInt64, g.VectorsInt64)
	}
	if g.VectorsUint64 != nil {
		newG.VectorsUint64 = make([]uint64, len(g.VectorsUint64))
		copy(newG.VectorsUint64, g.VectorsUint64)
	}
	if g.VectorsInt32 != nil {
		newG.VectorsInt32 = make([]uint64, len(g.VectorsInt32))
		copy(newG.VectorsInt32, g.VectorsInt32)
	}
	if g.VectorsUint32 != nil {
		newG.VectorsUint32 = make([]uint64, len(g.VectorsUint32))
		copy(newG.VectorsUint32, g.VectorsUint32)
	}
	if g.VectorsFloat64 != nil {
		newG.VectorsFloat64 = make([][]float64, len(g.VectorsFloat64))
		copy(newG.VectorsFloat64, g.VectorsFloat64)
	}
	if g.VectorsComplex64 != nil {
		newG.VectorsComplex64 = make([][]complex64, len(g.VectorsComplex64))
		copy(newG.VectorsComplex64, g.VectorsComplex64)
	}
	if g.VectorsComplex128 != nil {
		newG.VectorsComplex128 = make([][]complex128, len(g.VectorsComplex128))
		copy(newG.VectorsComplex128, g.VectorsComplex128)
	}
	if g.VectorsFloat64Offsets != nil {
		newG.VectorsFloat64Offsets = make([]uint64, len(g.VectorsFloat64Offsets))
		copy(newG.VectorsFloat64Offsets, g.VectorsFloat64Offsets)
	}
	if g.VectorsComplex64Offsets != nil {
		newG.VectorsComplex64Offsets = make([]uint64, len(g.VectorsComplex64Offsets))
		copy(newG.VectorsComplex64Offsets, g.VectorsComplex64Offsets)
	}
	if g.VectorsComplex128Offsets != nil {
		newG.VectorsComplex128Offsets = make([]uint64, len(g.VectorsComplex128Offsets))
		copy(newG.VectorsComplex128Offsets, g.VectorsComplex128Offsets)
	}

	return newG
}

// PreAllocate pre-allocates memory for the given number of vectors.
// This avoids lazy allocation overhead during vector insertion.
// Slab sizes are calculated to be power-of-2 for efficient memory management.
func (g *GraphData) PreAllocate(capacity int) error {
	if capacity <= 0 || g.Dims <= 0 {
		return nil
	}

	numChunks := (capacity + ChunkSize - 1) / ChunkSize
	if numChunks <= 0 {
		numChunks = 1
	}

	// Pre-allocate Float32 arena chunks
	if g.Type == VectorTypeFloat32 || g.Type == VectorTypeUnknown {
		// Calculate optimal slab size: fit all chunks + alignment buffer
		// Use power-of-2 for efficient memory management
		requiredSize := numChunks * ChunkSize * g.Dims * 4
		slabSize := requiredSize + 4096 // 4KB alignment buffer
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		// NewSlabArena already rounds to power-of-2
		if g.Float32Arena == nil {
			g.Float32Arena = memory.NewTypedArena[float32](memory.NewSlabArena(slabSize))
		}
		// Pre-allocate all chunks
		for i := 0; i < numChunks; i++ {
			ref, err := g.Float32Arena.AllocSliceDirty(ChunkSize * g.Dims)
			if err != nil {
				return err
			}
			g.VectorsF32 = append(g.VectorsF32, ref.Offset)
			g.Vectors = append(g.Vectors, nil) // Legacy compatibility
		}
	}

	// Pre-allocate Float64 arena chunks
	if g.Type == VectorTypeFloat64 {
		requiredSize := numChunks * ChunkSize * g.Dims * 8
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Float64Arena == nil {
			g.Float64Arena = memory.NewTypedArena[float64](memory.NewSlabArena(slabSize))
		}
		for i := 0; i < numChunks; i++ {
			ref, err := g.Float64Arena.AllocSliceDirty(ChunkSize * g.Dims)
			if err != nil {
				return err
			}
			g.VectorsFloat64Offsets = append(g.VectorsFloat64Offsets, ref.Offset)
		}
	}

	// Pre-allocate TurboQuant arena chunks
	if g.TurboQuantEnabled {
		stride := g.PackedSize()
		requiredSize := numChunks * ChunkSize * stride
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Uint8Arena == nil {
			g.Uint8Arena = memory.NewTypedArena[uint8](memory.NewSlabArena(slabSize))
		}
		for i := 0; i < numChunks; i++ {
			ref, err := g.Uint8Arena.AllocSliceDirty(ChunkSize * stride)
			if err != nil {
				return err
			}
			g.VectorsTQ = append(g.VectorsTQ, ref.Offset)
		}
	}

	// Pre-allocate Complex64 arena chunks
	if g.Type == VectorTypeComplex64 {
		requiredSize := numChunks * ChunkSize * g.Dims * 8
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Complex64Arena == nil {
			g.Complex64Arena = memory.NewTypedArena[complex64](memory.NewSlabArena(slabSize))
		}
		for i := 0; i < numChunks; i++ {
			ref, err := g.Complex64Arena.AllocSliceDirty(ChunkSize * g.Dims)
			if err != nil {
				return err
			}
			g.VectorsComplex64Offsets = append(g.VectorsComplex64Offsets, ref.Offset)
		}
	}

	// Pre-allocate Complex128 arena chunks
	if g.Type == VectorTypeComplex128 {
		requiredSize := numChunks * ChunkSize * g.Dims * 16
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Complex128Arena == nil {
			g.Complex128Arena = memory.NewTypedArena[complex128](memory.NewSlabArena(slabSize))
		}
		for i := 0; i < numChunks; i++ {
			ref, err := g.Complex128Arena.AllocSliceDirty(ChunkSize * g.Dims)
			if err != nil {
				return err
			}
			g.VectorsComplex128Offsets = append(g.VectorsComplex128Offsets, ref.Offset)
		}
	}

	// Pre-allocate Int64 arena chunks
	if g.Type == VectorTypeInt64 {
		requiredSize := numChunks * ChunkSize * g.Dims * 8
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Int64Arena == nil {
			g.Int64Arena = memory.NewTypedArena[int64](memory.NewSlabArena(slabSize))
		}
		for i := 0; i < numChunks; i++ {
			ref, err := g.Int64Arena.AllocSliceDirty(ChunkSize * g.Dims)
			if err != nil {
				return err
			}
			g.VectorsInt64 = append(g.VectorsInt64, ref.Offset)
		}
	}

	// Pre-allocate Uint64 arena chunks
	if g.Type == VectorTypeUint64 {
		requiredSize := numChunks * ChunkSize * g.Dims * 8
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Uint64Arena == nil {
			g.Uint64Arena = memory.NewTypedArena[uint64](memory.NewSlabArena(slabSize))
		}
		for i := 0; i < numChunks; i++ {
			ref, err := g.Uint64Arena.AllocSliceDirty(ChunkSize * g.Dims)
			if err != nil {
				return err
			}
			g.VectorsUint64 = append(g.VectorsUint64, ref.Offset)
		}
	}

	// Pre-allocate Int32 arena chunks
	if g.Type == VectorTypeInt32 {
		requiredSize := numChunks * ChunkSize * g.Dims * 4
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Int32Arena == nil {
			g.Int32Arena = memory.NewTypedArena[int32](memory.NewSlabArena(slabSize))
		}
		for i := 0; i < numChunks; i++ {
			ref, err := g.Int32Arena.AllocSliceDirty(ChunkSize * g.Dims)
			if err != nil {
				return err
			}
			g.VectorsInt32 = append(g.VectorsInt32, ref.Offset)
		}
	}

	// Pre-allocate Uint32 arena chunks
	if g.Type == VectorTypeUint32 {
		requiredSize := numChunks * ChunkSize * g.Dims * 4
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Uint32Arena == nil {
			g.Uint32Arena = memory.NewTypedArena[uint32](memory.NewSlabArena(slabSize))
		}
		for i := 0; i < numChunks; i++ {
			ref, err := g.Uint32Arena.AllocSliceDirty(ChunkSize * g.Dims)
			if err != nil {
				return err
			}
			g.VectorsUint32 = append(g.VectorsUint32, ref.Offset)
		}
	}

	// Pre-allocate Int16 arena chunks
	if g.Type == VectorTypeInt16 {
		requiredSize := numChunks * ChunkSize * g.Dims * 2
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Int16Arena == nil {
			g.Int16Arena = memory.NewTypedArena[int16](memory.NewSlabArena(slabSize))
		}
		for i := 0; i < numChunks; i++ {
			ref, err := g.Int16Arena.AllocSliceDirty(ChunkSize * g.Dims)
			if err != nil {
				return err
			}
			g.VectorsInt16 = append(g.VectorsInt16, ref.Offset)
		}
	}

	// Pre-allocate Uint16 arena chunks
	if g.Type == VectorTypeUint16 {
		requiredSize := numChunks * ChunkSize * g.Dims * 2
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Uint16Arena == nil {
			g.Uint16Arena = memory.NewTypedArena[uint16](memory.NewSlabArena(slabSize))
		}
		for i := 0; i < numChunks; i++ {
			ref, err := g.Uint16Arena.AllocSliceDirty(ChunkSize * g.Dims)
			if err != nil {
				return err
			}
			g.VectorsUint16 = append(g.VectorsUint16, ref.Offset)
		}
	}

	// Pre-allocate Int8/Uint8 arena chunks
	if g.Type == VectorTypeInt8 || g.Type == VectorTypeUint8 {
		requiredSize := numChunks * ChunkSize * g.Dims
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Int8Arena == nil {
			g.Int8Arena = memory.NewTypedArena[int8](memory.NewSlabArena(slabSize))
		}
		for i := 0; i < numChunks; i++ {
			ref, err := g.Int8Arena.AllocSliceDirty(ChunkSize * g.Dims)
			if err != nil {
				return err
			}
			g.VectorsInt8 = append(g.VectorsInt8, ref.Offset)
		}
	}

	// Pre-allocate Float16 arena chunks
	if g.Type == VectorTypeFloat16 {
		requiredSize := numChunks * ChunkSize * g.Dims * 2
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Float16Arena == nil {
			g.Float16Arena = memory.NewTypedArena[float16.Num](memory.NewSlabArena(slabSize))
		}
		for i := 0; i < numChunks; i++ {
			ref, err := g.Float16Arena.AllocSliceDirty(ChunkSize * g.Dims)
			if err != nil {
				return err
			}
			g.VectorsF16 = append(g.VectorsF16, ref.Offset)
		}
	}

	// Pre-allocate SQ8 arena chunks if enabled
	if g.SQ8Enabled {
		paddedDims := (g.Dims + 63) & ^63
		requiredSize := numChunks * ChunkSize * paddedDims
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Uint8Arena == nil {
			g.Uint8Arena = memory.NewTypedArena[uint8](memory.NewSlabArena(slabSize))
		}
		for i := 0; i < numChunks; i++ {
			ref, err := g.Uint8Arena.AllocSliceDirty(ChunkSize * paddedDims)
			if err != nil {
				return err
			}
			g.VectorsSQ8 = append(g.VectorsSQ8, ref.Offset)
		}
	}

	// Pre-allocate BQ arena chunks if enabled
	if g.BQEnabled {
		paddedDims := (g.Dims + 63) & ^63
		numWords := paddedDims / 64
		requiredSize := numChunks * ChunkSize * numWords * 8
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Uint64Arena == nil {
			g.Uint64Arena = memory.NewTypedArena[uint64](memory.NewSlabArena(slabSize))
		}
		for i := 0; i < numChunks; i++ {
			ref, err := g.Uint64Arena.AllocSliceDirty(ChunkSize * numWords)
			if err != nil {
				return err
			}
			g.VectorsBQ = append(g.VectorsBQ, ref.Offset)
		}
	}

	// Pre-allocate PQ arena chunks if enabled
	if g.PQEnabled && g.PQM > 0 {
		numWords := (g.PQM + 7) / 8
		requiredSize := numChunks * ChunkSize * numWords * 8
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Uint64Arena == nil {
			g.Uint64Arena = memory.NewTypedArena[uint64](memory.NewSlabArena(slabSize))
		}
		for i := 0; i < numChunks; i++ {
			ref, err := g.Uint64Arena.AllocSliceDirty(ChunkSize * numWords)
			if err != nil {
				return err
			}
			g.VectorsPQ = append(g.VectorsPQ, ref.Offset)
		}
	}

	// Pre-allocate Levels for all chunks
	for i := 0; i < numChunks; i++ {
		g.Levels = append(g.Levels, make([]uint8, ChunkSize))
	}

	// Pre-allocate Neighbors, Counts, Versions for all layers
	if len(g.Neighbors) == 0 {
		g.Neighbors = make([][][]uint32, ArrowMaxLayers)
		g.Counts = make([][][]int32, ArrowMaxLayers)
		g.Versions = make([][][]uint32, ArrowMaxLayers)
	}
	for layer := 0; layer < ArrowMaxLayers; layer++ {
		for i := 0; i < numChunks; i++ {
			g.Neighbors[layer] = append(g.Neighbors[layer], make([]uint32, ChunkSize*MaxNeighbors))
			g.Counts[layer] = append(g.Counts[layer], make([]int32, ChunkSize))
			g.Versions[layer] = append(g.Versions[layer], make([]uint32, ChunkSize))
		}
	}

	g.Capacity = capacity

	return nil
}

// NewGraphData creates a new GraphData instance.

// This is a helper for legacy tests.
// NewGraphData creates a new GraphData instance.
func NewGraphData(capacity, dim int, mmap bool, useDisk bool, fd int,
	quantization bool, sq8 bool, persistent bool,
	dataType VectorDataType, bqEnabled bool, pqEnabled bool,
	tqEnabled bool, tqBits int) *GraphData {

	// Initialize arenas with power-of-2 slab sizes
	// Slab size: fit at least one chunk + overhead.
	// Float32: 1024 * dim * 4 bytes.
	f32SlabSize := ChunkSize*dim*4 + 64
	if f32SlabSize < 1024*1024 {
		f32SlabSize = 1024 * 1024
	}
	f32Arena := memory.NewSlabArena(f32SlabSize)

	// Uint8: 1024 * dim * 1 bytes.
	u8SlabSize := ChunkSize*dim + 64
	if u8SlabSize < 1024*1024 {
		u8SlabSize = 1024 * 1024
	}
	u8Arena := memory.NewSlabArena(u8SlabSize)

	// Float64: 8 bytes
	f64SlabSize := ChunkSize*dim*8 + 64
	if f64SlabSize < 1024*1024 {
		f64SlabSize = 1024 * 1024
	}
	f64Arena := memory.NewSlabArena(f64SlabSize)

	// Int8: 1 byte (reuse logic/size if creating distinct arena, but simpler to separate)
	i8Arena := memory.NewSlabArena(u8SlabSize)

	// Complex64: 8 bytes
	c64SlabSize := ChunkSize*dim*8 + 64
	if c64SlabSize < 1024*1024 {
		c64SlabSize = 1024 * 1024
	}
	c64Arena := memory.NewSlabArena(c64SlabSize)

	// Complex128: 16 bytes
	c128SlabSize := ChunkSize*dim*16 + 64
	if c128SlabSize < 1024*1024 {
		c128SlabSize = 1024 * 1024
	}
	c128Arena := memory.NewSlabArena(c128SlabSize)

	// Int64: 8 bytes
	i64SlabSize := ChunkSize*dim*8 + 64
	if i64SlabSize < 1024*1024 {
		i64SlabSize = 1024 * 1024
	}
	i64Arena := memory.NewSlabArena(i64SlabSize)

	numChunks := (capacity + ChunkSize - 1) / ChunkSize
	if numChunks < 0 {
		numChunks = 0
	}

	gd := &GraphData{
		Capacity:          capacity,
		Dims:              dim,
		Type:              dataType,
		SQ8Enabled:        sq8,
		BQEnabled:         bqEnabled,
		PQEnabled:         pqEnabled,
		Vectors:           make([][]float32, numChunks),
		VectorsFloat64:    make([][]float64, numChunks),
		VectorsComplex64:  make([][]complex64, numChunks),
		VectorsComplex128: make([][]complex128, numChunks),
		TurboQuantEnabled: tqEnabled,
		TurboQuantBits:    tqBits,
		Neighbors:         make([][][]uint32, ArrowMaxLayers),
		Counts:            make([][][]int32, ArrowMaxLayers),
		Versions:          make([][][]uint32, ArrowMaxLayers),
		Levels:            make([][]uint8, 0, numChunks),
		Float32Arena:      memory.NewTypedArena[float32](f32Arena),
		Uint8Arena:        memory.NewTypedArena[uint8](u8Arena),
		Float64Arena:      memory.NewTypedArena[float64](f64Arena),
		Int8Arena:         memory.NewTypedArena[int8](i8Arena),
		Int64Arena:        memory.NewTypedArena[int64](i64Arena),
		Complex64Arena:    memory.NewTypedArena[complex64](c64Arena),
		Complex128Arena:    memory.NewTypedArena[complex128](c128Arena),
		VectorsTQ:          make([]uint64, 0, numChunks),
	}

	for i := 0; i < ArrowMaxLayers; i++ {
		gd.Neighbors[i] = make([][]uint32, 0, numChunks)
		gd.Counts[i] = make([][]int32, 0, numChunks)
		gd.Versions[i] = make([][]uint32, 0, numChunks)
	}

	// Pre-allocate chunks for the given capacity to avoid lazy allocation overhead
	if capacity > 0 && dim > 0 {
		_ = gd.PreAllocate(capacity)
	}

	return gd
}

func (g *GraphData) Release() {
	if g.Float32Arena != nil {
		g.Float32Arena.Free()
		g.Float32Arena = nil
	}
	if g.Float64Arena != nil {
		g.Float64Arena.Free()
		g.Float64Arena = nil
	}
	if g.Uint8Arena != nil {
		g.Uint8Arena.Free()
		g.Uint8Arena = nil
	}
	if g.Uint16Arena != nil {
		g.Uint16Arena.Free()
		g.Uint16Arena = nil
	}
	if g.Uint32Arena != nil {
		g.Uint32Arena.Free()
		g.Uint32Arena = nil
	}
	if g.Uint64Arena != nil {
		g.Uint64Arena.Free()
		g.Uint64Arena = nil
	}
	if g.Int8Arena != nil {
		g.Int8Arena.Free()
		g.Int8Arena = nil
	}
	if g.Int16Arena != nil {
		g.Int16Arena.Free()
		g.Int16Arena = nil
	}
	if g.Int32Arena != nil {
		g.Int32Arena.Free()
		g.Int32Arena = nil
	}
	if g.Int64Arena != nil {
		g.Int64Arena.Free()
		g.Int64Arena = nil
	}
	if g.Float16Arena != nil {
		g.Float16Arena.Free()
		g.Float16Arena = nil
	}
	if g.Complex64Arena != nil {
		g.Complex64Arena.Free()
		g.Complex64Arena = nil
	}
	if g.Complex128Arena != nil {
		g.Complex128Arena.Free()
		g.Complex128Arena = nil
	}
}

func (g *GraphData) Unregister() {
	if g.Float32Arena != nil {
		memory.UnregisterArena(g.Float32Arena.Slab())
	}
	if g.Float64Arena != nil {
		memory.UnregisterArena(g.Float64Arena.Slab())
	}
	if g.Uint8Arena != nil {
		memory.UnregisterArena(g.Uint8Arena.Slab())
	}
	if g.Uint16Arena != nil {
		memory.UnregisterArena(g.Uint16Arena.Slab())
	}
	if g.Uint32Arena != nil {
		memory.UnregisterArena(g.Uint32Arena.Slab())
	}
	if g.Uint64Arena != nil {
		memory.UnregisterArena(g.Uint64Arena.Slab())
	}
	if g.Int8Arena != nil {
		memory.UnregisterArena(g.Int8Arena.Slab())
	}
	if g.Int16Arena != nil {
		memory.UnregisterArena(g.Int16Arena.Slab())
	}
	if g.Int32Arena != nil {
		memory.UnregisterArena(g.Int32Arena.Slab())
	}
	if g.Int64Arena != nil {
		memory.UnregisterArena(g.Int64Arena.Slab())
	}
	if g.Float16Arena != nil {
		memory.UnregisterArena(g.Float16Arena.Slab())
	}
	if g.Complex64Arena != nil {
		memory.UnregisterArena(g.Complex64Arena.Slab())
	}
	if g.Complex128Arena != nil {
		memory.UnregisterArena(g.Complex128Arena.Slab())
	}
}
