package types

import (
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"unsafe"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"runtime"
	"sync"
)

// PaddedMutex is a sync.Mutex padded to a full 64-byte cache line to prevent false sharing.
type PaddedMutex struct {
	sync.Mutex
	_ [56]byte // Padding to 64 bytes (assuming 8-byte mutex)
}

// GraphData holds the vector data and graph topology.
// It effectively implements the component storage for ArrowHNSW.
type GraphData struct {
	// Metadata
	Capacity      int            // Total number of nodes the graph can currently hold.
	Dims          int            // Number of dimensions for the vectors.
	Type          VectorDataType // Underlying data type of the vectors.
	SQ8Enabled    bool           // Whether Scalar Quantization (8-bit) is enabled.
	SQ8Ready      uint32         // 0=not ready, 1=ready (atomic).
	BQEnabled     bool           // Whether Binary Quantization is enabled.
	PQEnabled     bool           // Whether Product Quantization is enabled.
	PQM           int            // Number of sub-spaces for Product Quantization.
	GlobalVersion uint64         // Incremented on structural changes for cache validation.
	BackingGraph  any            // Interface to a persistent storage (e.g., *DiskGraph).
	Name          string         // Unique identifier for the dataset (used in metrics).

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

	// Neighbors (Layer -> Chunk -> Arena Offset)
	Neighbors [][]uint64

	// Levels (Chunk -> Data)
	Levels [][]uint8

	// Versions (Layer -> Chunk -> Arena Offset)
	Versions [][]uint64

	// Counts (Layer -> Chunk -> Arena Offset)
	Counts [][]uint64

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
	
	// ArrowRefs holds references to external Arrow arrays providing vector data.
	// Used for zero-copy ingestion paths.
	ArrowRefs []arrow.Array
 
	// Sharded locks for fine-grained concurrency control
	ShardedMus [1024]PaddedMutex
}

// graphFallback provides a secondary mechanism for neighbor and vector retrieval.
type graphFallback interface {
	GetNeighbors(layer int, id uint32, buf []uint32) []uint32
	GetVector(id uint32) (any, error)
}

// PackedNeighbors interface for graph adjacency management with atomic support.
type PackedNeighbors interface {
	// GetNeighbors returns the list of neighbor IDs for a given node.
	GetNeighbors(id uint32) ([]uint32, bool)
	// GetPackedNeighbors returns a packed representation of neighbors for atomic operations.
	GetPackedNeighbors(id uint32) (uint64, bool)
	// GetNeighborsFromPacked extracts a list of neighbor IDs from a packed uint64.
	GetNeighborsFromPacked(packed uint64) []uint32
	// SetNeighbors updates the neighbor list for a node.
	SetNeighbors(id uint32, neighbors []uint32) error
	// CASNeighbors performs an atomic compare-and-swap on the neighbor list.
	CASNeighbors(id uint32, oldPacked uint64, new []uint32) bool
	// GetNeighborsF16 returns neighbors and their distances in float16 precision.
	GetNeighborsF16(id uint32) ([]uint32, []float16.Num, bool)
	// SetNeighborsF16 updates neighbors and their distances in float16 precision.
	SetNeighborsF16(id uint32, neighbors []uint32, dists []float16.Num) error
	// EnsureCapacity ensures the underlying storage can accommodate the given node ID.
	EnsureCapacity(id uint32)
	// Lock acquires a node-specific lock (usually a shard lock).
	Lock(id uint32)
	// Unlock releases a node-specific lock.
	Unlock(id uint32)
	// UpdateNeighbors performs an atomic read-modify-write update using a callback.
	UpdateNeighbors(id uint32, fn func(old []uint32) []uint32) error
}

// GetNodeCount returns the current capacity of the graph (number of addressable nodes).
func (g *GraphData) GetNodeCount() int {
	return g.Capacity
}

// NeedsChunk returns true if the given chunk ID requires allocation for any enabled data type.
func (g *GraphData) NeedsChunk(cID int) bool {
	// 1. Primary Float32 check
	if (g.Type == VectorTypeFloat32 || g.Type == VectorTypeUnknown) {
		if cID >= len(g.VectorsF32) || g.Float32Arena == nil {
			return true
		}
	}

	// 2. Specialized quantization checks
	if g.SQ8Enabled {
		if cID >= len(g.VectorsSQ8) || g.Uint8Arena == nil {
			return true
		}
	}
	if g.PQEnabled && g.PQM > 0 {
		if cID >= len(g.VectorsPQ) || g.Uint64Arena == nil {
			return true
		}
	}
	if g.BQEnabled {
		if cID >= len(g.VectorsBQ) || g.Uint64Arena == nil {
			return true
		}
	}
	if g.TurboQuantEnabled {
		if cID >= len(g.VectorsTQ) || g.Uint8Arena == nil {
			return true
		}
	}

	// 3. Int8/Uint8 vectors
	if g.Type == VectorTypeInt8 || g.Type == VectorTypeUint8 {
		if cID >= len(g.VectorsInt8) || g.Int8Arena == nil {
			return true
		}
	}

	// 4. Float16 vectors
	if g.Type == VectorTypeFloat16 {
		if cID >= len(g.VectorsF16) || g.Float16Arena == nil {
			return true
		}
	}

	// 5. Int16 vectors
	if g.Type == VectorTypeInt16 {
		if cID >= len(g.VectorsInt16) || g.Int16Arena == nil {
			return true
		}
	}

	// 6. Uint16 vectors
	if g.Type == VectorTypeUint16 {
		if cID >= len(g.VectorsUint16) || g.Uint16Arena == nil {
			return true
		}
	}

	// 7. Int32 vectors
	if g.Type == VectorTypeInt32 {
		if cID >= len(g.VectorsInt32) || g.Int32Arena == nil {
			return true
		}
	}

	// 8. Uint32 vectors
	if g.Type == VectorTypeUint32 {
		if cID >= len(g.VectorsUint32) || g.Uint32Arena == nil {
			return true
		}
	}

	// 9. Int64 vectors
	if g.Type == VectorTypeInt64 {
		if cID >= len(g.VectorsInt64) || g.Int64Arena == nil {
			return true
		}
	}

	// 10. Uint64 vectors
	if g.Type == VectorTypeUint64 {
		if cID >= len(g.VectorsUint64) || g.Uint64Arena == nil {
			return true
		}
	}

	// 11. Float64 vectors
	if g.Type == VectorTypeFloat64 {
		if cID >= len(g.VectorsFloat64Offsets) || g.Float64Arena == nil {
			return true
		}
	}

	// 12. Complex64 vectors
	if g.Type == VectorTypeComplex64 {
		if cID >= len(g.VectorsComplex64Offsets) || g.Complex64Arena == nil {
			return true
		}
	}

	// 13. Complex128 vectors
	if g.Type == VectorTypeComplex128 {
		if cID >= len(g.VectorsComplex128Offsets) || g.Complex128Arena == nil {
			return true
		}
	}

	// 14. Metadata and Neighbors
	if cID >= len(g.Levels) || g.Levels[cID] == nil {
		return true
	}
	if len(g.Neighbors) > 0 {
		if cID >= len(g.Neighbors[0]) || g.Neighbors[0][cID] == 0 {
			return true
		}
	}

	return false
}

// GetVectorsChunk returns the vector chunk for the given ID.
func (g *GraphData) GetVectorsChunk(chunkID int) []float32 {
	// Try arena first (off-heap, GC-free)
	if g.Float32Arena != nil && chunkID < len(g.VectorsF32) {
		pd := g.GetPaddedDimsForType(VectorTypeFloat32)
		return g.Float32Arena.Get(memory.SliceRef{Offset: g.VectorsF32[chunkID], Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	// Fallback to legacy slice
	if chunkID < len(g.Vectors) {
		return g.Vectors[chunkID]
	}
	return nil
}

// PackedSize returns the byte size of a TurboQuant packed vector.
func (g *GraphData) PackedSize() int {
	if g.Dims <= 0 {
		return 0
	}
	p2 := int(1 << uint(math.Ceil(math.Log2(float64(g.Dims)))))
	angleBytes := ((p2-1)*g.TurboQuantBits + 7) / 8
	bitBytes := (p2 + 7) / 8
	return 4 + angleBytes + bitBytes
}

// GetVectorsTQChunk returns a chunk of TurboQuant compressed vectors.
func (g *GraphData) GetVectorsTQChunk(chunkID int) []byte {
	if chunkID < len(g.VectorsTQ) && g.Uint8Arena != nil {
		stride := g.PackedSize()
		return g.Uint8Arena.Get(memory.SliceRef{Offset: g.VectorsTQ[chunkID], Len: uint32(ChunkSize * stride), Cap: uint32(ChunkSize * stride)}) // #nosec G115
	}
	return nil
}

// GetVectorsFloat64Chunk returns a chunk of float64 vectors.
func (g *GraphData) GetVectorsFloat64Chunk(chunkID int) []float64 {
	if chunkID < len(g.VectorsFloat64Offsets) && g.Float64Arena != nil {
		return g.Float64Arena.Get(memory.SliceRef{Offset: g.VectorsFloat64Offsets[chunkID], Len: uint32(ChunkSize * g.Dims), Cap: uint32(ChunkSize * g.Dims)}) // #nosec G115
	}
	if chunkID < len(g.VectorsFloat64) {
		return g.VectorsFloat64[chunkID]
	}
	return nil
}

// GetVectorsComplex64Chunk returns a chunk of complex64 vectors.
func (g *GraphData) GetVectorsComplex64Chunk(chunkID int) []complex64 {
	if chunkID < len(g.VectorsComplex64Offsets) && g.Complex64Arena != nil {
		return g.Complex64Arena.Get(memory.SliceRef{Offset: g.VectorsComplex64Offsets[chunkID], Len: uint32(ChunkSize * g.Dims), Cap: uint32(ChunkSize * g.Dims)}) // #nosec G115
	}
	if chunkID < len(g.VectorsComplex64) {
		return g.VectorsComplex64[chunkID]
	}
	return nil
}

// GetVectorsComplex128Chunk returns a chunk of complex128 vectors.
func (g *GraphData) GetVectorsComplex128Chunk(chunkID int) []complex128 {
	if chunkID < len(g.VectorsComplex128Offsets) && g.Complex128Arena != nil {
		return g.Complex128Arena.Get(memory.SliceRef{Offset: g.VectorsComplex128Offsets[chunkID], Len: uint32(ChunkSize * g.Dims), Cap: uint32(ChunkSize * g.Dims)}) // #nosec G115
	}
	if chunkID < len(g.VectorsComplex128) {
		return g.VectorsComplex128[chunkID]
	}
	return nil
}

// GetVectorsInt64Chunk returns a chunk of int64 vectors.
func (g *GraphData) GetVectorsInt64Chunk(chunkID int) []int64 {
	if chunkID < len(g.VectorsInt64) && g.Int64Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeInt64)
		return g.Int64Arena.Get(memory.SliceRef{Offset: g.VectorsInt64[chunkID], Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	return nil
}

// GetVectorsUint64Chunk returns a chunk of uint64 vectors.
func (g *GraphData) GetVectorsUint64Chunk(chunkID int) []uint64 {
	if chunkID < len(g.VectorsUint64) && g.Uint64Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeUint64)
		return g.Uint64Arena.Get(memory.SliceRef{Offset: g.VectorsUint64[chunkID], Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	return nil
}

// GetVectorsInt32Chunk returns a chunk of int32 vectors.
func (g *GraphData) GetVectorsInt32Chunk(chunkID int) []int32 {
	if chunkID < len(g.VectorsInt32) && g.Int32Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeInt32)
		return g.Int32Arena.Get(memory.SliceRef{Offset: g.VectorsInt32[chunkID], Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	return nil
}

// GetVectorsUint32Chunk returns a chunk of uint32 vectors.
func (g *GraphData) GetVectorsUint32Chunk(chunkID int) []uint32 {
	if chunkID < len(g.VectorsUint32) && g.Uint32Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeUint32)
		return g.Uint32Arena.Get(memory.SliceRef{Offset: g.VectorsUint32[chunkID], Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	return nil
}

// GetPaddedDims returns the padded dimension for the primary vector type.
func (g *GraphData) GetPaddedDims() int {
	return g.GetPaddedDimsForType(g.Type)
}

// GetPaddedDimsForType returns the padded dimension for a specific vector type to ensure SIMD alignment.
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
		return g.Uint8Arena.Get(memory.SliceRef{Offset: g.VectorsSQ8[chunkID], Len: uint32(ChunkSize * paddedDims), Cap: uint32(ChunkSize * paddedDims)}) // #nosec G115
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
			Len:    uint32(chunkLen), // #nosec G115
			Cap:    uint32(chunkLen), // #nosec G115
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
			Len:    uint32(numWords), // #nosec G115
			Cap:    uint32(numWords), // #nosec G115
		})

		if len(chunk) == 0 {
			return nil
		}

		// Cast uint64 to byte slice
		ptr := unsafe.Pointer(&chunk[0])           // #nosec G103
		return unsafe.Slice((*byte)(ptr), numWords*8) // #nosec G103
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
			Len:    uint32(numWords), // #nosec G115
			Cap:    uint32(numWords), // #nosec G115
		})

		if len(chunk) == 0 {
			return fmt.Errorf("PQ chunk is empty (arena %p, offset %d)", g.Uint64Arena, g.VectorsPQ[cID])
		}

		// Cast uint64 to byte slice
		ptr := unsafe.Pointer(&chunk[0])           // #nosec G103
		byteChunk := unsafe.Slice((*byte)(ptr), numWords*8) // #nosec G103

		start := cOff * m
		if start+m <= len(byteChunk) {
			copy(byteChunk[start:start+m], code)
			return nil
		}
	}
	return fmt.Errorf("failed to set PQ vector for id %d", id)
}

func (g *GraphData) GetCountsChunk(layer, chunkID int) []int32 {
	if layer < len(g.Counts) && chunkID < len(g.Counts[layer]) && g.Int32Arena != nil {
		offset := g.Counts[layer][chunkID]
		if offset == 0 {
			return nil
		}
		return g.Int32Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize), Cap: uint32(ChunkSize)}) // #nosec G115
	}
	return nil
}

func (g *GraphData) GetNeighborsChunk(layer, chunkID int) []uint32 {
	if layer < len(g.Neighbors) && chunkID < len(g.Neighbors[layer]) && g.Uint32Arena != nil {
		offset := g.Neighbors[layer][chunkID]
		if offset == 0 {
			return nil
		}
		return g.Uint32Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * MaxNeighbors), Cap: uint32(ChunkSize * MaxNeighbors)}) // #nosec G115
	}
	return nil
}

func (g *GraphData) GetVersionsChunk(layer, chunkID int) []uint32 {
	if layer < len(g.Versions) && chunkID < len(g.Versions[layer]) && g.Uint32Arena != nil {
		offset := g.Versions[layer][chunkID]
		if offset == 0 {
			return nil
		}
		return g.Uint32Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize), Cap: uint32(ChunkSize)}) // #nosec G115
	}
	return nil
}

func (g *GraphData) GetVectorsInt8Chunk(chunkID int) []int8 {
	if chunkID < len(g.VectorsInt8) && g.Int8Arena != nil {
		paddedDims := g.GetPaddedDimsForType(VectorTypeInt8)
		return g.Int8Arena.Get(memory.SliceRef{Offset: g.VectorsInt8[chunkID], Len: uint32(ChunkSize * paddedDims), Cap: uint32(ChunkSize * paddedDims)}) // #nosec G115
	}
	return nil
}

func (g *GraphData) GetVectorsInt16Chunk(chunkID int) []int16 {
	if chunkID < len(g.VectorsInt16) && g.Int16Arena != nil {
		paddedDims := g.GetPaddedDimsForType(VectorTypeInt16)
		return g.Int16Arena.Get(memory.SliceRef{Offset: g.VectorsInt16[chunkID], Len: uint32(ChunkSize * paddedDims), Cap: uint32(ChunkSize * paddedDims)}) // #nosec G115
	}
	return nil
}

func (g *GraphData) GetVectorsUint16Chunk(chunkID int) []uint16 {
	if chunkID < len(g.VectorsUint16) && g.Uint16Arena != nil {
		paddedDims := g.GetPaddedDimsForType(VectorTypeUint16)
		return g.Uint16Arena.Get(memory.SliceRef{Offset: g.VectorsUint16[chunkID], Len: uint32(ChunkSize * paddedDims), Cap: uint32(ChunkSize * paddedDims)}) // #nosec G115
	}
	return nil
}

func (g *GraphData) EnsureChunk(cID, cOff, dims int) error {
	// 1. Ensure Vectors (Float32 / Unknown)
	if g.Type == VectorTypeFloat32 || g.Type == VectorTypeUnknown {
		paddedDims := g.GetPaddedDimsForType(VectorTypeFloat32)
		for len(g.VectorsF32) <= cID {
			if dims == 0 {
				g.VectorsF32 = append(g.VectorsF32, 0)
				g.Vectors = append(g.Vectors, nil)
				continue
			}
			if g.Float32Arena == nil {
				slabSize := ChunkSize*paddedDims*4 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				g.Float32Arena = memory.NewTypedArena[float32](memory.NewSlabArena(slabSize))
			}
			ref, err := g.Float32Arena.AllocSlice(ChunkSize * paddedDims)
			if err != nil {
				return err
			}
			g.VectorsF32 = append(g.VectorsF32, ref.Offset)
			g.Vectors = append(g.Vectors, nil)
		}
	}

	// 2. Ensure SQ8 if enabled
	if g.SQ8Enabled {
		paddedDims := (dims + 63) & ^63
		for len(g.VectorsSQ8) <= cID {
			if dims == 0 {
				g.VectorsSQ8 = append(g.VectorsSQ8, 0)
				continue
			}
			if g.Uint8Arena == nil {
				slabSize := ChunkSize*paddedDims + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				g.Uint8Arena = memory.NewTypedArena[uint8](memory.NewSlabArena(slabSize))
			}
			ref, err := g.Uint8Arena.AllocSliceDirty(ChunkSize * paddedDims)
			if err != nil {
				return err
			}
			g.VectorsSQ8 = append(g.VectorsSQ8, ref.Offset)
		}
	}

	// 3. Ensure PQ if enabled
	if g.PQEnabled && g.PQM > 0 {
		numWordsPerNode := (g.PQM + 7) / 8
		for len(g.VectorsPQ) <= cID {
			if dims == 0 {
				g.VectorsPQ = append(g.VectorsPQ, 0)
				continue
			}
			if g.Uint64Arena == nil {
				slabSize := ChunkSize*numWordsPerNode*8 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				g.Uint64Arena = memory.NewTypedArena[uint64](memory.NewSlabArena(slabSize))
			}
			ref, err := g.Uint64Arena.AllocSliceDirty(ChunkSize * numWordsPerNode)
			if err != nil {
				return err
			}
			g.VectorsPQ = append(g.VectorsPQ, ref.Offset)
		}
	}

	// 4. Ensure Levels
	for len(g.Levels) <= cID {
		g.Levels = append(g.Levels, nil)
	}
	if g.Levels[cID] == nil {
		g.Levels[cID] = make([]uint8, ChunkSize)
	}


	// Ensure Neighbors, Counts, Versions for ALL possible HNSW layers to avoid concurrent appends.
	if len(g.Neighbors) == 0 {
		g.Neighbors = make([][]uint64, ArrowMaxLayers)
		g.Counts = make([][]uint64, ArrowMaxLayers)
		g.Versions = make([][]uint64, ArrowMaxLayers)
	}
 
	for l := 0; l < ArrowMaxLayers; l++ {
		requiredLen := cID + 1
		if len(g.Neighbors[l]) < requiredLen {
			delta := requiredLen - len(g.Neighbors[l])
			g.Neighbors[l] = append(g.Neighbors[l], make([]uint64, delta)...)
			g.Counts[l] = append(g.Counts[l], make([]uint64, delta)...)
			g.Versions[l] = append(g.Versions[l], make([]uint64, delta)...)
		}
 
		if g.Neighbors[l][cID] == 0 {
			if g.Uint32Arena == nil {
				// Ensure slab size can accommodate a full chunk of neighbors.
				// ChunkSize (1024) * MaxNeighbors (512) * 4 bytes = 2MB.
				// We use 16MB to allow for multiple concurrent chunks per slab.
				slabSize := 16 * 1024 * 1024
				g.Uint32Arena = memory.NewTypedArena[uint32](memory.NewSlabArena(slabSize))
			}
			ref, err := g.Uint32Arena.AllocSlice(ChunkSize * MaxNeighbors)
			if err != nil {
				return err
			}
			g.Neighbors[l][cID] = ref.Offset
		}

		if g.Counts[l][cID] == 0 {
			if g.Int32Arena == nil {
				// Use 4MB slab for counts (1024 * 4 = 4KB per chunk, many chunks per slab)
				g.Int32Arena = memory.NewTypedArena[int32](memory.NewSlabArena(4 * 1024 * 1024))
			}
			ref, err := g.Int32Arena.AllocSlice(ChunkSize)
			if err != nil {
				return err
			}
			g.Counts[l][cID] = ref.Offset
		}

		if g.Versions[l][cID] == 0 {
			if g.Uint32Arena == nil {
				// Share Uint32Arena with neighbors if it already exists.
				// Ensure same 16MB slab size as neighbors.
				slabSize := 16 * 1024 * 1024
				g.Uint32Arena = memory.NewTypedArena[uint32](memory.NewSlabArena(slabSize))
			}
			ref, err := g.Uint32Arena.AllocSlice(ChunkSize)
			if err != nil {
				return err
			}
			g.Versions[l][cID] = ref.Offset
		}
	}

	// Ensure Float64 - use arena for off-heap allocation
	if g.Type == VectorTypeFloat64 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeFloat64)
		if paddedDims > 0 {
			for len(g.VectorsFloat64Offsets) <= cID {
				if g.Float64Arena == nil {
					slabSize := ChunkSize*paddedDims*8 + 64
					if slabSize < 1024*1024 {
						slabSize = 1024 * 1024
					}
					g.Float64Arena = memory.NewTypedArena[float64](memory.NewSlabArena(slabSize))
				}
				ref, err := g.Float64Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				g.VectorsFloat64Offsets = append(g.VectorsFloat64Offsets, ref.Offset)
			}
		}
	}

	// Ensure Complex64 - use arena for off-heap allocation
	if g.Type == VectorTypeComplex64 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeComplex64)
		if paddedDims > 0 {
			for len(g.VectorsComplex64Offsets) <= cID {
				if g.Complex64Arena == nil {
					slabSize := ChunkSize*paddedDims*8 + 64
					if slabSize < 1024*1024 {
						slabSize = 1024 * 1024
					}
					g.Complex64Arena = memory.NewTypedArena[complex64](memory.NewSlabArena(slabSize))
				}
				ref, err := g.Complex64Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				g.VectorsComplex64Offsets = append(g.VectorsComplex64Offsets, ref.Offset)
			}
		}
	}

	// Ensure Complex128 - use arena for off-heap allocation
	if g.Type == VectorTypeComplex128 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeComplex128)
		if paddedDims > 0 {
			for len(g.VectorsComplex128Offsets) <= cID {
				if g.Complex128Arena == nil {
					slabSize := ChunkSize*paddedDims*16 + 64
					if slabSize < 1024*1024 {
						slabSize = 1024 * 1024
					}
					g.Complex128Arena = memory.NewTypedArena[complex128](memory.NewSlabArena(slabSize))
				}
				ref, err := g.Complex128Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				g.VectorsComplex128Offsets = append(g.VectorsComplex128Offsets, ref.Offset)
			}
		}
	}

	// Ensure Int64 - use arena for off-heap allocation
	if g.Type == VectorTypeInt64 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeInt64)
		if paddedDims > 0 {
			for len(g.VectorsInt64) <= cID {
				if g.Int64Arena == nil {
					slabSize := ChunkSize*paddedDims*8 + 64
					if slabSize < 1024*1024 {
						slabSize = 1024 * 1024
					}
					g.Int64Arena = memory.NewTypedArena[int64](memory.NewSlabArena(slabSize))
				}
				ref, err := g.Int64Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				g.VectorsInt64 = append(g.VectorsInt64, ref.Offset)
			}
		}
	}

	// Ensure Uint64 - use arena for off-heap allocation
	if g.Type == VectorTypeUint64 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeUint64)
		if paddedDims > 0 {
			for len(g.VectorsUint64) <= cID {
				if g.Uint64Arena == nil {
					slabSize := ChunkSize*paddedDims*8 + 64
					if slabSize < 1024*1024 {
						slabSize = 1024 * 1024
					}
					g.Uint64Arena = memory.NewTypedArena[uint64](memory.NewSlabArena(slabSize))
				}
				ref, err := g.Uint64Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				g.VectorsUint64 = append(g.VectorsUint64, ref.Offset)
			}
		}
	}

	// Ensure Int32
	if g.Type == VectorTypeInt32 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeInt32)
		if paddedDims > 0 {
			for len(g.VectorsInt32) <= cID {
				if g.Int32Arena == nil {
					slabSize := ChunkSize*paddedDims*4 + 64
					if slabSize < 1024*1024 {
						slabSize = 1024 * 1024
					}
					g.Int32Arena = memory.NewTypedArena[int32](memory.NewSlabArena(slabSize))
				}
				ref, err := g.Int32Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				g.VectorsInt32 = append(g.VectorsInt32, ref.Offset)
			}
		}
	}

	// Ensure Uint32
	if g.Type == VectorTypeUint32 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeUint32)
		if paddedDims > 0 {
			for len(g.VectorsUint32) <= cID {
				if g.Uint32Arena == nil {
					slabSize := ChunkSize*paddedDims*4 + 64
					if slabSize < 1024*1024 {
						slabSize = 1024 * 1024
					}
					g.Uint32Arena = memory.NewTypedArena[uint32](memory.NewSlabArena(slabSize))
				}
				ref, err := g.Uint32Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				g.VectorsUint32 = append(g.VectorsUint32, ref.Offset)
			}
		}
	}

	// Ensure Int16 - use arena for off-heap allocation
	if g.Type == VectorTypeInt16 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeInt16)
		if paddedDims > 0 {
			for len(g.VectorsInt16) <= cID {
				if g.Int16Arena == nil {
					slabSize := ChunkSize*paddedDims*2 + 64
					if slabSize < 1024*1024 {
						slabSize = 1024 * 1024
					}
					g.Int16Arena = memory.NewTypedArena[int16](memory.NewSlabArena(slabSize))
				}
				ref, err := g.Int16Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				g.VectorsInt16 = append(g.VectorsInt16, ref.Offset)
			}
		}
	}

	// Ensure TQ if enabled
	if g.TurboQuantEnabled {
		stride := g.PackedSize()
		if stride > 0 {
			for len(g.VectorsTQ) <= cID {
				if g.Uint8Arena == nil {
					slabSize := ChunkSize*stride + 64
					if slabSize < 1024*1024 {
						slabSize = 1024 * 1024
					}
					g.Uint8Arena = memory.NewTypedArena[uint8](memory.NewSlabArena(slabSize))
				}
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
			for len(g.VectorsUint16) <= cID {
				if g.Uint16Arena == nil {
					slabSize := ChunkSize*paddedDims*2 + 64
					if slabSize < 1024*1024 {
						slabSize = 1024 * 1024
					}
					g.Uint16Arena = memory.NewTypedArena[uint16](memory.NewSlabArena(slabSize))
				}
				ref, err := g.Uint16Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				g.VectorsUint16 = append(g.VectorsUint16, ref.Offset)
			}
		}
	}

	// Ensure Int8/Uint8
	if g.Type == VectorTypeInt8 || g.Type == VectorTypeUint8 {
		paddedDims := g.GetPaddedDimsForType(g.Type)
		if paddedDims > 0 {
			for len(g.VectorsInt8) <= cID {
				if g.Int8Arena == nil {
					slabSize := ChunkSize*paddedDims + 64
					if slabSize < 1024*1024 {
						slabSize = 1024 * 1024
					}
					g.Int8Arena = memory.NewTypedArena[int8](memory.NewSlabArena(slabSize))
				}
				ref, err := g.Int8Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				g.VectorsInt8 = append(g.VectorsInt8, ref.Offset)
			}
		}
	}

	// Ensure BQ if enabled
	if g.BQEnabled {
		paddedDims := (dims + 63) & ^63
		numWords := paddedDims / 64
		for len(g.VectorsBQ) <= cID {
			if g.Uint64Arena == nil {
				slabSize := ChunkSize*numWords*8 + 64
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				g.Uint64Arena = memory.NewTypedArena[uint64](memory.NewSlabArena(slabSize))
			}
			ref, err := g.Uint64Arena.AllocSlice(ChunkSize * numWords)
			if err != nil {
				return err
			}
			g.VectorsBQ = append(g.VectorsBQ, ref.Offset)
		}
	}

	// Ensure F16
	if g.Type == VectorTypeFloat16 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeFloat16)
		if paddedDims > 0 {
			for len(g.VectorsF16) <= cID {
				if g.Float16Arena == nil {
					slabSize := ChunkSize*paddedDims*2 + 64
					if slabSize < 1024*1024 {
						slabSize = 1024 * 1024
					}
					g.Float16Arena = memory.NewTypedArena[float16.Num](memory.NewSlabArena(slabSize))
				}
				ref, err := g.Float16Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				g.VectorsF16 = append(g.VectorsF16, ref.Offset)
			}
		}
	}

	return nil
}

func (g *GraphData) SetNeighbors(id uint32, neighbors []uint32) error {
	return g.SetNeighborsAtLayer(0, id, neighbors)
}

func (g *GraphData) SetNeighborsAtLayer(layer int, id uint32, neighbors []uint32) error {
	mu := &g.ShardedMus[id%1024]
	mu.Lock()
	defer mu.Unlock()
 
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	// Ensure chunk exists
	countsChunk := g.GetCountsChunk(layer, cID)
	neighborsChunk := g.GetNeighborsChunk(layer, cID)
	versionsChunk := g.GetVersionsChunk(layer, cID)

	if countsChunk == nil || neighborsChunk == nil {
		if err := g.EnsureChunk(cID, layer, g.Dims); err != nil {
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
		paddedDims := g.GetPaddedDimsForType(VectorTypeFloat16)
		return g.Float16Arena.Get(memory.SliceRef{Offset: g.VectorsF16[chunkID], Len: uint32(ChunkSize * paddedDims), Cap: uint32(ChunkSize * paddedDims)}) // #nosec G115
	}
	return nil
}

// GetVector returns the vector for the given ID.
func (g *GraphData) GetVector(id uint32) (any, error) {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	// Based on type, get the appropriate chunk
	// Only supporting float32 and float16 for now in this generic method
	if g.Uint8Arena != nil && len(g.VectorsSQ8) > cID && g.SQ8Enabled && atomic.LoadUint32(&g.SQ8Ready) == 1 {
		chunk := g.GetVectorsSQ8Chunk(cID)
		if chunk != nil {
			paddedDims := (g.Dims + 63) & ^63
			start := cOff * paddedDims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	switch g.Type {
	case VectorTypeUint8:
		chunk := g.GetVectorsInt8Chunk(cID)
		if chunk == nil { return nil, nil }
		pd := g.GetPaddedDimsForType(VectorTypeUint8)
		start := cOff * pd
		if start+g.Dims <= len(chunk) {
			ptr := unsafe.Pointer(&chunk[0]) // #nosec G103
			u8Chunk := unsafe.Slice((*uint8)(ptr), len(chunk)) // #nosec G103
			return u8Chunk[start : start+g.Dims], nil
		}
	case VectorTypeInt8:
		chunk := g.GetVectorsInt8Chunk(cID)
		if chunk == nil { return nil, nil }
		pd := g.GetPaddedDimsForType(VectorTypeInt8)
		start := cOff * pd
		if start+g.Dims <= len(chunk) {
			return chunk[start : start+g.Dims], nil
		}
	case VectorTypeInt16:
		chunk := g.GetVectorsInt16Chunk(cID)
		if chunk == nil { return nil, nil }
		pd := g.GetPaddedDimsForType(VectorTypeInt16)
		start := cOff * pd
		if start+g.Dims <= len(chunk) { return chunk[start : start+g.Dims], nil }
	case VectorTypeUint16:
		chunk := g.GetVectorsUint16Chunk(cID)
		if chunk == nil { return nil, nil }
		pd := g.GetPaddedDimsForType(VectorTypeUint16)
		start := cOff * pd
		if start+g.Dims <= len(chunk) { return chunk[start : start+g.Dims], nil }
	case VectorTypeInt32:
		chunk := g.GetVectorsInt32Chunk(cID)
		if chunk == nil { return nil, nil }
		pd := g.GetPaddedDimsForType(VectorTypeInt32)
		start := cOff * pd
		if start+g.Dims <= len(chunk) { return chunk[start : start+g.Dims], nil }
	case VectorTypeUint32:
		chunk := g.GetVectorsUint32Chunk(cID)
		if chunk == nil { return nil, nil }
		pd := g.GetPaddedDimsForType(VectorTypeUint32)
		start := cOff * pd
		if start+g.Dims <= len(chunk) { return chunk[start : start+g.Dims], nil }
	case VectorTypeInt64:
		chunk := g.GetVectorsInt64Chunk(cID)
		if chunk == nil { return nil, nil }
		pd := g.GetPaddedDimsForType(VectorTypeInt64)
		start := cOff * pd
		if start+g.Dims <= len(chunk) { return chunk[start : start+g.Dims], nil }
	case VectorTypeUint64:
		chunk := g.GetVectorsUint64Chunk(cID)
		if chunk == nil { return nil, nil }
		pd := g.GetPaddedDimsForType(VectorTypeUint64)
		start := cOff * pd
		if start+g.Dims <= len(chunk) { return chunk[start : start+g.Dims], nil }
	case VectorTypeFloat32:
		chunk := g.GetVectorsChunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeFloat32)
			start := cOff * paddedDims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	case VectorTypeFloat64:
		chunk := g.GetVectorsFloat64Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeFloat64)
			start := cOff * paddedDims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	case VectorTypeComplex64:
		chunk := g.GetVectorsComplex64Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeComplex64)
			start := cOff * paddedDims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	case VectorTypeComplex128:
		chunk := g.GetVectorsComplex128Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeComplex128)
			start := cOff * paddedDims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	case VectorTypeFloat16:
		chunk := g.GetVectorsF16Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeFloat16)
			start := cOff * paddedDims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	return nil, fmt.Errorf("vector type mismatch or uninitialized for ID %d (type %v)", id, g.Type)
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
				paddedDims := g.GetPaddedDimsForType(VectorTypeFloat16)
				start := cOff * paddedDims
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
			paddedDims := g.GetPaddedDimsForType(VectorTypeFloat32)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				dest := chunk[start : start+len(v)]
				// Zero-Copy Optimization: Skip copy if source and destination are the same memory
				if len(dest) > 0 && len(v) > 0 && &dest[0] == &v[0] {
					return nil
				}
				copy(dest, v)
			}
		}
	case []float16.Num:
		chunk := g.GetVectorsF16Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeFloat16)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				dest := chunk[start : start+len(v)]
				if len(dest) > 0 && len(v) > 0 && &dest[0] == &v[0] {
					return nil
				}
				copy(dest, v)
			}
		}
	case []float64:
		chunk := g.GetVectorsFloat64Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeFloat64)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				dest := chunk[start : start+len(v)]
				if len(dest) > 0 && len(v) > 0 && &dest[0] == &v[0] {
					return nil
				}
				copy(dest, v)
			}
		}
	case []complex64:
		chunk := g.GetVectorsComplex64Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeComplex64)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []complex128:
		chunk := g.GetVectorsComplex128Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeComplex128)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []uint8:
		chunk := g.GetVectorsInt8Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeUint8)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				// Copy []uint8 to []int8 chunk via unsafe cast
				uint8Chunk := *(*[]uint8)(unsafe.Pointer(&chunk)) // #nosec G103
				copy(uint8Chunk[start:start+len(v)], v)
			}
		}
	case []int8:
		chunk := g.GetVectorsInt8Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeInt8)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []int16:
		chunk := g.GetVectorsInt16Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeInt16)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []uint16:
		chunk := g.GetVectorsUint16Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeUint16)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []int64:
		chunk := g.GetVectorsInt64Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeInt64)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []uint64:
		chunk := g.GetVectorsUint64Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeUint64)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []int32:
		chunk := g.GetVectorsInt32Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeInt32)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []uint32:
		chunk := g.GetVectorsUint32Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeUint32)
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
			if len(vec) > 0 {
				simd.MemcpyNTA(unsafe.Pointer(&chunk[start]), unsafe.Pointer(&vec[0]), len(vec)*4) // #nosec G103
			}
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
			Len:    uint32(chunkLen), // #nosec G115
			Cap:    uint32(chunkLen), // #nosec G115
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
			Len:    uint32(chunkLen), // #nosec G115
			Cap:    uint32(chunkLen), // #nosec G115
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
	
	// 1. Try Lock-Free PackedNeighbors first (truly lock-free)
	if layer < len(g.PackedNeighbors) && g.PackedNeighbors[layer] != nil {
		if res, ok := g.PackedNeighbors[layer].GetNeighbors(id); ok {
			return res
		}
	}

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
// GetVersion returns the current version/lock state of a node at a given layer.
func (g *GraphData) GetVersion(layer int, id uint32) uint32 {
	versions := g.GetVersionsChunk(layer, int(id)/ChunkSize)
	if versions == nil {
		return 0
	}
	return atomic.LoadUint32(&versions[int(id)%ChunkSize])
}


// LockNode acquires a per-node spinlock.
func (g *GraphData) LockNode(layer int, id uint32) uint32 {
	versions := g.GetVersionsChunk(layer, int(id)/ChunkSize)
	if versions == nil {
		return 0
	}
	verAddr := &versions[int(id)%ChunkSize]

	var spinCycles uint64
	for {
		v := atomic.LoadUint32(verAddr)
		if v&NodeLockMask == 0 {
			if atomic.CompareAndSwapUint32(verAddr, v, v|NodeLockMask) {
				if spinCycles > 0 {
					metrics.LockNodeSpinCyclesTotal.WithLabelValues(g.Name, strconv.Itoa(layer)).Add(float64(spinCycles))
				}
				return v // Return old version for Unlock
			}
		}
		// Spin with exponential backoff
		spinCycles++
		if spinCycles < 20 {
			for i := 0; i < 10; i++ {
				simd.Pause()
			}
		} else {
			// Yield the processor to other goroutines
			runtime.Gosched()
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
	newG.Name = g.Name
	newG.GlobalVersion = atomic.LoadUint64(&g.GlobalVersion)
	newG.BackingGraph = g.BackingGraph
	newG.TurboQuantEnabled = g.TurboQuantEnabled
	newG.TurboQuantBits = g.TurboQuantBits

	// Slabs/Arenas - share with original for read access.
	// New chunks allocated via EnsureChunk will use the original's arena.
	// This is safe because COW is serialized (protected by growMu lock).
	// Clone's Vectors* slices reference chunks allocated from original's arena.
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


	// Deep copy Levels
	if g.Levels != nil {
		newG.Levels = make([][]uint8, len(g.Levels))
		for i := range g.Levels {
			if g.Levels[i] != nil {
				newG.Levels[i] = make([]uint8, len(g.Levels[i]))
				copy(newG.Levels[i], g.Levels[i])
			}
		}
	}

	// Deep copy Vectors (Slice of slices)
	if g.Vectors != nil {
		newG.Vectors = make([][]float32, len(g.Vectors))
		copy(newG.Vectors, g.Vectors)
	}

	// Deep copy Neighbors (Layer -> Chunk -> Offset)
	if g.Neighbors != nil {
		newG.Neighbors = make([][]uint64, len(g.Neighbors))
		for l := range g.Neighbors {
			if g.Neighbors[l] != nil {
				// Allocate with some headroom for future chunks
				targetCap := len(g.Neighbors[l])
				if targetCap < 16 {
					targetCap = 16
				}
				newG.Neighbors[l] = make([]uint64, len(g.Neighbors[l]), targetCap)
				copy(newG.Neighbors[l], g.Neighbors[l])
			}
		}
	}

	// Deep copy Counts (Layer -> Chunk -> Offset)
	if g.Counts != nil {
		newG.Counts = make([][]uint64, len(g.Counts))
		for l := range g.Counts {
			if g.Counts[l] != nil {
				targetCap := len(g.Counts[l])
				if targetCap < 16 {
					targetCap = 16
				}
				newG.Counts[l] = make([]uint64, len(g.Counts[l]), targetCap)
				copy(newG.Counts[l], g.Counts[l])
			}
		}
	}

	// Deep copy Versions (Layer -> Chunk -> Offset)
	if g.Versions != nil {
		newG.Versions = make([][]uint64, len(g.Versions))
		for l := range g.Versions {
			if g.Versions[l] != nil {
				targetCap := len(g.Versions[l])
				if targetCap < 16 {
					targetCap = 16
				}
				newG.Versions[l] = make([]uint64, len(g.Versions[l]), targetCap)
				copy(newG.Versions[l], g.Versions[l])
			}
		}
	}


	// Shallow copy PackedNeighbors (the structures themselves are thread-safe and manage their own growth)
	if g.PackedNeighbors != nil {
		newG.PackedNeighbors = make([]PackedNeighbors, len(g.PackedNeighbors))
		copy(newG.PackedNeighbors, g.PackedNeighbors)
	}

	// Copy Arrow References (with Retain if not nil)
	if g.ArrowRefs != nil {
		newG.ArrowRefs = make([]arrow.Array, len(g.ArrowRefs))
		for i, ref := range g.ArrowRefs {
			if ref != nil {
				ref.Retain()
				newG.ArrowRefs[i] = ref
			}
		}
	}

	// Deep copy vector offset slices
	copyOffsetSlice := func(src []uint64, enabled bool) []uint64 {
		if src == nil { 
			if enabled { return []uint64{} }
			return nil 
		}
		dst := make([]uint64, len(src))
		copy(dst, src)
		return dst
	}

	newG.VectorsF32 = copyOffsetSlice(g.VectorsF32, true)
	newG.VectorsSQ8 = copyOffsetSlice(g.VectorsSQ8, g.SQ8Enabled)
	newG.VectorsPQ = copyOffsetSlice(g.VectorsPQ, g.PQEnabled)
	newG.VectorsBQ = copyOffsetSlice(g.VectorsBQ, g.BQEnabled)
	newG.VectorsTQ = copyOffsetSlice(g.VectorsTQ, g.TurboQuantEnabled)
	newG.VectorsF16 = copyOffsetSlice(g.VectorsF16, false)
	newG.VectorsInt8 = copyOffsetSlice(g.VectorsInt8, false)
	newG.VectorsInt16 = copyOffsetSlice(g.VectorsInt16, false)
	newG.VectorsUint16 = copyOffsetSlice(g.VectorsUint16, false)
	newG.VectorsInt32 = copyOffsetSlice(g.VectorsInt32, false)
	newG.VectorsUint32 = copyOffsetSlice(g.VectorsUint32, false)
	newG.VectorsInt64 = copyOffsetSlice(g.VectorsInt64, false)
	newG.VectorsUint64 = copyOffsetSlice(g.VectorsUint64, false)
	newG.VectorsFloat64Offsets = copyOffsetSlice(g.VectorsFloat64Offsets, false)
	newG.VectorsComplex64Offsets = copyOffsetSlice(g.VectorsComplex64Offsets, false)
	newG.VectorsComplex128Offsets = copyOffsetSlice(g.VectorsComplex128Offsets, false)

	// Legacy compatibility: some older code might still check g.Vectors[chunkID] == nil
	if g.Vectors != nil {
		newG.Vectors = make([][]float32, len(g.Vectors))
		copy(newG.Vectors, g.Vectors)
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

	return newG
}

// SetExternalVectorsChunk maps a chunk of the graph directly to an external slice.
// This is used for zero-copy ingestion from Arrow buffers.
func (g *GraphData) SetExternalVectorsChunk(chunkID int, data []float32, ref arrow.Array) error {
	// Ensure metadata matches
	if g.Type != VectorTypeFloat32 && g.Type != VectorTypeUnknown {
		return fmt.Errorf("SetExternalVectorsChunk only supported for Float32 vectors")
	}
	
	// Pre-allocate legacy Vectors slice if needed
	for len(g.Vectors) <= chunkID {
		g.Vectors = append(g.Vectors, nil)
	}
	
	g.Vectors[chunkID] = data
	
	// Retain external reference to prevent premature release
	if ref != nil {
		ref.Retain()
		g.ArrowRefs = append(g.ArrowRefs, ref)
	}
	
	return nil
}


// SetZeroCopyMapping maps a chunk of the graph directly to an external slice of memory.
// This is used for Zero-Copy Arrow ingestion. It ensures that GetVectorsChunk will 
// return this external slice by bypassing the arena-based storage for this specific chunk.
func (g *GraphData) SetZeroCopyMapping(chunkID int, data []float32, ref arrow.Array) error {
	if g.Type != VectorTypeFloat32 && g.Type != VectorTypeUnknown {
		return fmt.Errorf("Zero-Copy mapping only supported for Float32 vectors")
	}

	// Step 1: Ensure legacy Vectors slice has the slot
	for len(g.Vectors) <= chunkID {
		g.Vectors = append(g.Vectors, nil)
	}
	g.Vectors[chunkID] = data

	// Step 2: Ensure arena offset slice has the slot but is set to 0 (NULL)
	// This tells GetVectorsChunk to fall back to the legacy Vectors slice.
	for len(g.VectorsF32) <= chunkID {
		g.VectorsF32 = append(g.VectorsF32, 0)
	}
	// Note: We don't overwrite if it was already allocated unless we are sure.
	g.VectorsF32[chunkID] = 0 

	// Step 3: Retain reference
	if ref != nil {
		ref.Retain()
		g.ArrowRefs = append(g.ArrowRefs, ref)
	}

	return nil
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
		// FIX: Use padded dims for SIMD alignment
		paddedDims := g.GetPaddedDimsForType(VectorTypeFloat32)
		// Use a reasonable slab size (e.g. 64MB) instead of calculating it from capacity
		slabSize := 64 * 1024 * 1024 
		if g.Float32Arena == nil {
			g.Float32Arena = memory.NewTypedArena[float32](memory.NewSlabArena(slabSize))
		}
		// Pre-allocate all chunks
		if len(g.VectorsF32) < numChunks {
			for i := len(g.VectorsF32); i < numChunks; i++ {
				ref, err := g.Float32Arena.AllocSlice(ChunkSize * paddedDims) // FIX: use AllocSlice for zeroed padding
				if err != nil {
					return err
				}
				g.VectorsF32 = append(g.VectorsF32, ref.Offset)
				if len(g.Vectors) < numChunks {
					g.Vectors = append(g.Vectors, nil)
				}
			}
		}
	}

	// Pre-allocate Float64 arena chunks
	if g.Type == VectorTypeFloat64 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeFloat64)
		requiredSize := numChunks * ChunkSize * paddedDims * 8
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Float64Arena == nil {
			g.Float64Arena = memory.NewTypedArena[float64](memory.NewSlabArena(slabSize))
		}
		if len(g.VectorsFloat64Offsets) < numChunks {
			for i := len(g.VectorsFloat64Offsets); i < numChunks; i++ {
				ref, err := g.Float64Arena.AllocSlice(ChunkSize * paddedDims) // FIX: AllocSlice for zeroed padding
				if err != nil {
					return err
				}
				g.VectorsFloat64Offsets = append(g.VectorsFloat64Offsets, ref.Offset)
			}
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
		if len(g.VectorsTQ) < numChunks {
			for i := len(g.VectorsTQ); i < numChunks; i++ {
				ref, err := g.Uint8Arena.AllocSliceDirty(ChunkSize * stride)
				if err != nil {
					return err
				}
				g.VectorsTQ = append(g.VectorsTQ, ref.Offset)
			}
		}
	}

	// Pre-allocate Complex64 arena chunks
	if g.Type == VectorTypeComplex64 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeComplex64)
		requiredSize := numChunks * ChunkSize * paddedDims * 8
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Complex64Arena == nil {
			g.Complex64Arena = memory.NewTypedArena[complex64](memory.NewSlabArena(slabSize))
		}
		if len(g.VectorsComplex64Offsets) < numChunks {
			for i := len(g.VectorsComplex64Offsets); i < numChunks; i++ {
				ref, err := g.Complex64Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				g.VectorsComplex64Offsets = append(g.VectorsComplex64Offsets, ref.Offset)
			}
		}
	}

	// Pre-allocate Complex128 arena chunks
	if g.Type == VectorTypeComplex128 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeComplex128)
		requiredSize := numChunks * ChunkSize * paddedDims * 16
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Complex128Arena == nil {
			g.Complex128Arena = memory.NewTypedArena[complex128](memory.NewSlabArena(slabSize))
		}
		if len(g.VectorsComplex128Offsets) < numChunks {
			for i := len(g.VectorsComplex128Offsets); i < numChunks; i++ {
				ref, err := g.Complex128Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				g.VectorsComplex128Offsets = append(g.VectorsComplex128Offsets, ref.Offset)
			}
		}
	}

	// Pre-allocate SQ8 arena chunks
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
		if len(g.VectorsSQ8) < numChunks {
			for i := len(g.VectorsSQ8); i < numChunks; i++ {
				ref, err := g.Uint8Arena.AllocSliceDirty(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				g.VectorsSQ8 = append(g.VectorsSQ8, ref.Offset)
			}
		}
	}

	// Pre-allocate PQ arena chunks
	if g.PQEnabled && g.PQM > 0 {
		numWordsPerNode := (g.PQM + 7) / 8
		numWords := ChunkSize * numWordsPerNode
		requiredSize := numChunks * numWords * 8
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Uint64Arena == nil {
			g.Uint64Arena = memory.NewTypedArena[uint64](memory.NewSlabArena(slabSize))
		}
		if len(g.VectorsPQ) < numChunks {
			for i := len(g.VectorsPQ); i < numChunks; i++ {
				ref, err := g.Uint64Arena.AllocSliceDirty(numWords)
				if err != nil {
					return err
				}
				g.VectorsPQ = append(g.VectorsPQ, ref.Offset)
			}
		}
	}

	// Pre-allocate BQ arena chunks
	if g.BQEnabled {
		paddedDims := (g.Dims + 63) & ^63
		numWordsPerNode := paddedDims / 64
		numWords := ChunkSize * numWordsPerNode
		requiredSize := numChunks * numWords * 8
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Uint64Arena == nil {
			g.Uint64Arena = memory.NewTypedArena[uint64](memory.NewSlabArena(slabSize))
		}
		if len(g.VectorsBQ) < numChunks {
			for i := len(g.VectorsBQ); i < numChunks; i++ {
				ref, err := g.Uint64Arena.AllocSliceDirty(numWords)
				if err != nil {
					return err
				}
				g.VectorsBQ = append(g.VectorsBQ, ref.Offset)
			}
		}
	}

	// Pre-allocate Int8 arena chunks
	if g.Type == VectorTypeInt8 || g.Type == VectorTypeUint8 {
		paddedDims := g.GetPaddedDimsForType(g.Type)
		requiredSize := numChunks * ChunkSize * paddedDims
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Int8Arena == nil {
			g.Int8Arena = memory.NewTypedArena[int8](memory.NewSlabArena(slabSize))
		}
		for i := len(g.VectorsInt8); i < numChunks; i++ {
			ref, err := g.Int8Arena.AllocSlice(ChunkSize * paddedDims)
			if err != nil {
				return err
			}
			g.VectorsInt8 = append(g.VectorsInt8, ref.Offset)
		}
	}

	// Pre-allocate Complex128 arena chunks
	if g.Type == VectorTypeComplex128 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeComplex128)
		requiredSize := numChunks * ChunkSize * paddedDims * 16
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Complex128Arena == nil {
			g.Complex128Arena = memory.NewTypedArena[complex128](memory.NewSlabArena(slabSize))
		}
		for i := len(g.VectorsComplex128Offsets); i < numChunks; i++ {
			ref, err := g.Complex128Arena.AllocSlice(ChunkSize * paddedDims)
			if err != nil {
				return err
			}
			g.VectorsComplex128Offsets = append(g.VectorsComplex128Offsets, ref.Offset)
		}
	}

	// Pre-allocate Int64 arena chunks
	if g.Type == VectorTypeInt64 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeInt64)
		requiredSize := numChunks * ChunkSize * paddedDims * 8
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Int64Arena == nil {
			g.Int64Arena = memory.NewTypedArena[int64](memory.NewSlabArena(slabSize))
		}
		for i := len(g.VectorsInt64); i < numChunks; i++ {
			ref, err := g.Int64Arena.AllocSlice(ChunkSize * paddedDims)
			if err != nil {
				return err
			}
			g.VectorsInt64 = append(g.VectorsInt64, ref.Offset)
		}
	}

	// Pre-allocate Uint64 arena chunks
	if g.Type == VectorTypeUint64 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeUint64)
		requiredSize := numChunks * ChunkSize * paddedDims * 8
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Uint64Arena == nil {
			g.Uint64Arena = memory.NewTypedArena[uint64](memory.NewSlabArena(slabSize))
		}
		for i := len(g.VectorsUint64); i < numChunks; i++ {
			ref, err := g.Uint64Arena.AllocSlice(ChunkSize * paddedDims)
			if err != nil {
				return err
			}
			g.VectorsUint64 = append(g.VectorsUint64, ref.Offset)
		}
	}

	// Pre-allocate Int32 arena chunks
	if g.Type == VectorTypeInt32 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeInt32)
		requiredSize := numChunks * ChunkSize * paddedDims * 4
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Int32Arena == nil {
			g.Int32Arena = memory.NewTypedArena[int32](memory.NewSlabArena(slabSize))
		}
		for i := len(g.VectorsInt32); i < numChunks; i++ {
			ref, err := g.Int32Arena.AllocSlice(ChunkSize * paddedDims)
			if err != nil {
				return err
			}
			g.VectorsInt32 = append(g.VectorsInt32, ref.Offset)
		}
	}

	// Pre-allocate Uint32 arena chunks
	if g.Type == VectorTypeUint32 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeUint32)
		requiredSize := numChunks * ChunkSize * paddedDims * 4
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Uint32Arena == nil {
			g.Uint32Arena = memory.NewTypedArena[uint32](memory.NewSlabArena(slabSize))
		}
		for i := len(g.VectorsUint32); i < numChunks; i++ {
			ref, err := g.Uint32Arena.AllocSlice(ChunkSize * paddedDims)
			if err != nil {
				return err
			}
			g.VectorsUint32 = append(g.VectorsUint32, ref.Offset)
		}
	}

	// Pre-allocate Int16 arena chunks
	if g.Type == VectorTypeInt16 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeInt16)
		requiredSize := numChunks * ChunkSize * paddedDims * 2
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Int16Arena == nil {
			g.Int16Arena = memory.NewTypedArena[int16](memory.NewSlabArena(slabSize))
		}
		for i := len(g.VectorsInt16); i < numChunks; i++ {
			ref, err := g.Int16Arena.AllocSlice(ChunkSize * paddedDims)
			if err != nil {
				return err
			}
			g.VectorsInt16 = append(g.VectorsInt16, ref.Offset)
		}
	}

	// Pre-allocate Uint16 arena chunks
	if g.Type == VectorTypeUint16 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeUint16)
		requiredSize := numChunks * ChunkSize * paddedDims * 2
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Uint16Arena == nil {
			g.Uint16Arena = memory.NewTypedArena[uint16](memory.NewSlabArena(slabSize))
		}
		for i := len(g.VectorsUint16); i < numChunks; i++ {
			ref, err := g.Uint16Arena.AllocSlice(ChunkSize * paddedDims)
			if err != nil {
				return err
			}
			g.VectorsUint16 = append(g.VectorsUint16, ref.Offset)
		}
	}

	// Pre-allocate Float16 arena chunks
	if g.Type == VectorTypeFloat16 {
		paddedDims := g.GetPaddedDimsForType(VectorTypeFloat16)
		requiredSize := numChunks * ChunkSize * paddedDims * 2
		slabSize := requiredSize + 4096
		if slabSize < 1024*1024 {
			slabSize = 1024 * 1024
		}
		if g.Float16Arena == nil {
			g.Float16Arena = memory.NewTypedArena[float16.Num](memory.NewSlabArena(slabSize))
		}
		for i := len(g.VectorsF16); i < numChunks; i++ {
			ref, err := g.Float16Arena.AllocSlice(ChunkSize * paddedDims)
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
		if len(g.VectorsSQ8) < numChunks {
			for i := len(g.VectorsSQ8); i < numChunks; i++ {
				ref, err := g.Uint8Arena.AllocSlice(ChunkSize * paddedDims)
				if err != nil {
					return err
				}
				g.VectorsSQ8 = append(g.VectorsSQ8, ref.Offset)
			}
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
		if len(g.VectorsBQ) < numChunks {
			for i := len(g.VectorsBQ); i < numChunks; i++ {
				ref, err := g.Uint64Arena.AllocSliceDirty(ChunkSize * numWords)
				if err != nil {
					return err
				}
				g.VectorsBQ = append(g.VectorsBQ, ref.Offset)
			}
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
		if len(g.VectorsPQ) < numChunks {
			for i := len(g.VectorsPQ); i < numChunks; i++ {
				ref, err := g.Uint64Arena.AllocSliceDirty(ChunkSize * numWords)
				if err != nil {
					return err
				}
				g.VectorsPQ = append(g.VectorsPQ, ref.Offset)
			}
		}
	}

	// Pre-allocate Levels for all chunks
	if len(g.Levels) < numChunks {
		for i := len(g.Levels); i < numChunks; i++ {
			g.Levels = append(g.Levels, make([]uint8, ChunkSize))
		}
	}

	// Pre-allocate Neighbors, Counts, Versions for all layers
	if len(g.Neighbors) == 0 {
		g.Neighbors = make([][]uint64, ArrowMaxLayers)
		g.Counts = make([][]uint64, ArrowMaxLayers)
		g.Versions = make([][]uint64, ArrowMaxLayers)
	}
	// Optimized: Only pre-allocate layer 0. Higher layers remain lazy.
	layer := 0
	if len(g.Neighbors[layer]) < numChunks {
		delta := numChunks - len(g.Neighbors[layer])
		g.Neighbors[layer] = append(g.Neighbors[layer], make([]uint64, delta)...)
		g.Counts[layer] = append(g.Counts[layer], make([]uint64, delta)...)
		g.Versions[layer] = append(g.Versions[layer], make([]uint64, delta)...)
		
		for i := numChunks - delta; i < numChunks; i++ {
			// Ensure arenas exist
			if g.Uint32Arena == nil {
				g.Uint32Arena = memory.NewTypedArena[uint32](memory.NewSlabArena(16 * 1024 * 1024))
			}
			if g.Int32Arena == nil {
				g.Int32Arena = memory.NewTypedArena[int32](memory.NewSlabArena(1024 * 1024))
			}
			
			// Neighbors
			refN, _ := g.Uint32Arena.AllocSlice(ChunkSize * MaxNeighbors)
			g.Neighbors[layer][i] = refN.Offset
			
			// Counts
			refC, _ := g.Int32Arena.AllocSlice(ChunkSize)
			g.Counts[layer][i] = refC.Offset
			
			// Versions
			refV, _ := g.Uint32Arena.AllocSlice(ChunkSize)
			g.Versions[layer][i] = refV.Offset
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
	tqEnabled bool, tqBits int, name string) *GraphData {

	// Enforce minimum capacity to avoid rapid initial COW cycles
	if capacity < 1024 {
		capacity = 1024
	}
	
	var f32Arena, u8Arena, f64Arena, i8Arena, c64Arena, c128Arena, i64Arena, i16Arena, u16Arena, i32Arena, f16Arena, u64Arena, u32Arena *memory.SlabArena
	if dim > 0 {
		minSlabSize := 16 * 1024 * 1024 // 16MB minimum for all arenas

		f32SlabSize := ChunkSize*dim*4 + 64
		if f32SlabSize < minSlabSize { f32SlabSize = minSlabSize }
		f32Arena = memory.NewSlabArena(f32SlabSize)

		u8SlabSize := ChunkSize*dim + 64
		if u8SlabSize < minSlabSize { u8SlabSize = minSlabSize }
		u8Arena = memory.NewSlabArena(u8SlabSize)

		f64SlabSize := ChunkSize*dim*8 + 64
		if f64SlabSize < minSlabSize { f64SlabSize = minSlabSize }
		f64Arena = memory.NewSlabArena(f64SlabSize)

		i8Arena = memory.NewSlabArena(u8SlabSize)

		c64SlabSize := ChunkSize*dim*8 + 64
		if c64SlabSize < minSlabSize { c64SlabSize = minSlabSize }
		c64Arena = memory.NewSlabArena(c64SlabSize)

		c128SlabSize := ChunkSize*dim*16 + 64
		if c128SlabSize < minSlabSize { c128SlabSize = minSlabSize }
		c128Arena = memory.NewSlabArena(c128SlabSize)

		i64SlabSize := ChunkSize*dim*8 + 64
		if i64SlabSize < minSlabSize { i64SlabSize = minSlabSize }
		i64Arena = memory.NewSlabArena(i64SlabSize)

		i16SlabSize := ChunkSize*dim*2 + 64
		if i16SlabSize < minSlabSize { i16SlabSize = minSlabSize }
		i16Arena = memory.NewSlabArena(i16SlabSize)

		u16SlabSize := ChunkSize*dim*2 + 64
		if u16SlabSize < minSlabSize { u16SlabSize = minSlabSize }
		u16Arena = memory.NewSlabArena(u16SlabSize)

		i32SlabSize := ChunkSize*dim*4 + 64
		if i32SlabSize < minSlabSize { i32SlabSize = minSlabSize }
		i32Arena = memory.NewSlabArena(i32SlabSize)

		f16SlabSize := ChunkSize*dim*2 + 64
		if f16SlabSize < minSlabSize { f16SlabSize = minSlabSize }
		f16Arena = memory.NewSlabArena(f16SlabSize)

		u64Arena = memory.NewSlabArena(minSlabSize)
		u32Arena = memory.NewSlabArena(minSlabSize)
	} else {
		u64Arena = memory.NewSlabArena(16 * 1024 * 1024)
		u32Arena = memory.NewSlabArena(16 * 1024 * 1024)
	}


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
		Name:              name,
		Vectors:           make([][]float32, numChunks),
		VectorsFloat64:    make([][]float64, numChunks),
		VectorsComplex64:  make([][]complex64, numChunks),
		VectorsComplex128: make([][]complex128, numChunks),
		TurboQuantEnabled: tqEnabled,
		TurboQuantBits:    tqBits,
		Neighbors:         make([][]uint64, ArrowMaxLayers),
		Counts:            make([][]uint64, ArrowMaxLayers),
		Versions:          make([][]uint64, ArrowMaxLayers),
		Levels:            make([][]uint8, 0, numChunks),
		VectorsTQ:         nil,
		VectorsPQ:         nil,
		VectorsSQ8:        nil,
		VectorsBQ:         nil,
		VectorsF16:        nil,
		VectorsF32:         make([]uint64, 0, numChunks),
	}

	if f32Arena != nil {
		gd.Float32Arena = memory.NewTypedArena[float32](f32Arena)
	}
	if u8Arena != nil {
		gd.Uint8Arena = memory.NewTypedArena[uint8](u8Arena)
	}
	if f64Arena != nil {
		gd.Float64Arena = memory.NewTypedArena[float64](f64Arena)
	}
	if i8Arena != nil {
		gd.Int8Arena = memory.NewTypedArena[int8](i8Arena)
	}
	if i64Arena != nil {
		gd.Int64Arena = memory.NewTypedArena[int64](i64Arena)
	}
	if i16Arena != nil {
		gd.Int16Arena = memory.NewTypedArena[int16](i16Arena)
	}
	if u16Arena != nil {
		gd.Uint16Arena = memory.NewTypedArena[uint16](u16Arena)
	}
	if i32Arena != nil {
		gd.Int32Arena = memory.NewTypedArena[int32](i32Arena)
	}
	if f16Arena != nil {
		gd.Float16Arena = memory.NewTypedArena[float16.Num](f16Arena)
	}
	if c64Arena != nil {
		gd.Complex64Arena = memory.NewTypedArena[complex64](c64Arena)
	}
	if c128Arena != nil {
		gd.Complex128Arena = memory.NewTypedArena[complex128](c128Arena)
	}
	if u64Arena != nil {
		gd.Uint64Arena = memory.NewTypedArena[uint64](u64Arena)
	}
	if u32Arena != nil {
		gd.Uint32Arena = memory.NewTypedArena[uint32](u32Arena)
	}

	for i := 0; i < ArrowMaxLayers; i++ {
		gd.Neighbors[i] = make([]uint64, 0, numChunks)
		gd.Counts[i] = make([]uint64, 0, numChunks)
		gd.Versions[i] = make([]uint64, 0, numChunks)
	}

	// Pre-allocate chunks for the given capacity to avoid lazy allocation overhead
	if capacity > 0 && dim > 0 {
		_ = gd.PreAllocate(capacity)
	}

	return gd
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
		newL := make([][]uint8, numChunks)
		copy(newL, g.Levels)
		g.Levels = newL
	}
}

func (g *GraphData) Release() {
	// Release Arrow references
	for _, ref := range g.ArrowRefs {
		if ref != nil {
			ref.Release()
		}
	}
	g.ArrowRefs = nil
	g.Vectors = nil

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
