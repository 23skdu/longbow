package types

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"runtime"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/float16"
	arrowmemory "github.com/apache/arrow-go/v18/arrow/memory"
)

var debugRelease = os.Getenv("LONGBOW_DEBUG_RELEASE") != ""

// PaddedMutex is a sync.Mutex padded to a full 64-byte cache line to prevent false sharing.
type PaddedMutex struct {
	sync.Mutex
	_ [56]byte // Padding to 64 bytes (assuming 8-byte mutex)
}

// GraphData holds the vector data and graph topology.
// It effectively implements the component storage for ArrowHNSW.
type GraphData struct {
	// Metadata
	Capacity      int                   // Total number of nodes the graph can currently hold.
	Dims          int                   // Number of dimensions for the vectors.
	Type          VectorDataType        // Underlying data type of the vectors.
	SQ8Enabled    bool                  // Whether Scalar Quantization (8-bit) is enabled.
	SQ8Ready      uint32                // 0=not ready, 1=ready (atomic).
	BQEnabled     bool                  // Whether Binary Quantization is enabled.
	PQEnabled     bool                  // Whether Product Quantization is enabled.
	PQM           int                   // Number of sub-spaces for Product Quantization.
	GlobalVersion uint64                // Incremented on structural changes for cache validation.
	BackingGraph  any                   // Interface to a persistent storage (e.g., *DiskGraph).
	Name          string                // Unique identifier for the dataset (used in metrics).
	Allocator     arrowmemory.Allocator // Optional allocator for NUMA-aware memory placement.

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
	Levels [][]uint32

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

	SharedVectorSpace bool // If true, skip primary vector storage allocation

	cloneCount  int32  // Atomic: incremented during Clone, checked by Release before freeing
	readerCount int32  // Atomic: incremented by AcquireReader on read paths; Release waits for 0 before freeing typed arenas
	released    uint32 // Atomic flag to prevent double-release/idempotency

	// OnNeighborsMiss is a callback hook triggered when neighbor data for a layer is accessed but evicted (offset == 0).
	OnNeighborsMiss func(layer int) error

	// OnEvict is a callback hook triggered when a layer is evicted.
	OnEvict func(layer int)
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
	// Release frees the underlying memory resources.
	Release()
	// Retain increments the reference count of the structure.
	Retain()
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
	// GetNeighborsWithGen returns the neighbor list for a node with generation isolation.
	GetNeighborsWithGen(id uint32, maxGen uint64) ([]uint32, bool)
	// GetNeighborsF16WithGen returns neighbors and their distances with generation isolation.
	GetNeighborsF16WithGen(id uint32, maxGen uint64) ([]uint32, []float16.Num, bool)
	// GetNeighborsFromPackedWithGen extracts neighbors from a packed uint64 with generation isolation.
	GetNeighborsFromPackedWithGen(packed uint64, maxGen uint64) []uint32
	// IsOffHeap returns true if the backing storage is off-heap.
	IsOffHeap() bool
	// EvictToDisk writes all neighbor chunks for the given layer to w
	// and clears in-memory storage. Returns (numChunks, chunkSizes, bytesWritten, error).
	// chunkSizes is pre-allocated; the implementation writes into it and may grow
	// it as needed. The caller uses the returned slice.
	EvictToDisk(gd *GraphData, layer int, chunkSizes []int, w interface{ Write([]byte) (int, error) }) (nChunks int, outChunkSizes []int, bytesWritten int64, err error)
	// RestoreFromDisk reads neighbor chunks back from r, repopulating storage.
	RestoreFromDisk(gd *GraphData, layer int, chunkSizes []int, r interface{ Read([]byte) (int, error) }) error
}

// GetNodeCount returns the current capacity of the graph (number of addressable nodes).
func (g *GraphData) GetNodeCount() int {
	return g.Capacity
}

// BumpGeneration increments the generation for all arenas in the graph.
func (g *GraphData) BumpGeneration() uint64 {
	gen := atomic.AddUint64(&g.GlobalVersion, 1)
	g.SetGeneration(gen)
	return gen
}

// SetGeneration sets the generation for all arenas in the graph.
func (g *GraphData) SetGeneration(gen uint64) {
	if g.Float32Arena != nil {
		g.Float32Arena.SetGeneration(gen)
	}
	if g.Float64Arena != nil {
		g.Float64Arena.SetGeneration(gen)
	}
	if g.Uint8Arena != nil {
		g.Uint8Arena.SetGeneration(gen)
	}
	if g.Uint16Arena != nil {
		g.Uint16Arena.SetGeneration(gen)
	}
	if g.Uint32Arena != nil {
		g.Uint32Arena.SetGeneration(gen)
	}
	if g.Uint64Arena != nil {
		g.Uint64Arena.SetGeneration(gen)
	}
	if g.Int8Arena != nil {
		g.Int8Arena.SetGeneration(gen)
	}
	if g.Int16Arena != nil {
		g.Int16Arena.SetGeneration(gen)
	}
	if g.Int32Arena != nil {
		g.Int32Arena.SetGeneration(gen)
	}
	if g.Int64Arena != nil {
		g.Int64Arena.SetGeneration(gen)
	}
	if g.Float16Arena != nil {
		g.Float16Arena.SetGeneration(gen)
	}
	if g.Complex64Arena != nil {
		g.Complex64Arena.SetGeneration(gen)
	}
	if g.Complex128Arena != nil {
		g.Complex128Arena.SetGeneration(gen)
	}
}

func (g *GraphData) NeedsChunk(cID int) bool {
	// 0. Topology check (Levels are always needed locally)
	// If Levels hasn't been preallocated for this cID yet, wait for GrowMetadataSlices
	if cID >= len(g.Levels) || g.Levels[cID] == nil {
		return true
	}
	for l := range g.Neighbors {
		// Only layer 0 still uses gd.Neighbors pre-allocation; upper layers use
		// PackedNeighbors/FlatAdjacency and are allocated on demand.
		if l == 0 {
			if cID >= len(g.Neighbors[l]) || atomic.LoadUint64(&g.Neighbors[l][cID]) == 0 {
				return true
			}
		}
		if cID >= len(g.Counts[l]) || atomic.LoadUint64(&g.Counts[l][cID]) == 0 {
			return true
		}
		if cID >= len(g.Versions[l]) || atomic.LoadUint64(&g.Versions[l][cID]) == 0 {
			return true
		}
	}

	// If using shared vector space, we don't need local vector chunks
	if g.SharedVectorSpace {
		return false
	}

	// 1. Primary Float32 check
	if !g.SharedVectorSpace && (g.Type == VectorTypeFloat32 || g.Type == VectorTypeUnknown) {
		if cID >= len(g.VectorsF32) || atomic.LoadUint64(&g.VectorsF32[cID]) == 0 {
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
		if cID >= len(g.Neighbors[0]) || atomic.LoadUint64(&g.Neighbors[0][cID]) == 0 {
			return true
		}
	}

	return false
}

// GetVectorsChunk returns the vector chunk for the given ID.
func (g *GraphData) GetVectorsChunk(chunkID int) []float32 {
	return g.GetVectorsChunkWithGen(chunkID, math.MaxUint64)
}

// GetVectorsChunkWithGen returns the vector chunk for the given ID with generation isolation.
// Uses non-atomic offset read for performance. Chunk offset arrays are written once by
// EnsureChunk and stable during search; the arena handles concurrent safety internally.
func (g *GraphData) GetVectorsChunkWithGen(chunkID int, maxGen uint64) []float32 {
	// Try arena first (off-heap, GC-free)
	if g.Float32Arena != nil && chunkID < len(g.VectorsF32) {
		pd := g.GetPaddedDimsForType(VectorTypeFloat32)
		offset := atomic.LoadUint64(&g.VectorsF32[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Float32Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}, maxGen) // #nosec G115
	}
	// Fallback to legacy slice
	if chunkID < len(g.Vectors) {
		return g.Vectors[chunkID]
	}
	return nil
}

// GetVectorsChunkFast returns the vector chunk using a non-atomic offset read.
// Safe because chunk offset arrays are written once by EnsureChunk and are
// GetVectorsChunkFast returns the vector chunk using a non-atomic offset read.
// This is an optimization for search threads. For committed data (no generation isolation).
func (g *GraphData) GetVectorsChunkFast(chunkID int) []float32 {
	if g.Float32Arena != nil && chunkID < len(g.VectorsF32) {
		pd := g.GetPaddedDimsForType(VectorTypeFloat32)
		offset := atomic.LoadUint64(&g.VectorsF32[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Float32Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	if chunkID < len(g.Vectors) {
		return g.Vectors[chunkID]
	}
	return nil
}

// GetVectorsChunkFastWithGen returns the vector chunk using an atomic offset read
// with generation isolation.
func (g *GraphData) GetVectorsChunkFastWithGen(chunkID int, maxGen uint64) []float32 {
	if g.Float32Arena != nil && chunkID < len(g.VectorsF32) {
		pd := g.GetPaddedDimsForType(VectorTypeFloat32)
		offset := atomic.LoadUint64(&g.VectorsF32[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Float32Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}, maxGen) // #nosec G115
	}
	if chunkID < len(g.Vectors) {
		return g.Vectors[chunkID]
	}
	return nil
}

// GetVectorsTQChunkFast returns a TurboQuant chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsTQChunkFast(chunkID int) []byte {
	if chunkID < len(g.VectorsTQ) && g.Uint8Arena != nil {
		stride := g.PackedSize()
		offset := atomic.LoadUint64(&g.VectorsTQ[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint8Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * stride), Cap: uint32(ChunkSize * stride)}) // #nosec G115
	}
	return nil
}

// GetVectorsFloat64ChunkFast returns a float64 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsFloat64ChunkFast(chunkID int) []float64 {
	if chunkID < len(g.VectorsFloat64Offsets) && g.Float64Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeFloat64)
		offset := atomic.LoadUint64(&g.VectorsFloat64Offsets[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Float64Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	if chunkID < len(g.VectorsFloat64) {
		return g.VectorsFloat64[chunkID]
	}
	return nil
}

// GetVectorsComplex64ChunkFast returns a complex64 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsComplex64ChunkFast(chunkID int) []complex64 {
	if chunkID < len(g.VectorsComplex64Offsets) && g.Complex64Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeComplex64)
		offset := atomic.LoadUint64(&g.VectorsComplex64Offsets[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Complex64Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	if chunkID < len(g.VectorsComplex64) {
		return g.VectorsComplex64[chunkID]
	}
	return nil
}

// GetVectorsComplex128ChunkFast returns a complex128 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsComplex128ChunkFast(chunkID int) []complex128 {
	if chunkID < len(g.VectorsComplex128Offsets) && g.Complex128Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeComplex128)
		offset := atomic.LoadUint64(&g.VectorsComplex128Offsets[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Complex128Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	if chunkID < len(g.VectorsComplex128) {
		return g.VectorsComplex128[chunkID]
	}
	return nil
}

// GetVectorsInt64ChunkFast returns an int64 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsInt64ChunkFast(chunkID int) []int64 {
	if chunkID < len(g.VectorsInt64) && g.Int64Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeInt64)
		offset := atomic.LoadUint64(&g.VectorsInt64[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Int64Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	return nil
}

// GetVectorsUint64ChunkFast returns a uint64 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsUint64ChunkFast(chunkID int) []uint64 {
	if chunkID < len(g.VectorsUint64) && g.Uint64Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeUint64)
		offset := atomic.LoadUint64(&g.VectorsUint64[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint64Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	return nil
}

// GetVectorsInt32ChunkFast returns an int32 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsInt32ChunkFast(chunkID int) []int32 {
	if chunkID < len(g.VectorsInt32) && g.Int32Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeInt32)
		offset := atomic.LoadUint64(&g.VectorsInt32[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Int32Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	return nil
}

// GetVectorsUint32ChunkFast returns a uint32 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsUint32ChunkFast(chunkID int) []uint32 {
	if chunkID < len(g.VectorsUint32) && g.Uint32Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeUint32)
		offset := atomic.LoadUint64(&g.VectorsUint32[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint32Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	return nil
}

// GetVectorsInt8ChunkFast returns an int8 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsInt8ChunkFast(chunkID int) []int8 {
	if chunkID < len(g.VectorsInt8) && g.Int8Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeInt8)
		offset := atomic.LoadUint64(&g.VectorsInt8[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Int8Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	return nil
}

// GetVectorsUint8ChunkFast returns a uint8 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsUint8ChunkFast(chunkID int) []uint8 {
	chunk := g.GetVectorsInt8ChunkFast(chunkID)
	if chunk == nil {
		return nil
	}
	ptr := unsafe.Pointer(&chunk[0])               // #nosec G103
	return unsafe.Slice((*uint8)(ptr), len(chunk)) // #nosec G103
}

// GetVectorsInt16ChunkFast returns an int16 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsInt16ChunkFast(chunkID int) []int16 {
	if chunkID < len(g.VectorsInt16) && g.Int16Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeInt16)
		offset := atomic.LoadUint64(&g.VectorsInt16[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Int16Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	return nil
}

// GetVectorsUint16ChunkFast returns a uint16 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsUint16ChunkFast(chunkID int) []uint16 {
	if chunkID < len(g.VectorsUint16) && g.Uint16Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeUint16)
		offset := atomic.LoadUint64(&g.VectorsUint16[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint16Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	return nil
}

// GetVectorsF16ChunkFast returns a float16 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsF16ChunkFast(chunkID int) []float16.Num {
	if chunkID < len(g.VectorsF16) && g.Float16Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeFloat16)
		offset := atomic.LoadUint64(&g.VectorsF16[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Float16Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	return nil
}

// GetVectorsSQ8ChunkFast returns an SQ8 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsSQ8ChunkFast(chunkID int) []byte {
	if chunkID < len(g.VectorsSQ8) && g.Uint8Arena != nil {
		paddedDims := (g.Dims + 63) & ^63
		offset := atomic.LoadUint64(&g.VectorsSQ8[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint8Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * paddedDims), Cap: uint32(ChunkSize * paddedDims)}) // #nosec G115
	}
	return nil
}

// GetVectorsBQChunkFast returns a BQ chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsBQChunkFast(chunkID int) []uint64 {
	if g.Uint64Arena != nil && chunkID < len(g.VectorsBQ) {
		paddedDims := (g.Dims + 63) & ^63
		numWordsPerNode := paddedDims / 64
		chunkLen := ChunkSize * numWordsPerNode
		if chunkLen == 0 {
			return nil
		}
		offset := atomic.LoadUint64(&g.VectorsBQ[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint64Arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(chunkLen), // #nosec G115
			Cap:    uint32(chunkLen), // #nosec G115
		})
	}
	return nil
}

// GetVectorsPQChunkFast returns a PQ chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsPQChunkFast(chunkID int) []byte {
	if g.Uint64Arena != nil && chunkID < len(g.VectorsPQ) && g.PQM > 0 {
		numWordsPerNode := (g.PQM + 7) / 8
		numWords := ChunkSize * numWordsPerNode
		offset := atomic.LoadUint64(&g.VectorsPQ[chunkID])
		if offset == 0 {
			return nil
		}
		chunk := g.Uint64Arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(numWords), // #nosec G115
			Cap:    uint32(numWords), // #nosec G115
		})
		if len(chunk) == 0 {
			return nil
		}
		ptr := unsafe.Pointer(&chunk[0])              // #nosec G103
		return unsafe.Slice((*byte)(ptr), numWords*8) // #nosec G103
	}
	return nil
}

// GetCountsChunkFast returns a counts chunk using a non-atomic offset read.
func (g *GraphData) GetCountsChunkFast(layer, chunkID int) []int32 {
	if layer < len(g.Counts) && chunkID < len(g.Counts[layer]) && g.Int32Arena != nil {
		offset := atomic.LoadUint64(&g.Counts[layer][chunkID])
		if offset == 0 {
			return nil
		}
		return g.Int32Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize), Cap: uint32(ChunkSize)}) // #nosec G115
	}
	return nil
}

// GetNeighborsChunkFast returns a neighbors chunk using a non-atomic offset read.
func (g *GraphData) GetNeighborsChunkFast(layer, chunkID int) []uint32 {
	if layer < len(g.Neighbors) && chunkID < len(g.Neighbors[layer]) && g.Uint32Arena != nil {
		offset := atomic.LoadUint64(&g.Neighbors[layer][chunkID])
		if offset == 0 && g.OnNeighborsMiss != nil {
			_ = g.OnNeighborsMiss(layer)
			offset = atomic.LoadUint64(&g.Neighbors[layer][chunkID])
		}
		if offset == 0 {
			return nil
		}
		return g.Uint32Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * MaxNeighbors), Cap: uint32(ChunkSize * MaxNeighbors)}) // #nosec G115
	}
	return nil
}

// GetVersionsChunkFast returns a versions chunk using a non-atomic offset read.
func (g *GraphData) GetVersionsChunkFast(layer, chunkID int) []uint32 {
	if layer < len(g.Versions) && chunkID < len(g.Versions[layer]) && g.Uint32Arena != nil {
		offset := atomic.LoadUint64(&g.Versions[layer][chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint32Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize), Cap: uint32(ChunkSize)}) // #nosec G115
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
	size := 4 + angleBytes + bitBytes
	return (size + 3) &^ 3 // Pad to 4 bytes for GPU alignment
}

// GetVectorsTQChunk returns a chunk of TurboQuant compressed vectors.
func (g *GraphData) GetVectorsTQChunk(chunkID int) []byte {
	return g.GetVectorsTQChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsTQChunkWithGen(chunkID int, maxGen uint64) []byte {
	if chunkID < len(g.VectorsTQ) && g.Uint8Arena != nil {
		stride := g.PackedSize()
		offset := atomic.LoadUint64(&g.VectorsTQ[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint8Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * stride), Cap: uint32(ChunkSize * stride)}, maxGen) // #nosec G115
	}
	return nil
}

// GetVectorsFloat64Chunk returns a chunk of float64 vectors.
func (g *GraphData) GetVectorsFloat64Chunk(chunkID int) []float64 {
	return g.GetVectorsFloat64ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsFloat64ChunkWithGen(chunkID int, maxGen uint64) []float64 {
	if chunkID < len(g.VectorsFloat64Offsets) && g.Float64Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeFloat64)
		offset := atomic.LoadUint64(&g.VectorsFloat64Offsets[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Float64Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}, maxGen) // #nosec G115
	}
	if chunkID < len(g.VectorsFloat64) {
		return g.VectorsFloat64[chunkID]
	}
	return nil
}

// GetVectorsComplex64Chunk returns a chunk of complex64 vectors.
func (g *GraphData) GetVectorsComplex64Chunk(chunkID int) []complex64 {
	return g.GetVectorsComplex64ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsComplex64ChunkWithGen(chunkID int, maxGen uint64) []complex64 {
	if chunkID < len(g.VectorsComplex64Offsets) && g.Complex64Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeComplex64)
		offset := atomic.LoadUint64(&g.VectorsComplex64Offsets[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Complex64Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}, maxGen) // #nosec G115
	}
	if chunkID < len(g.VectorsComplex64) {
		return g.VectorsComplex64[chunkID]
	}
	return nil
}

// GetVectorsComplex128Chunk returns a chunk of complex128 vectors.
func (g *GraphData) GetVectorsComplex128Chunk(chunkID int) []complex128 {
	return g.GetVectorsComplex128ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsComplex128ChunkWithGen(chunkID int, maxGen uint64) []complex128 {
	if chunkID < len(g.VectorsComplex128Offsets) && g.Complex128Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeComplex128)
		offset := atomic.LoadUint64(&g.VectorsComplex128Offsets[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Complex128Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}, maxGen) // #nosec G115
	}
	if chunkID < len(g.VectorsComplex128) {
		return g.VectorsComplex128[chunkID]
	}
	return nil
}

// GetVectorsInt64Chunk returns a chunk of int64 vectors.
func (g *GraphData) GetVectorsInt64Chunk(chunkID int) []int64 {
	return g.GetVectorsInt64ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsInt64ChunkWithGen(chunkID int, maxGen uint64) []int64 {
	if chunkID < len(g.VectorsInt64) && g.Int64Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeInt64)
		offset := atomic.LoadUint64(&g.VectorsInt64[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Int64Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}, maxGen) // #nosec G115
	}
	return nil
}

// GetVectorsUint64Chunk returns a chunk of uint64 vectors.
func (g *GraphData) GetVectorsUint64Chunk(chunkID int) []uint64 {
	return g.GetVectorsUint64ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsUint64ChunkWithGen(chunkID int, maxGen uint64) []uint64 {
	if chunkID < len(g.VectorsUint64) && g.Uint64Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeUint64)
		offset := atomic.LoadUint64(&g.VectorsUint64[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint64Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}, maxGen) // #nosec G115
	}
	return nil
}

// GetVectorsInt32Chunk returns a chunk of int32 vectors.
func (g *GraphData) GetVectorsInt32Chunk(chunkID int) []int32 {
	return g.GetVectorsInt32ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsInt32ChunkWithGen(chunkID int, maxGen uint64) []int32 {
	if chunkID < len(g.VectorsInt32) && g.Int32Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeInt32)
		offset := atomic.LoadUint64(&g.VectorsInt32[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Int32Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}, maxGen) // #nosec G115
	}
	return nil
}

// GetVectorsUint32Chunk returns a chunk of uint32 vectors.
func (g *GraphData) GetVectorsUint32Chunk(chunkID int) []uint32 {
	return g.GetVectorsUint32ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsUint32ChunkWithGen(chunkID int, maxGen uint64) []uint32 {
	if chunkID < len(g.VectorsUint32) && g.Uint32Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeUint32)
		offset := atomic.LoadUint64(&g.VectorsUint32[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint32Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}, maxGen) // #nosec G115
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
	return g.GetVectorsSQ8ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsSQ8ChunkWithGen(chunkID int, maxGen uint64) []byte {
	if chunkID < len(g.VectorsSQ8) && g.Uint8Arena != nil {
		paddedDims := (g.Dims + 63) & ^63
		offset := atomic.LoadUint64(&g.VectorsSQ8[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint8Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * paddedDims), Cap: uint32(ChunkSize * paddedDims)}, maxGen) // #nosec G115
	}
	return nil
}

func (g *GraphData) GetVectorsBQChunk(chunkID int) []uint64 {
	return g.GetVectorsBQChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsBQChunkWithGen(chunkID int, maxGen uint64) []uint64 {
	if g.Uint64Arena != nil && chunkID < len(g.VectorsBQ) {
		paddedDims := (g.Dims + 63) & ^63
		numWordsPerNode := paddedDims / 64
		chunkLen := ChunkSize * numWordsPerNode
		if chunkLen == 0 {
			return nil
		}

		offset := atomic.LoadUint64(&g.VectorsBQ[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint64Arena.GetWithGeneration(memory.SliceRef{
			Offset: offset,
			Len:    uint32(chunkLen), // #nosec G115
			Cap:    uint32(chunkLen), // #nosec G115
		}, maxGen)
	}
	return nil
}

// GetVectorsPQChunk returns the PQ vectors chunk for the given ID.
func (g *GraphData) GetVectorsPQChunk(chunkID int) []byte {
	return g.GetVectorsPQChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsPQChunkWithGen(chunkID int, maxGen uint64) []byte {
	if g.Uint64Arena != nil && chunkID < len(g.VectorsPQ) && g.PQM > 0 {
		numWordsPerNode := (g.PQM + 7) / 8
		numWords := ChunkSize * numWordsPerNode

		offset := atomic.LoadUint64(&g.VectorsPQ[chunkID])
		if offset == 0 {
			return nil
		}
		chunk := g.Uint64Arena.GetWithGeneration(memory.SliceRef{
			Offset: offset,
			Len:    uint32(numWords), // #nosec G115
			Cap:    uint32(numWords), // #nosec G115
		}, maxGen)

		if len(chunk) == 0 {
			return nil
		}

		// Cast uint64 to byte slice
		ptr := unsafe.Pointer(&chunk[0])              // #nosec G103
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
		ptr := unsafe.Pointer(&chunk[0])                    // #nosec G103
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
	return g.GetCountsChunkWithGen(layer, chunkID, math.MaxUint64)
}

func (g *GraphData) GetCountsChunkWithGen(layer, chunkID int, maxGen uint64) []int32 {
	if layer < len(g.Counts) && chunkID < len(g.Counts[layer]) && g.Int32Arena != nil {
		offset := atomic.LoadUint64(&g.Counts[layer][chunkID])
		if offset == 0 {
			return nil
		}
		return g.Int32Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize), Cap: uint32(ChunkSize)}, maxGen) // #nosec G115
	}
	return nil
}

func (g *GraphData) GetNeighborsChunk(layer, chunkID int) []uint32 {
	return g.GetNeighborsChunkWithGen(layer, chunkID, math.MaxUint64)
}

func (g *GraphData) GetNeighborsChunkWithGen(layer, chunkID int, maxGen uint64) []uint32 {
	if layer < len(g.Neighbors) && chunkID < len(g.Neighbors[layer]) && g.Uint32Arena != nil {
		offset := atomic.LoadUint64(&g.Neighbors[layer][chunkID])
		if offset == 0 && g.OnNeighborsMiss != nil {
			_ = g.OnNeighborsMiss(layer)
			offset = atomic.LoadUint64(&g.Neighbors[layer][chunkID])
		}
		if offset == 0 {
			return nil
		}
		return g.Uint32Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * MaxNeighbors), Cap: uint32(ChunkSize * MaxNeighbors)}, maxGen) // #nosec G115
	}
	return nil
}

func (g *GraphData) GetVersionsChunk(layer, chunkID int) []uint32 {
	return g.GetVersionsChunkWithGen(layer, chunkID, math.MaxUint64)
}

func (g *GraphData) GetVersionsChunkWithGen(layer, chunkID int, maxGen uint64) []uint32 {
	if layer < len(g.Versions) && chunkID < len(g.Versions[layer]) && g.Uint32Arena != nil {
		offset := atomic.LoadUint64(&g.Versions[layer][chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint32Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize), Cap: uint32(ChunkSize)}, maxGen) // #nosec G115
	}
	return nil
}

func (g *GraphData) GetVectorsInt8Chunk(chunkID int) []int8 {
	return g.GetVectorsInt8ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsInt8ChunkWithGen(chunkID int, maxGen uint64) []int8 {
	if chunkID < len(g.VectorsInt8) && g.Int8Arena != nil {
		paddedDims := g.GetPaddedDimsForType(VectorTypeInt8)
		return g.Int8Arena.GetWithGeneration(memory.SliceRef{Offset: g.VectorsInt8[chunkID], Len: uint32(ChunkSize * paddedDims), Cap: uint32(ChunkSize * paddedDims)}, maxGen) // #nosec G115
	}
	return nil
}

func (g *GraphData) GetVectorsUint8Chunk(chunkID int) []uint8 {
	return g.GetVectorsUint8ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsUint8ChunkWithGen(chunkID int, maxGen uint64) []uint8 {
	chunk := g.GetVectorsInt8ChunkWithGen(chunkID, maxGen)
	if chunk == nil {
		return nil
	}
	ptr := unsafe.Pointer(&chunk[0])               // #nosec G103
	return unsafe.Slice((*uint8)(ptr), len(chunk)) // #nosec G103
}

func (g *GraphData) GetVectorsInt16Chunk(chunkID int) []int16 {
	return g.GetVectorsInt16ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsInt16ChunkWithGen(chunkID int, maxGen uint64) []int16 {
	if chunkID < len(g.VectorsInt16) && g.Int16Arena != nil {
		paddedDims := g.GetPaddedDimsForType(VectorTypeInt16)
		return g.Int16Arena.GetWithGeneration(memory.SliceRef{Offset: g.VectorsInt16[chunkID], Len: uint32(ChunkSize * paddedDims), Cap: uint32(ChunkSize * paddedDims)}, maxGen) // #nosec G115
	}
	return nil
}

func (g *GraphData) GetVectorsUint16Chunk(chunkID int) []uint16 {
	return g.GetVectorsUint16ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsUint16ChunkWithGen(chunkID int, maxGen uint64) []uint16 {
	if chunkID < len(g.VectorsUint16) && g.Uint16Arena != nil {
		paddedDims := g.GetPaddedDimsForType(VectorTypeUint16)
		return g.Uint16Arena.GetWithGeneration(memory.SliceRef{Offset: g.VectorsUint16[chunkID], Len: uint32(ChunkSize * paddedDims), Cap: uint32(ChunkSize * paddedDims)}, maxGen) // #nosec G115
	}
	return nil
}

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
				initArenaSafe(&g.Uint32Arena, 1024*1024*64, g.Allocator)
				ref, err := g.Uint32Arena.AllocSlice(ChunkSize * MaxNeighbors)
				if err != nil {
					return err
				}
				atomic.StoreUint64(&g.Neighbors[l][cID], ref.Offset)
			}
		}

		if atomic.LoadUint64(&g.Counts[l][cID]) == 0 {
			initArenaSafe(&g.Int32Arena, 4*1024*1024, g.Allocator)
			ref, err := g.Int32Arena.AllocSlice(ChunkSize)
			if err != nil {
				return err
			}
			atomic.StoreUint64(&g.Counts[l][cID], ref.Offset)
		}

		if atomic.LoadUint64(&g.Versions[l][cID]) == 0 {
			initArenaSafe(&g.Uint32Arena, 16*1024*1024, g.Allocator)
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
		atomic.StoreUint32(&neighborsChunk[baseIdx+i], n)
	}
	atomic.StoreInt32(&countsChunk[cOff], int32(len(neighbors))) // #nosec G115

	if versionsChunk != nil {
		atomic.AddUint32(&versionsChunk[cOff], 1)
	}

	// Increment global version
	atomic.AddUint64(&g.GlobalVersion, 1)

	return nil
}

func (g *GraphData) GetVectorsF16Chunk(chunkID int) []float16.Num {
	return g.GetVectorsF16ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsF16ChunkWithGen(chunkID int, maxGen uint64) []float16.Num {
	if chunkID < len(g.VectorsF16) && g.Float16Arena != nil {
		paddedDims := g.GetPaddedDimsForType(VectorTypeFloat16)
		return g.Float16Arena.GetWithGeneration(memory.SliceRef{Offset: g.VectorsF16[chunkID], Len: uint32(ChunkSize * paddedDims), Cap: uint32(ChunkSize * paddedDims)}, maxGen) // #nosec G115
	}
	return nil
}

// GetVector returns the vector for the given ID.
func (g *GraphData) GetVector(id uint32) (any, error) {
	return g.GetVectorWithGen(id, math.MaxUint64)
}

func (g *GraphData) GetVectorWithGen(id uint32, maxGen uint64) (any, error) {
	if g.Dims <= 0 {
		return nil, fmt.Errorf("vector type mismatch or uninitialized for ID %d (dims is 0)", id)
	}

	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	// Based on type, get the appropriate chunk
	// Only supporting float32 and float16 for now in this generic method
	if g.Uint8Arena != nil && len(g.VectorsSQ8) > cID && g.SQ8Enabled && atomic.LoadUint32(&g.SQ8Ready) == 1 {
		chunk := g.GetVectorsSQ8ChunkWithGen(cID, maxGen)
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
		chunk := g.GetVectorsInt8ChunkWithGen(cID, maxGen)
		if chunk == nil {
			return nil, nil
		}
		pd := g.GetPaddedDimsForType(VectorTypeUint8)
		start := cOff * pd
		if start+g.Dims <= len(chunk) {
			ptr := unsafe.Pointer(&chunk[0])                   // #nosec G103
			u8Chunk := unsafe.Slice((*uint8)(ptr), len(chunk)) // #nosec G103
			return u8Chunk[start : start+g.Dims], nil
		}
	case VectorTypeInt8:
		chunk := g.GetVectorsInt8ChunkWithGen(cID, maxGen)
		if chunk == nil {
			return nil, nil
		}
		pd := g.GetPaddedDimsForType(VectorTypeInt8)
		start := cOff * pd
		if start+g.Dims <= len(chunk) {
			return chunk[start : start+g.Dims], nil
		}
	case VectorTypeInt16:
		chunk := g.GetVectorsInt16ChunkWithGen(cID, maxGen)
		if chunk == nil {
			return nil, nil
		}
		pd := g.GetPaddedDimsForType(VectorTypeInt16)
		start := cOff * pd
		if start+g.Dims <= len(chunk) {
			return chunk[start : start+g.Dims], nil
		}
	case VectorTypeUint16:
		chunk := g.GetVectorsUint16ChunkWithGen(cID, maxGen)
		if chunk == nil {
			return nil, nil
		}
		pd := g.GetPaddedDimsForType(VectorTypeUint16)
		start := cOff * pd
		if start+g.Dims <= len(chunk) {
			return chunk[start : start+g.Dims], nil
		}
	case VectorTypeInt32:
		chunk := g.GetVectorsInt32ChunkWithGen(cID, maxGen)
		if chunk == nil {
			return nil, nil
		}
		pd := g.GetPaddedDimsForType(VectorTypeInt32)
		start := cOff * pd
		if start+g.Dims <= len(chunk) {
			return chunk[start : start+g.Dims], nil
		}
	case VectorTypeUint32:
		chunk := g.GetVectorsUint32ChunkWithGen(cID, maxGen)
		if chunk == nil {
			return nil, nil
		}
		pd := g.GetPaddedDimsForType(VectorTypeUint32)
		start := cOff * pd
		if start+g.Dims <= len(chunk) {
			return chunk[start : start+g.Dims], nil
		}
	case VectorTypeInt64:
		chunk := g.GetVectorsInt64ChunkWithGen(cID, maxGen)
		if chunk == nil {
			return nil, nil
		}
		pd := g.GetPaddedDimsForType(VectorTypeInt64)
		start := cOff * pd
		if start+g.Dims <= len(chunk) {
			return chunk[start : start+g.Dims], nil
		}
	case VectorTypeUint64:
		chunk := g.GetVectorsUint64ChunkWithGen(cID, maxGen)
		if chunk == nil {
			return nil, nil
		}
		pd := g.GetPaddedDimsForType(VectorTypeUint64)
		start := cOff * pd
		if start+g.Dims <= len(chunk) {
			return chunk[start : start+g.Dims], nil
		}
	case VectorTypeFloat32:
		chunk := g.GetVectorsChunkWithGen(cID, maxGen)
		if chunk != nil {
			pd := g.GetPaddedDimsForType(VectorTypeFloat32)
			start := cOff * pd
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	case VectorTypeFloat64:
		chunk := g.GetVectorsFloat64ChunkWithGen(cID, maxGen)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeFloat64)
			start := cOff * paddedDims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	case VectorTypeComplex64:
		chunk := g.GetVectorsComplex64ChunkWithGen(cID, maxGen)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeComplex64)
			start := cOff * paddedDims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	case VectorTypeComplex128:
		chunk := g.GetVectorsComplex128ChunkWithGen(cID, maxGen)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeComplex128)
			start := cOff * paddedDims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	case VectorTypeFloat16:
		chunk := g.GetVectorsF16ChunkWithGen(cID, maxGen)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeFloat16)
			start := cOff * paddedDims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	case VectorTypeTQ:
		chunk := g.GetVectorsTQChunkWithGen(cID, maxGen)
		if chunk != nil {
			stride := g.PackedSize()
			start := cOff * stride
			if start+stride <= len(chunk) {
				return chunk[start : start+stride], nil
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

func (g *GraphData) GetVectorPQWithGen(id uint32, maxGen uint64) []byte {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	if g.Uint64Arena != nil && cID < len(g.VectorsPQ) {
		m := g.PQM
		numWordsPerNode := (m + 7) / 8
		numWords := ChunkSize * numWordsPerNode

		offset := atomic.LoadUint64(&g.VectorsPQ[cID])
		chunk := g.Uint64Arena.GetWithGeneration(memory.SliceRef{
			Offset: offset,
			Len:    uint32(numWords), // #nosec G115
			Cap:    uint32(numWords), // #nosec G115
		}, maxGen)

		if len(chunk) == 0 {
			return nil
		}

		ptr := unsafe.Pointer(&chunk[0])                    // #nosec G103
		byteChunk := unsafe.Slice((*byte)(ptr), numWords*8) // #nosec G103

		start := cOff * m
		if start+m <= len(byteChunk) {
			return byteChunk[start : start+m]
		}
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

func (g *GraphData) GetNeighbors(layer int, id uint32, buf []uint32) []uint32 {
	return g.GetNeighborsWithGen(layer, id, buf, math.MaxUint64)
}

// GetNeighborsWithGen returns the neighbor list for a node with generation isolation.
func (g *GraphData) GetNeighborsWithGen(layer int, id uint32, buf []uint32, maxGen uint64) []uint32 {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	counts := g.GetCountsChunkWithGen(layer, cID, maxGen)
	neighbors := g.GetNeighborsChunkWithGen(layer, cID, maxGen)
	versions := g.GetVersionsChunkWithGen(layer, cID, maxGen)

	// When both chunk-based neighbors and counts are nil (upper layers after Fix #1),
	// try PackedNeighbors first, then fall back to BackingGraph.
	if counts == nil && neighbors == nil {
		// 1. Try Lock-Free PackedNeighbors first
		if layer < len(g.PackedNeighbors) && g.PackedNeighbors[layer] != nil {
			if res, ok := g.PackedNeighbors[layer].GetNeighborsWithGen(id, maxGen); ok {
				return res
			}
		}
		// 2. Fall back to DiskGraph / backing graph
		if g.BackingGraph != nil {
			if bg, ok := g.BackingGraph.(graphFallback); ok {
				return bg.GetNeighbors(layer, id, buf)
			}
		}
		return nil
	}

	// Try Lock-Free PackedNeighbors (also applies when counts exist but neighbors are nil)
	if layer < len(g.PackedNeighbors) && g.PackedNeighbors[layer] != nil {
		if res, ok := g.PackedNeighbors[layer].GetNeighborsWithGen(id, maxGen); ok {
			return res
		}
	}

	// If counts exists but neighbors are nil (upper layers after Fix #1), we're done
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

// GetNeighborsLockFree returns neighbors without seqlock checks.
// Should only be used when the node lock is already held.
func (g *GraphData) GetNeighborsLockFree(layer int, id uint32) []uint32 {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	counts := g.GetCountsChunk(layer, cID)
	neighbors := g.GetNeighborsChunk(layer, cID)

	if counts == nil || neighbors == nil {
		return nil
	}

	count := int(atomic.LoadInt32(&counts[cOff]))
	if count == 0 {
		return nil
	}

	base := cOff * MaxNeighbors
	res := make([]uint32, count)
	for i := 0; i < count; i++ {
		res[i] = atomic.LoadUint32(&neighbors[base+i])
	}
	return res
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
func (g *GraphData) GetLevelsChunk(chunkID int) []uint32 {
	if chunkID < len(g.Levels) {
		return g.Levels[chunkID]
	}
	return nil
}

// AcquireReader pins the GraphData against premature typed-arena release.
// Bracket any read path that accesses the typed-arena fields (Int8Arena,
// Float32Arena, etc.) with AcquireReader/​ReleaseReader so a concurrent
// compareAndSwapData-driven Release() waits until the read completes.
// Cheap: a single atomic add. Hot-path readers (search, GetNeighbors) should
// also use it; the contention window is short.
func (g *GraphData) AcquireReader() {
	atomic.AddInt32(&g.readerCount, 1)
}

// ReleaseReader decrements the reader count acquired by AcquireReader.
// Pairs 1:1 with AcquireReader. Use defer to be panic-safe.
func (g *GraphData) ReleaseReader() {
	atomic.AddInt32(&g.readerCount, -1)
}

// Clone creates a shallow copy of the GraphData with deep copies of the structure slices.
// This allows concurrent readers to safely access the old structure while a new one is being built (COW).
func (g *GraphData) Clone() *GraphData {
	// Signal to any concurrent Release() that a Clone is in progress.
	// Release() will spin-wait until all outstanding Clones finish
	// before freeing the underlying arenas.
	atomic.AddInt32(&g.cloneCount, 1)
	defer atomic.AddInt32(&g.cloneCount, -1)
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
	newG.SharedVectorSpace = g.SharedVectorSpace

	// Slabs/Arenas - share with original for read access.
	// New chunks allocated via EnsureChunk will use the original's arena.
	// This is safe because COW is serialized (protected by growMu lock).
	// Clone's Vectors* slices reference chunks allocated from original's arena.
	// Slabs/Arenas - Create new wrappers but share underlying SlabArena
	if g.Float32Arena != nil {
		newG.Float32Arena = memory.NewTypedArena[float32](g.Float32Arena.Slab())
		newG.Float32Arena.Retain()
	}
	if g.Float64Arena != nil {
		newG.Float64Arena = memory.NewTypedArena[float64](g.Float64Arena.Slab())
		newG.Float64Arena.Retain()
	}
	if g.Uint8Arena != nil {
		newG.Uint8Arena = memory.NewTypedArena[uint8](g.Uint8Arena.Slab())
		newG.Uint8Arena.Retain()
	}
	if g.Uint16Arena != nil {
		newG.Uint16Arena = memory.NewTypedArena[uint16](g.Uint16Arena.Slab())
		newG.Uint16Arena.Retain()
	}
	if g.Uint32Arena != nil {
		newG.Uint32Arena = memory.NewTypedArena[uint32](g.Uint32Arena.Slab())
		newG.Uint32Arena.Retain()
	}
	if g.Uint64Arena != nil {
		newG.Uint64Arena = memory.NewTypedArena[uint64](g.Uint64Arena.Slab())
		newG.Uint64Arena.Retain()
	}
	if g.Int8Arena != nil {
		newG.Int8Arena = memory.NewTypedArena[int8](g.Int8Arena.Slab())
		newG.Int8Arena.Retain()
	}
	if g.Int16Arena != nil {
		newG.Int16Arena = memory.NewTypedArena[int16](g.Int16Arena.Slab())
		newG.Int16Arena.Retain()
	}
	if g.Int32Arena != nil {
		newG.Int32Arena = memory.NewTypedArena[int32](g.Int32Arena.Slab())
		newG.Int32Arena.Retain()
	}
	if g.Int64Arena != nil {
		newG.Int64Arena = memory.NewTypedArena[int64](g.Int64Arena.Slab())
		newG.Int64Arena.Retain()
	}
	if g.Float16Arena != nil {
		newG.Float16Arena = memory.NewTypedArena[float16.Num](g.Float16Arena.Slab())
		newG.Float16Arena.Retain()
	}
	if g.Complex64Arena != nil {
		newG.Complex64Arena = memory.NewTypedArena[complex64](g.Complex64Arena.Slab())
		newG.Complex64Arena.Retain()
	}
	if g.Complex128Arena != nil {
		newG.Complex128Arena = memory.NewTypedArena[complex128](g.Complex128Arena.Slab())
		newG.Complex128Arena.Retain()
	}

	// Deep copy Levels
	if g.Levels != nil {
		newG.Levels = make([][]uint32, len(g.Levels))
		for i := range g.Levels {
			if g.Levels[i] != nil {
				newG.Levels[i] = make([]uint32, len(g.Levels[i]))
				for j := range g.Levels[i] {
					newG.Levels[i][j] = atomic.LoadUint32(&g.Levels[i][j])
				}
			}
		}
	}

	// Deep copy Vectors (Slice of slices)
	if g.Vectors != nil {
		newG.Vectors = make([][]float32, len(g.Vectors))
		for i := range g.Vectors {
			if g.Vectors[i] != nil {
				newG.Vectors[i] = make([]float32, len(g.Vectors[i]))
				copy(newG.Vectors[i], g.Vectors[i])
			}
		}
	}
	if g.VectorsFloat64 != nil {
		newG.VectorsFloat64 = make([][]float64, len(g.VectorsFloat64))
		for i := range g.VectorsFloat64 {
			if g.VectorsFloat64[i] != nil {
				newG.VectorsFloat64[i] = make([]float64, len(g.VectorsFloat64[i]))
				copy(newG.VectorsFloat64[i], g.VectorsFloat64[i])
			}
		}
	}
	if g.VectorsComplex64 != nil {
		newG.VectorsComplex64 = make([][]complex64, len(g.VectorsComplex64))
		for i := range g.VectorsComplex64 {
			if g.VectorsComplex64[i] != nil {
				newG.VectorsComplex64[i] = make([]complex64, len(g.VectorsComplex64[i]))
				copy(newG.VectorsComplex64[i], g.VectorsComplex64[i])
			}
		}
	}
	if g.VectorsComplex128 != nil {
		newG.VectorsComplex128 = make([][]complex128, len(g.VectorsComplex128))
		for i := range g.VectorsComplex128 {
			if g.VectorsComplex128[i] != nil {
				newG.VectorsComplex128[i] = make([]complex128, len(g.VectorsComplex128[i]))
				copy(newG.VectorsComplex128[i], g.VectorsComplex128[i])
			}
		}
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
				for j := range g.Neighbors[l] {
					newG.Neighbors[l][j] = atomic.LoadUint64(&g.Neighbors[l][j])
				}
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
				for j := range g.Counts[l] {
					newG.Counts[l][j] = atomic.LoadUint64(&g.Counts[l][j])
				}
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
				for j := range g.Versions[l] {
					newG.Versions[l][j] = atomic.LoadUint64(&g.Versions[l][j])
				}
			}
		}
	}

	// Shallow copy PackedNeighbors and retain them (they are thread-safe and shared)
	if g.PackedNeighbors != nil {
		newG.PackedNeighbors = make([]PackedNeighbors, len(g.PackedNeighbors))
		copy(newG.PackedNeighbors, g.PackedNeighbors)
		for i := range newG.PackedNeighbors {
			if newG.PackedNeighbors[i] != nil {
				newG.PackedNeighbors[i].Retain()
			}
		}
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
			if enabled {
				return []uint64{}
			}
			return nil
		}
		dst := make([]uint64, len(src))
		for i := range src {
			dst[i] = atomic.LoadUint64(&src[i])
		}
		return dst
	}

	newG.VectorsF32 = copyOffsetSlice(g.VectorsF32, true)
	newG.VectorsSQ8 = copyOffsetSlice(g.VectorsSQ8, g.SQ8Enabled)
	newG.VectorsPQ = copyOffsetSlice(g.VectorsPQ, g.PQEnabled)
	newG.VectorsBQ = copyOffsetSlice(g.VectorsBQ, g.BQEnabled)
	newG.VectorsTQ = copyOffsetSlice(g.VectorsTQ, g.TurboQuantEnabled)
	newG.VectorsF16 = copyOffsetSlice(g.VectorsF16, g.Type == VectorTypeFloat16)
	newG.VectorsInt8 = copyOffsetSlice(g.VectorsInt8, g.Type == VectorTypeInt8 || g.Type == VectorTypeUint8)
	newG.VectorsInt16 = copyOffsetSlice(g.VectorsInt16, g.Type == VectorTypeInt16)
	newG.VectorsUint16 = copyOffsetSlice(g.VectorsUint16, g.Type == VectorTypeUint16)
	newG.VectorsInt32 = copyOffsetSlice(g.VectorsInt32, g.Type == VectorTypeInt32)
	newG.VectorsUint32 = copyOffsetSlice(g.VectorsUint32, g.Type == VectorTypeUint32)
	newG.VectorsInt64 = copyOffsetSlice(g.VectorsInt64, g.Type == VectorTypeInt64)
	newG.VectorsUint64 = copyOffsetSlice(g.VectorsUint64, g.Type == VectorTypeUint64)
	newG.VectorsFloat64Offsets = copyOffsetSlice(g.VectorsFloat64Offsets, g.Type == VectorTypeFloat64)
	newG.VectorsComplex64Offsets = copyOffsetSlice(g.VectorsComplex64Offsets, g.Type == VectorTypeComplex64)
	newG.VectorsComplex128Offsets = copyOffsetSlice(g.VectorsComplex128Offsets, g.Type == VectorTypeComplex128)

	// Set finalizer to ensure automatic Release when snapshot is orphaned
	runtime.SetFinalizer(newG, func(g *GraphData) { g.Release() })

	return newG
}

// ShallowStructuralClone creates a new GraphData that shares the
// per-chunk vector slice headers (Vectors, VectorsFloat64,
// VectorsComplex64, VectorsComplex128) with the original, but still
// deep-copies the structural slices (Levels, Neighbors, Counts,
// Versions, offset slices) and retains the typed-arena Slabs.
//
// Why this is safe:
//
//  1. The per-chunk vector data lives in the shared typed-arena Slab,
//     and the Slab is ref-counted. The new GraphData's typed-arena
//     retains the Slab (same as Clone), so the data is alive as long
//     as the new GraphData is alive.
//
//  2. The per-chunk slice headers (Vectors[i], etc.) are read-only
//     after publication. They are set once by SetExternalVectorsChunk
//     or SetZeroCopyMapping during initial setup, and the modern write
//     path uses VectorsF32[cID] (offset) + Float32Arena.Get(...) to
//     write to NEW chunks; it does NOT write to Vectors[i] directly.
//     Sharing the slice header between old and new is therefore safe —
//     both readers see the same read-only data.
//
//  3. The structural slices (Levels, Neighbors, Counts, Versions) and
//     the offset slices (VectorsF32, VectorsInt8, etc.) are deep-copied
//     because their elements are mutated via atomic CAS operations or
//     atomic StoreUint64 by EnsureChunk. Sharing them would corrupt
//     the old GraphData.
//
//  4. The PackedNeighbors and ArrowRefs fields are retained (same as
//     Clone) because they are reference-counted by external code.
//
// Memory savings at int8 50k dim=384: ~19 MB per Clone (50 chunks ×
// 1000 vectors × 384 bytes). Reduces Clone's heap pressure from
// ~26.5 MB to ~7 MB per Clone call. At 50k inserts with 5 concurrent
// batches, this is a ~95 MB reduction in transient heap allocations.
//
// Intended use: the per-batch private-clone path in
// ArrowHNSW.insertInternal, where the writer needs a private copy of
// the published GraphData but will only write to NEW chunks (or
// freshly-allocated structural elements). NOT a general-purpose
// replacement for Clone — the persistence path and any code that
// mutates Vectors[i] directly should still use Clone. Callers MUST
// verify that the writer does not write to a chunk via the shared
// Vectors[i] path (i.e., the writer must go through the
// Vectors<Type>Offset → typed-arena.Get path).
func (g *GraphData) ShallowStructuralClone() *GraphData {
	// Signal to any concurrent Release() that a Clone is in progress.
	// Release() will spin-wait until all outstanding Clones finish
	// before freeing the underlying arenas.
	atomic.AddInt32(&g.cloneCount, 1)
	defer atomic.AddInt32(&g.cloneCount, -1)
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
	newG.SharedVectorSpace = g.SharedVectorSpace

	// Slabs/Arenas - share with original for read access (same as Clone).
	// The Retain() ensures the Slab stays alive as long as the new
	// GraphData is alive, even after the old is Released.
	if g.Float32Arena != nil {
		newG.Float32Arena = memory.NewTypedArena[float32](g.Float32Arena.Slab())
		newG.Float32Arena.Retain()
	}
	if g.Float64Arena != nil {
		newG.Float64Arena = memory.NewTypedArena[float64](g.Float64Arena.Slab())
		newG.Float64Arena.Retain()
	}
	if g.Uint8Arena != nil {
		newG.Uint8Arena = memory.NewTypedArena[uint8](g.Uint8Arena.Slab())
		newG.Uint8Arena.Retain()
	}
	if g.Uint16Arena != nil {
		newG.Uint16Arena = memory.NewTypedArena[uint16](g.Uint16Arena.Slab())
		newG.Uint16Arena.Retain()
	}
	if g.Uint32Arena != nil {
		newG.Uint32Arena = memory.NewTypedArena[uint32](g.Uint32Arena.Slab())
		newG.Uint32Arena.Retain()
	}
	if g.Uint64Arena != nil {
		newG.Uint64Arena = memory.NewTypedArena[uint64](g.Uint64Arena.Slab())
		newG.Uint64Arena.Retain()
	}
	if g.Int8Arena != nil {
		newG.Int8Arena = memory.NewTypedArena[int8](g.Int8Arena.Slab())
		newG.Int8Arena.Retain()
	}
	if g.Int16Arena != nil {
		newG.Int16Arena = memory.NewTypedArena[int16](g.Int16Arena.Slab())
		newG.Int16Arena.Retain()
	}
	if g.Int32Arena != nil {
		newG.Int32Arena = memory.NewTypedArena[int32](g.Int32Arena.Slab())
		newG.Int32Arena.Retain()
	}
	if g.Int64Arena != nil {
		newG.Int64Arena = memory.NewTypedArena[int64](g.Int64Arena.Slab())
		newG.Int64Arena.Retain()
	}
	if g.Float16Arena != nil {
		newG.Float16Arena = memory.NewTypedArena[float16.Num](g.Float16Arena.Slab())
		newG.Float16Arena.Retain()
	}
	if g.Complex64Arena != nil {
		newG.Complex64Arena = memory.NewTypedArena[complex64](g.Complex64Arena.Slab())
		newG.Complex64Arena.Retain()
	}
	if g.Complex128Arena != nil {
		newG.Complex128Arena = memory.NewTypedArena[complex128](g.Complex128Arena.Slab())
		newG.Complex128Arena.Retain()
	}

	// SHALLOW COPY: share per-chunk vector slice headers.
	// The per-chunk data is read-only after publication. Sharing the
	// slice header is safe because:
	//   - The modern write path goes through Vectors<Type>Offset +
	//     typed-arena.Get(...), not Vectors[i] directly.
	//   - Vectors[i] is set once during initial setup
	//     (SetExternalVectorsChunk / SetZeroCopyMapping) and is
	//     read-only thereafter.
	//   - The shared underlying Slab chunk is alive as long as either
	//     GraphData is alive (the Slab is ref-counted, the new's
	//     typed-arena retains it).
	if g.Vectors != nil {
		newG.Vectors = make([][]float32, len(g.Vectors))
		for i := range g.Vectors {
			if g.Vectors[i] != nil {
				newG.Vectors[i] = g.Vectors[i] // shared slice header
			}
		}
	}
	if g.VectorsFloat64 != nil {
		newG.VectorsFloat64 = make([][]float64, len(g.VectorsFloat64))
		for i := range g.VectorsFloat64 {
			if g.VectorsFloat64[i] != nil {
				newG.VectorsFloat64[i] = g.VectorsFloat64[i]
			}
		}
	}
	if g.VectorsComplex64 != nil {
		newG.VectorsComplex64 = make([][]complex64, len(g.VectorsComplex64))
		for i := range g.VectorsComplex64 {
			if g.VectorsComplex64[i] != nil {
				newG.VectorsComplex64[i] = g.VectorsComplex64[i]
			}
		}
	}
	if g.VectorsComplex128 != nil {
		newG.VectorsComplex128 = make([][]complex128, len(g.VectorsComplex128))
		for i := range g.VectorsComplex128 {
			if g.VectorsComplex128[i] != nil {
				newG.VectorsComplex128[i] = g.VectorsComplex128[i]
			}
		}
	}

	// Deep copy Levels (same as Clone) — elements are mutated via atomic CAS
	if g.Levels != nil {
		newG.Levels = make([][]uint32, len(g.Levels))
		for i := range g.Levels {
			if g.Levels[i] != nil {
				newG.Levels[i] = make([]uint32, len(g.Levels[i]))
				for j := range g.Levels[i] {
					newG.Levels[i][j] = atomic.LoadUint32(&g.Levels[i][j])
				}
			}
		}
	}

	// Deep copy Neighbors (same as Clone) — elements are mutated via atomic CAS
	if g.Neighbors != nil {
		newG.Neighbors = make([][]uint64, len(g.Neighbors))
		for l := range g.Neighbors {
			if g.Neighbors[l] != nil {
				targetCap := len(g.Neighbors[l])
				if targetCap < 16 {
					targetCap = 16
				}
				newG.Neighbors[l] = make([]uint64, len(g.Neighbors[l]), targetCap)
				for j := range g.Neighbors[l] {
					newG.Neighbors[l][j] = atomic.LoadUint64(&g.Neighbors[l][j])
				}
			}
		}
	}

	// Deep copy Counts (same as Clone)
	if g.Counts != nil {
		newG.Counts = make([][]uint64, len(g.Counts))
		for l := range g.Counts {
			if g.Counts[l] != nil {
				targetCap := len(g.Counts[l])
				if targetCap < 16 {
					targetCap = 16
				}
				newG.Counts[l] = make([]uint64, len(g.Counts[l]), targetCap)
				for j := range g.Counts[l] {
					newG.Counts[l][j] = atomic.LoadUint64(&g.Counts[l][j])
				}
			}
		}
	}

	// Deep copy Versions (same as Clone)
	if g.Versions != nil {
		newG.Versions = make([][]uint64, len(g.Versions))
		for l := range g.Versions {
			if g.Versions[l] != nil {
				targetCap := len(g.Versions[l])
				if targetCap < 16 {
					targetCap = 16
				}
				newG.Versions[l] = make([]uint64, len(g.Versions[l]), targetCap)
				for j := range g.Versions[l] {
					newG.Versions[l][j] = atomic.LoadUint64(&g.Versions[l][j])
				}
			}
		}
	}

	// Shallow copy PackedNeighbors and retain them (they are thread-safe and shared)
	if g.PackedNeighbors != nil {
		newG.PackedNeighbors = make([]PackedNeighbors, len(g.PackedNeighbors))
		copy(newG.PackedNeighbors, g.PackedNeighbors)
		for i := range newG.PackedNeighbors {
			if newG.PackedNeighbors[i] != nil {
				newG.PackedNeighbors[i].Retain()
			}
		}
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

	// Deep copy offset slices (VectorsF32, VectorsInt8, etc.) — these
	// are written by EnsureChunk via atomic StoreUint64, so a deep copy
	// is required for COW correctness.
	copyOffsetSlice := func(src []uint64, enabled bool) []uint64 {
		if src == nil {
			if enabled {
				return []uint64{}
			}
			return nil
		}
		dst := make([]uint64, len(src))
		for i := range src {
			dst[i] = atomic.LoadUint64(&src[i])
		}
		return dst
	}

	newG.VectorsF32 = copyOffsetSlice(g.VectorsF32, true)
	newG.VectorsSQ8 = copyOffsetSlice(g.VectorsSQ8, g.SQ8Enabled)
	newG.VectorsPQ = copyOffsetSlice(g.VectorsPQ, g.PQEnabled)
	newG.VectorsBQ = copyOffsetSlice(g.VectorsBQ, g.BQEnabled)
	newG.VectorsTQ = copyOffsetSlice(g.VectorsTQ, g.TurboQuantEnabled)
	newG.VectorsF16 = copyOffsetSlice(g.VectorsF16, g.Type == VectorTypeFloat16)
	newG.VectorsInt8 = copyOffsetSlice(g.VectorsInt8, g.Type == VectorTypeInt8 || g.Type == VectorTypeUint8)
	newG.VectorsInt16 = copyOffsetSlice(g.VectorsInt16, g.Type == VectorTypeInt16)
	newG.VectorsUint16 = copyOffsetSlice(g.VectorsUint16, g.Type == VectorTypeUint16)
	newG.VectorsInt32 = copyOffsetSlice(g.VectorsInt32, g.Type == VectorTypeInt32)
	newG.VectorsUint32 = copyOffsetSlice(g.VectorsUint32, g.Type == VectorTypeUint32)
	newG.VectorsInt64 = copyOffsetSlice(g.VectorsInt64, g.Type == VectorTypeInt64)
	newG.VectorsUint64 = copyOffsetSlice(g.VectorsUint64, g.Type == VectorTypeUint64)
	newG.VectorsFloat64Offsets = copyOffsetSlice(g.VectorsFloat64Offsets, g.Type == VectorTypeFloat64)
	newG.VectorsComplex64Offsets = copyOffsetSlice(g.VectorsComplex64Offsets, g.Type == VectorTypeComplex64)
	newG.VectorsComplex128Offsets = copyOffsetSlice(g.VectorsComplex128Offsets, g.Type == VectorTypeComplex128)

	// Set finalizer to ensure automatic Release when snapshot is orphaned
	runtime.SetFinalizer(newG, func(g *GraphData) { g.Release() })

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
		slabSize := 64 * 1024 * 1024

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
func NewGraphData(capacity, dim int, mmap bool, useDisk bool, fd int,
	quantization bool, sq8 bool, persistent bool,
	dataType VectorDataType, bqEnabled bool, pqEnabled bool,
	tqEnabled bool, tqBits int, name string, alloc arrowmemory.Allocator,
	sharedVectorSpace bool) *GraphData {

	// Enforce minimum capacity to avoid rapid initial COW cycles
	if capacity < 1024 {
		capacity = 1024
	}

	var f32Arena, u8Arena, f64Arena, i8Arena, c64Arena, c128Arena, i64Arena, i16Arena, u16Arena, i32Arena, f16Arena, u64Arena, u32Arena *memory.SlabArena
	if dim > 0 && !sharedVectorSpace {
		minSlabSize := 4 * 1024 * 1024 // 4MB minimum for all arenas to reduce GC overhead

		if dataType == VectorTypeFloat32 || dataType == VectorTypeUnknown {
			f32SlabSize := ChunkSize*dim*4 + 64
			if f32SlabSize < minSlabSize {
				f32SlabSize = minSlabSize
			}
			if alloc != nil {
				f32Arena = memory.NewSlabArenaWithAllocator(f32SlabSize, alloc)
			} else {
				f32Arena = memory.NewSlabArena(f32SlabSize)
			}
		}

		if dataType == VectorTypeUint8 {
			u8SlabSize := ChunkSize*dim + 64
			if u8SlabSize < minSlabSize {
				u8SlabSize = minSlabSize
			}
			if alloc != nil {
				u8Arena = memory.NewSlabArenaWithAllocator(u8SlabSize, alloc)
			} else {
				u8Arena = memory.NewSlabArena(u8SlabSize)
			}
		}

		if dataType == VectorTypeFloat64 {
			f64SlabSize := ChunkSize*dim*8 + 64
			if f64SlabSize < minSlabSize {
				f64SlabSize = minSlabSize
			}
			if alloc != nil {
				f64Arena = memory.NewSlabArenaWithAllocator(f64SlabSize, alloc)
			} else {
				f64Arena = memory.NewSlabArena(f64SlabSize)
			}
		}

		if dataType == VectorTypeInt8 {
			u8SlabSize := ChunkSize*dim + 64
			if u8SlabSize < minSlabSize {
				u8SlabSize = minSlabSize
			}
			if alloc != nil {
				i8Arena = memory.NewSlabArenaWithAllocator(u8SlabSize, alloc)
			} else {
				i8Arena = memory.NewSlabArena(u8SlabSize)
			}
		}

		if dataType == VectorTypeComplex64 {
			c64SlabSize := ChunkSize*dim*8 + 64
			if c64SlabSize < minSlabSize {
				c64SlabSize = minSlabSize
			}
			if alloc != nil {
				c64Arena = memory.NewSlabArenaWithAllocator(c64SlabSize, alloc)
			} else {
				c64Arena = memory.NewSlabArena(c64SlabSize)
			}
		}

		if dataType == VectorTypeComplex128 {
			c128SlabSize := ChunkSize*dim*16 + 64
			if c128SlabSize < minSlabSize {
				c128SlabSize = minSlabSize
			}
			if alloc != nil {
				c128Arena = memory.NewSlabArenaWithAllocator(c128SlabSize, alloc)
			} else {
				c128Arena = memory.NewSlabArena(c128SlabSize)
			}
		}

		if dataType == VectorTypeInt64 {
			i64SlabSize := ChunkSize*dim*8 + 64
			if i64SlabSize < minSlabSize {
				i64SlabSize = minSlabSize
			}
			if alloc != nil {
				i64Arena = memory.NewSlabArenaWithAllocator(i64SlabSize, alloc)
			} else {
				i64Arena = memory.NewSlabArena(i64SlabSize)
			}
		}

		if dataType == VectorTypeInt16 {
			i16SlabSize := ChunkSize*dim*2 + 64
			if i16SlabSize < minSlabSize {
				i16SlabSize = minSlabSize
			}
			if alloc != nil {
				i16Arena = memory.NewSlabArenaWithAllocator(i16SlabSize, alloc)
			} else {
				i16Arena = memory.NewSlabArena(i16SlabSize)
			}
		}

		if dataType == VectorTypeUint16 {
			u16SlabSize := ChunkSize*dim*2 + 64
			if u16SlabSize < minSlabSize {
				u16SlabSize = minSlabSize
			}
			if alloc != nil {
				u16Arena = memory.NewSlabArenaWithAllocator(u16SlabSize, alloc)
			} else {
				u16Arena = memory.NewSlabArena(u16SlabSize)
			}
		}

		if dataType == VectorTypeInt32 {
			i32SlabSize := ChunkSize*dim*4 + 64
			if i32SlabSize < minSlabSize {
				i32SlabSize = minSlabSize
			}
			if alloc != nil {
				i32Arena = memory.NewSlabArenaWithAllocator(i32SlabSize, alloc)
			} else {
				i32Arena = memory.NewSlabArena(i32SlabSize)
			}
		}

		if dataType == VectorTypeFloat16 {
			f16SlabSize := ChunkSize*dim*2 + 64
			if f16SlabSize < minSlabSize {
				f16SlabSize = minSlabSize
			}
			if alloc != nil {
				f16Arena = memory.NewSlabArenaWithAllocator(f16SlabSize, alloc)
			} else {
				f16Arena = memory.NewSlabArena(f16SlabSize)
			}
		}

		if dataType == VectorTypeUint64 {
			u64SlabSize := ChunkSize*dim*8 + 64
			if u64SlabSize < minSlabSize {
				u64SlabSize = minSlabSize
			}
			if alloc != nil {
				u64Arena = memory.NewSlabArenaWithAllocator(u64SlabSize, alloc)
			} else {
				u64Arena = memory.NewSlabArena(u64SlabSize)
			}
		}

		if dataType == VectorTypeUint32 {
			u32SlabSize := ChunkSize*dim*4 + 64
			if u32SlabSize < minSlabSize {
				u32SlabSize = minSlabSize
			}
			if alloc != nil {
				u32Arena = memory.NewSlabArenaWithAllocator(u32SlabSize, alloc)
			} else {
				u32Arena = memory.NewSlabArena(u32SlabSize)
			}
		}
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
		Allocator:         alloc,
		Vectors:           make([][]float32, numChunks),
		VectorsFloat64:    make([][]float64, numChunks),
		VectorsComplex64:  make([][]complex64, numChunks),
		VectorsComplex128: make([][]complex128, numChunks),
		TurboQuantEnabled: tqEnabled,
		TurboQuantBits:    tqBits,
		Neighbors:         make([][]uint64, ArrowMaxLayers),
		Counts:            make([][]uint64, ArrowMaxLayers),
		Versions:          make([][]uint64, ArrowMaxLayers),
		Levels:            make([][]uint32, 0, numChunks),
		VectorsTQ:         nil,
		VectorsPQ:         nil,
		VectorsSQ8:        nil,
		VectorsBQ:         nil,
		VectorsF16:        nil,
		VectorsF32:        make([]uint64, 0, numChunks),
		SharedVectorSpace: sharedVectorSpace,
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
	if capacity > 0 {
		numChunks := (capacity + ChunkSize - 1) / ChunkSize
		if numChunks <= 0 {
			numChunks = 1
		}
		gd.GrowMetadataSlices(numChunks)
		if dim > 0 {
			_ = gd.PreAllocate(capacity)
		}
	}

	// Set finalizer to ensure automatic Release when snapshot is orphaned
	runtime.SetFinalizer(gd, func(g *GraphData) { g.Release() })

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

func (g *GraphData) Release() {
	if !atomic.CompareAndSwapUint32(&g.released, 0, 1) {
		return
	}

	// Wait for all concurrent Clone() operations to finish.
	// Clone() increments cloneCount while reading this GraphData's fields
	// and takes a Retain() on shared arenas. We must not free until done.
	for atomic.LoadInt32(&g.cloneCount) > 0 {
		runtime.Gosched()
	}

	// Wait for all concurrent read paths to finish. AcquireReader/ReleaseReader
	// bracket read access to the typed-arena fields (Int8Arena, Float32Arena,
	// etc.). This guarantees that the Slabs pointed at by the typed-arenas
	// are not released (SlabArena.refs reaches 0) while any reader is
	// still calling AllocSlice / Get on them. The h.data CAS in
	// compareAndSwapData synchronizes ordering, so a reader that did
	// h.data.Load() before our CAS must call AcquireReader before
	// reaching this point.
	for atomic.LoadInt32(&g.readerCount) > 0 {
		runtime.Gosched()
	}

	if debugRelease {
		// Compute approximate memory being released
		var totalArenaBytes int64
		for _, ta := range []*memory.TypedArena[float32]{g.Float32Arena} {
			if ta != nil {
				totalArenaBytes += ta.TotalAllocated()
			}
		}
		fmt.Printf("[DIAG] GraphData.Release: capacity=%d dims=%d name=%s\n", g.Capacity, g.Dims, g.Name)
	}

	// Release Arrow references
	for i, ref := range g.ArrowRefs {
		if ref != nil {
			ref.Release()
			g.ArrowRefs[i] = nil
		}
	}
	g.ArrowRefs = nil

	// We don't set slices to nil here to avoid panics in concurrent search threads.
	// The search threads hold a reference to this GraphData object and will finish safely.

	if g.Float32Arena != nil {
		g.Float32Arena.Release()
	}
	if g.Float64Arena != nil {
		g.Float64Arena.Release()
	}
	if g.Uint8Arena != nil {
		g.Uint8Arena.Release()
	}
	if g.Uint16Arena != nil {
		g.Uint16Arena.Release()
	}
	if g.Uint32Arena != nil {
		g.Uint32Arena.Release()
	}
	if g.Uint64Arena != nil {
		g.Uint64Arena.Release()
	}
	if g.Int8Arena != nil {
		g.Int8Arena.Release()
	}
	if g.Int16Arena != nil {
		g.Int16Arena.Release()
	}
	if g.Int32Arena != nil {
		g.Int32Arena.Release()
	}
	if g.Int64Arena != nil {
		g.Int64Arena.Release()
	}
	if g.Float16Arena != nil {
		g.Float16Arena.Release()
	}
	if g.Complex64Arena != nil {
		g.Complex64Arena.Release()
	}
	if g.Complex128Arena != nil {
		g.Complex128Arena.Release()
	}

	// Release PackedNeighbors. Do NOT set slots to nil: concurrent search
	// threads (held briefly by FlatAdjacency's refs counter) read this slice
	// header and would race with a nil-out. The underlying FlatAdjacency
	// becomes inert once refs hits zero; the header stays for safety.
	for i := range g.PackedNeighbors {
		if g.PackedNeighbors[i] != nil {
			g.PackedNeighbors[i].Release()
		}
	}

	if debugRelease {
		fmt.Printf("[DIAG] GraphData.Release: done. %s\n", memory.DebugSlabPoolsSnapshot())
	}
}

func (g *GraphData) Unregister() {
	if g.Float32Arena != nil {
		memory.UnregisterArena(g.Float32Arena.Slab().StatsRecord())
	}
	if g.Float64Arena != nil {
		memory.UnregisterArena(g.Float64Arena.Slab().StatsRecord())
	}
	if g.Uint8Arena != nil {
		memory.UnregisterArena(g.Uint8Arena.Slab().StatsRecord())
	}
	if g.Uint16Arena != nil {
		memory.UnregisterArena(g.Uint16Arena.Slab().StatsRecord())
	}
	if g.Uint32Arena != nil {
		memory.UnregisterArena(g.Uint32Arena.Slab().StatsRecord())
	}
	if g.Uint64Arena != nil {
		memory.UnregisterArena(g.Uint64Arena.Slab().StatsRecord())
	}
	if g.Int8Arena != nil {
		memory.UnregisterArena(g.Int8Arena.Slab().StatsRecord())
	}
	if g.Int16Arena != nil {
		memory.UnregisterArena(g.Int16Arena.Slab().StatsRecord())
	}
	if g.Int32Arena != nil {
		memory.UnregisterArena(g.Int32Arena.Slab().StatsRecord())
	}
	if g.Int64Arena != nil {
		memory.UnregisterArena(g.Int64Arena.Slab().StatsRecord())
	}
	if g.Float16Arena != nil {
		memory.UnregisterArena(g.Float16Arena.Slab().StatsRecord())
	}
	if g.Complex64Arena != nil {
		memory.UnregisterArena(g.Complex64Arena.Slab().StatsRecord())
	}
	if g.Complex128Arena != nil {
		memory.UnregisterArena(g.Complex128Arena.Slab().StatsRecord())
	}
}

func (g *GraphData) EstimateMemory() int64 {
	var total int64
	if g.Float32Arena != nil {
		total += g.Float32Arena.TotalAllocated()
	}
	if g.Float64Arena != nil {
		total += g.Float64Arena.TotalAllocated()
	}
	if g.Uint8Arena != nil {
		total += g.Uint8Arena.TotalAllocated()
	}
	if g.Uint16Arena != nil {
		total += g.Uint16Arena.TotalAllocated()
	}
	if g.Uint32Arena != nil {
		total += g.Uint32Arena.TotalAllocated()
	}
	if g.Uint64Arena != nil {
		total += g.Uint64Arena.TotalAllocated()
	}
	if g.Int8Arena != nil {
		total += g.Int8Arena.TotalAllocated()
	}
	if g.Int16Arena != nil {
		total += g.Int16Arena.TotalAllocated()
	}
	if g.Int32Arena != nil {
		total += g.Int32Arena.TotalAllocated()
	}
	if g.Int64Arena != nil {
		total += g.Int64Arena.TotalAllocated()
	}
	if g.Float16Arena != nil {
		total += g.Float16Arena.TotalAllocated()
	}
	if g.Complex64Arena != nil {
		total += g.Complex64Arena.TotalAllocated()
	}
	if g.Complex128Arena != nil {
		total += g.Complex128Arena.TotalAllocated()
	}

	// Add Go-allocated slices overhead
	total += int64(len(g.VectorsF32) * 8)
	total += int64(len(g.VectorsPQ) * 8)
	total += int64(len(g.VectorsInt8) * 8)
	total += int64(len(g.VectorsInt16) * 8)
	total += int64(len(g.VectorsUint16) * 8)
	total += int64(len(g.VectorsF16) * 8)
	total += int64(len(g.VectorsBQ) * 8)
	total += int64(len(g.VectorsSQ8) * 8)
	total += int64(len(g.VectorsTQ) * 8)
	total += int64(len(g.VectorsInt64) * 8)
	total += int64(len(g.VectorsUint64) * 8)
	total += int64(len(g.VectorsInt32) * 8)
	total += int64(len(g.VectorsUint32) * 8)
	total += int64(len(g.Neighbors) * 24) // roughly 24 bytes per slice header
	total += int64(len(g.Levels) * 24)

	return total
}
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
