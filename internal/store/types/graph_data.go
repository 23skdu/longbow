package types

import (
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"runtime"

	"github.com/23skdu/longbow/internal/memory"
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

	// Complex128Magnitudes holds pre-computed L2 magnitudes for complex128 vectors.
	// Indexed by global id. Used during search for triangle-inequality pruning.
	Complex128Magnitudes []float64

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
	tqPackedSize      int64 // cached PackedSize() result; 0 = uninitialized (atomic access)

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
	// GetNeighborsWithGenFast is like GetNeighborsWithGen but skips ref-counting.
	// Caller must guarantee the PackedNeighbors remains alive during the call.
	GetNeighborsWithGenFast(id uint32, maxGen uint64) ([]uint32, bool)
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

// VectorChunkBatch is a batch-scoped view over a chunked typed arena. It holds
// the arena batch together with the chunk offset table so that a per-vector
// loop (distance evaluation, for example) resolves the slab table and the
// generation policy once and then indexes vectors directly.
//
// Every accessor mirrors its GetXChunkWithGen counterpart exactly: the same
// chunk selection, the same arena/legacy fallback, the same generation
// visibility and the same bounds rejection, so any vector the reference
// accessor would not serve yields nil here as well.

func (g *GraphData) PackedSize() int {
	if g.Dims <= 0 {
		return 0
	}
	if ps := atomic.LoadInt64(&g.tqPackedSize); ps > 0 {
		return int(ps)
	}
	p2 := int(1 << uint(math.Ceil(math.Log2(float64(g.Dims)))))
	angleBytes := ((p2-1)*g.TurboQuantBits + 7) / 8
	bitBytes := (p2 + 7) / 8
	size := 4 + angleBytes + bitBytes
	size = (size + 3) &^ 3 // Pad to 4 bytes for GPU alignment
	atomic.StoreInt64(&g.tqPackedSize, int64(size))
	return size
}

// GetVectorsTQChunk returns a chunk of TurboQuant compressed vectors.

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

func (g *GraphData) DiskStore() any {
	return g.BackingGraph
}

func (g *GraphData) PQDims() int {
	return 0
}

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
		minSlabSize := ChunkSize*dim*4 + 64
		if minSlabSize < 256*1024 {
			minSlabSize = 256 * 1024
		}

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
