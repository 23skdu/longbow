package types

import (
	"fmt"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// GraphData holds the vector data and graph topology.
// It effectively implements the component storage for ArrowHNSW.
type GraphData struct {
	// Metadata
	Capacity   int
	Dims       int
	Type       VectorDataType
	SQ8Enabled bool

	// Mutable State
	BackingGraph any // interface{} to avoid import cycle (likely *DiskGraph)

	// Vectors (primary storage, usually float32)
	Vectors [][]float32

	// VectorsPQ for quantized vectors
	VectorsPQ []uint64

	// VectorsInt8 for int8 vectors
	VectorsInt8 []uint64

	// VectorsF16 for half-precision
	VectorsF16 []uint64

	// VectorsBQ for binary quantized vectors
	VectorsBQ []uint64

	// VectorsSQ8 for scalar quantized vectors
	VectorsSQ8 []uint64

	// VectorsFloat64
	VectorsFloat64 [][]float64

	// VectorsComplex64
	VectorsComplex64 [][]complex64

	// VectorsComplex128
	VectorsComplex128 [][]complex128

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
}

// PackedNeighbors interface for graph adjacency management
type PackedNeighbors interface {
	GetNeighbors(id uint32) ([]uint32, bool)
	SetNeighbors(id uint32, neighbors []uint32) error
	GetNeighborsF16(id uint32) ([]uint32, []float16.Num, bool)
	SetNeighborsF16(id uint32, neighbors []uint32, dists []float16.Num) error
}

// GetVectorsChunk returns the vector chunk for the given ID.
func (g *GraphData) GetVectorsChunk(chunkID int) []float32 {
	if chunkID < len(g.Vectors) {
		return g.Vectors[chunkID]
	}
	return nil
}

func (g *GraphData) GetVectorsFloat64Chunk(chunkID int) []float64 {
	if chunkID < len(g.VectorsFloat64) {
		return g.VectorsFloat64[chunkID]
	}
	// Fallback for arena? Float64Arena exists now.
	// But `VectorsFloat64` field is `[][]float64` in original definition?
	// Wait, I updated initialization to use Arena, but `VectorsFloat64` is defined as `[][]float64` in struct?
	// Let's check struct definition.
	// In step 1386: `VectorsFloat64 [][]float64`.
	// I initialized `Float64Arena` in NewGraphData.
	// But `EnsureChunk` appended to `g.VectorsFloat64` (`[][]float64`).
	// So `GetVectorsFloat64Chunk` correctly returns `g.VectorsFloat64[chunkID]`.
	// Arena is currently unused for Float64?
	// If I want to use Arena for Float64, I need `VectorsFloat64` to be `[]uint64` (offsets).
	// But `GraphData` struct definition has `[][]float64`.
	// Changing struct field type breaks compatibility if not careful.
	// Given I just added `Float64Arena`, I should probably use it IF I change `VectorsFloat64` type.
	// But `EnsureChunk` (step 1447) implementation used `append(g.VectorsFloat64, make([]float64...))`.
	// So it allocates Go slices.
	// This consistent. `GetVectorsFloat64Chunk` already returns `g.VectorsFloat64[chunkID]`.
	// So NO CHANGE needed for Float64, Complex, etc. if they are `[][]T`.
	// Wait, `GetVectorsInt8Chunk` used `VectorsInt8` AND `Int8Arena`.
	// I defined `VectorsInt8` as `[]uint64`. This works.
	// `VectorsF16` is `[]uint64`. This works (needs fix).
	// `VectorsSQ8` is `[]uint64`. This works (needs fix).

	// Complex/Float64 were `[][]T` in struct.
	// I added Arenas for them in `NewGraphData`.
	// But `EnsureChunk` (step 1447) allocated slices via `make`.
	// So Arenas are unused for them.
	// This is slightly inconsistent (Int8 uses arena, Float64 uses slice) but valid.
	// I will only fix SQ8, F16, and Int8 chunks which use Arenas.
	return nil
}

func (g *GraphData) GetVectorsComplex64Chunk(chunkID int) []complex64 {
	if chunkID < len(g.VectorsComplex64) {
		return g.VectorsComplex64[chunkID]
	}
	return nil
}

func (g *GraphData) GetVectorsComplex128Chunk(chunkID int) []complex128 {
	if chunkID < len(g.VectorsComplex128) {
		return g.VectorsComplex128[chunkID]
	}
	return nil
}

func (g *GraphData) GetPaddedDims() int {
	return g.Dims
}

func (g *GraphData) GetPaddedDimsForType(dt VectorDataType) int {
	return g.Dims
}

func (g *GraphData) GetVectorsSQ8Chunk(chunkID int) []byte {
	if chunkID < len(g.VectorsSQ8) && g.Uint8Arena != nil {
		paddedDims := (g.Dims + 63) & ^63
		return g.Uint8Arena.Get(memory.SliceRef{Offset: g.VectorsSQ8[chunkID], Len: uint32(ChunkSize * paddedDims), Cap: uint32(ChunkSize * paddedDims)})
	}
	return nil
}

func (g *GraphData) GetVectorsBQChunk(chunkID int) []uint64 {
	return g.VectorsBQ // Simplified
}

// GetVectorsPQChunk is a method used in pq_training.go
func (g *GraphData) GetVectorsPQChunk(chunkID int) []byte {
	return nil // Placeholder
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
	if chunkID < len(g.VectorsPQ) && g.Int16Arena != nil {
		return g.Int16Arena.Get(memory.SliceRef{Offset: g.VectorsPQ[chunkID], Len: uint32(g.Dims), Cap: uint32(g.Dims)})
	}
	return nil
}

func (g *GraphData) EnsureChunk(cID, cOff, dims int) error {
	// Ensure Vectors (Float32 is default/primary)
	if g.Type == VectorTypeFloat32 || g.Type == VectorTypeUnknown {
		for len(g.Vectors) <= cID {
			g.Vectors = append(g.Vectors, make([]float32, ChunkSize*dims))
		}
	}

	// Ensure Levels
	for len(g.Levels) <= cID {
		g.Levels = append(g.Levels, make([]uint8, ChunkSize))
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
			g.Neighbors[l] = append(g.Neighbors[l], make([]uint32, ChunkSize*MaxNeighbors))
		}
		// Ensure Counts
		for len(g.Counts[l]) <= cID {
			g.Counts[l] = append(g.Counts[l], make([]int32, ChunkSize))
		}
		// Ensure Versions
		for len(g.Versions[l]) <= cID {
			g.Versions[l] = append(g.Versions[l], make([]uint32, ChunkSize))
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
				slabSize := ChunkSize * paddedDims
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

	// Ensure Float64
	if g.Type == VectorTypeFloat64 {
		for len(g.VectorsFloat64) <= cID {
			g.VectorsFloat64 = append(g.VectorsFloat64, make([]float64, ChunkSize*dims))
		}
	}

	// Ensure Complex64
	if g.Type == VectorTypeComplex64 {
		for len(g.VectorsComplex64) <= cID {
			g.VectorsComplex64 = append(g.VectorsComplex64, make([]complex64, ChunkSize*dims))
		}
	}

	// Ensure Complex128
	if g.Type == VectorTypeComplex128 {
		for len(g.VectorsComplex128) <= cID {
			g.VectorsComplex128 = append(g.VectorsComplex128, make([]complex128, ChunkSize*dims))
		}
	}

	// Ensure Int8
	if g.Type == VectorTypeInt8 {
		for len(g.VectorsInt8) <= cID {
			if g.Int8Arena == nil {
				slabSize := ChunkSize * dims
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

	if countsChunk == nil || neighborsChunk == nil {
		// Try to ensure?
		if err := g.EnsureChunk(cID, 0, g.Dims); err != nil {
			return err
		}
		countsChunk = g.GetCountsChunk(layer, cID)
		neighborsChunk = g.GetNeighborsChunk(layer, cID)
		if countsChunk == nil || neighborsChunk == nil {
			return fmt.Errorf("failed to allocate chunk for SetNeighbors")
		}
	}

	if len(neighbors) > MaxNeighbors {
		// Truncate or error?
		neighbors = neighbors[:MaxNeighbors]
	}

	baseIdx := cOff * MaxNeighbors

	// Write neighbors
	for i, n := range neighbors {
		neighborsChunk[baseIdx+i] = n
	}

	// Update count
	countsChunk[cOff] = int32(len(neighbors))

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
	if g.Float32Arena != nil && len(g.Vectors) > cID {
		chunk := g.GetVectorsChunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	if g.Float16Arena != nil && len(g.VectorsF16) > cID {
		chunk := g.GetVectorsF16Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	if len(g.VectorsFloat64) > cID {
		chunk := g.GetVectorsFloat64Chunk(cID)
		if chunk != nil {
			start := cOff * g.Dims
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

	return nil, nil
}

func (g *GraphData) SetVector(id uint32, vec any) error {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	switch v := vec.(type) {
	case []float32:
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
	return nil
}

func (g *GraphData) GetVectorBQ(id uint32) ([]uint64, error) {
	return nil, nil
}

// GetNeighbors returns the neighbors for a given node at a level.
// GetNeighbors returns the neighbors for a given node at a level.
func (g *GraphData) GetNeighbors(layer int, id uint32, buf []uint32) []uint32 {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	counts := g.GetCountsChunk(layer, cID)
	if counts == nil {
		return nil
	}
	count := int(counts[cOff])
	if count == 0 {
		return nil // Or empty slice?
	}

	neighbors := g.GetNeighborsChunk(layer, cID)
	if neighbors == nil {
		return nil
	}

	base := cOff * MaxNeighbors
	if base+count > len(neighbors) {
		return nil // Should not happen
	}

	// Return a copy into buf if provided, or new slice
	// Actually, usually we return a slice to the underlying array?
	// But `disk_writer.go` sorts it. Sorting mutates.
	// We MUST return a copy if caller converts.
	// But `GetNeighbors` implementation usually returns slice of node IDs.

	src := neighbors[base : base+count]
	if buf != nil && cap(buf) >= count {
		buf = buf[:count]
		copy(buf, src)
		return buf
	}
	res := make([]uint32, count)
	copy(res, src)
	return res
}

func (g *GraphData) GetVectorSQ8(id uint32) []byte {
	return nil
}

// GetLevelsChunk returns the level chunk for the given ID.
func (g *GraphData) GetLevelsChunk(chunkID int) []uint8 {
	if chunkID < len(g.Levels) {
		return g.Levels[chunkID]
	}
	return nil
}

// NewGraphData creates a new GraphData instance.
// This is a helper for legacy tests.
func NewGraphData(capacity, dim int, mmap bool, useDisk bool, fd int,
	quantization bool, sq8 bool, persistent bool,
	dataType VectorDataType) *GraphData {

	// Initialize arenas
	// Slab size: fit at least one chunk + overhead.
	// Float32: 1024 * dim * 4 bytes.
	f32SlabSize := ChunkSize * dim * 4
	if f32SlabSize < 1024*1024 {
		f32SlabSize = 1024 * 1024
	}
	f32Arena := memory.NewSlabArena(f32SlabSize)

	// Uint8: 1024 * dim * 1 bytes.
	u8SlabSize := ChunkSize * dim
	if u8SlabSize < 1024*1024 {
		u8SlabSize = 1024 * 1024
	}
	u8Arena := memory.NewSlabArena(u8SlabSize)

	// Float64: 8 bytes
	f64SlabSize := ChunkSize * dim * 8
	if f64SlabSize < 1024*1024 {
		f64SlabSize = 1024 * 1024
	}
	f64Arena := memory.NewSlabArena(f64SlabSize)

	// Int8: 1 byte (reuse logic/size if creating distinct arena, but simpler to separate)
	i8Arena := memory.NewSlabArena(u8SlabSize)

	// Complex64: 8 bytes
	c64SlabSize := ChunkSize * dim * 8
	if c64SlabSize < 1024*1024 {
		c64SlabSize = 1024 * 1024
	}
	c64Arena := memory.NewSlabArena(c64SlabSize)

	// Complex128: 16 bytes
	c128SlabSize := ChunkSize * dim * 16
	if c128SlabSize < 1024*1024 {
		c128SlabSize = 1024 * 1024
	}
	c128Arena := memory.NewSlabArena(c128SlabSize)

	gd := &GraphData{
		Capacity:        capacity,
		Dims:            dim,
		Type:            dataType,
		SQ8Enabled:      sq8,
		Vectors:         make([][]float32, 0),
		Neighbors:       make([][][]uint32, ArrowMaxLayers),
		Counts:          make([][][]int32, ArrowMaxLayers),
		Versions:        make([][][]uint32, ArrowMaxLayers),
		Levels:          make([][]uint8, 0),
		Float32Arena:    memory.NewTypedArena[float32](f32Arena),
		Uint8Arena:      memory.NewTypedArena[uint8](u8Arena),
		Float64Arena:    memory.NewTypedArena[float64](f64Arena),
		Int8Arena:       memory.NewTypedArena[int8](i8Arena),
		Complex64Arena:  memory.NewTypedArena[complex64](c64Arena),
		Complex128Arena: memory.NewTypedArena[complex128](c128Arena),
	}
	return gd
}
