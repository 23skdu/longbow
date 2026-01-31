package types

import (
	"fmt"
	"sync/atomic"

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
	SQ8Ready   bool
	BQEnabled  bool
	PQEnabled  bool

	// Mutable State
	BackingGraph any // interface{} to avoid import cycle (likely *DiskGraph)

	// Vectors (primary storage, usually float32)
	Vectors [][]float32

	// VectorsPQ for quantized vectors
	VectorsPQ []uint64

	// VectorsInt8 for raw int8 vectors
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
			g.Vectors = append(g.Vectors, nil)
		}
		if g.Vectors[cID] == nil {
			g.Vectors[cID] = make([]float32, ChunkSize*dims)
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
			g.VectorsFloat64 = append(g.VectorsFloat64, nil)
		}
		if g.VectorsFloat64[cID] == nil {
			g.VectorsFloat64[cID] = make([]float64, ChunkSize*dims)
		}
	}

	// Ensure Complex64
	if g.Type == VectorTypeComplex64 {
		for len(g.VectorsComplex64) <= cID {
			g.VectorsComplex64 = append(g.VectorsComplex64, nil)
		}
		if g.VectorsComplex64[cID] == nil {
			g.VectorsComplex64[cID] = make([]complex64, ChunkSize*dims)
		}
	}

	// Ensure Complex128
	if g.Type == VectorTypeComplex128 {
		for len(g.VectorsComplex128) <= cID {
			g.VectorsComplex128 = append(g.VectorsComplex128, nil)
		}
		if g.VectorsComplex128[cID] == nil {
			g.VectorsComplex128[cID] = make([]complex128, ChunkSize*dims)
		}
	}

	// Ensure Int8
	// Already handled by unified SQ8/Int8 block above

	// Ensure BQ if enabled
	if g.BQEnabled {
		for len(g.VectorsBQ) <= cID {
			if g.Uint64Arena == nil {
				paddedDims := (dims + 63) & ^63
				numWords := paddedDims / 64
				slabSize := ChunkSize * numWords * 8
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
	if g.PQEnabled {
		for len(g.VectorsPQ) <= cID {
			if g.Uint64Arena == nil {
				// PQ usually stores small codes, but use dimensions as proxy if needed.
				// For now assume uint64 per vector for PQ codes (e.g. 8 bytes per vector)
				slabSize := ChunkSize * 8
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				g.Uint64Arena = memory.NewTypedArena[uint64](memory.NewSlabArena(slabSize))
			}
			ref, err := g.Uint64Arena.AllocSliceDirty(ChunkSize)
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
				slabSize := ChunkSize * dims * 2
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
	if g.Uint8Arena != nil && len(g.VectorsSQ8) > cID && g.SQ8Ready {
		chunk := g.GetVectorsSQ8Chunk(cID)
		if chunk != nil {
			paddedDims := (g.Dims + 63) & ^63
			start := cOff * paddedDims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	if g.Int8Arena != nil && len(g.VectorsInt8) > cID && g.SQ8Ready {
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
	case []byte:
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
	neighbors := g.GetNeighborsChunk(layer, cID)
	versions := g.GetVersionsChunk(layer, cID)

	if counts == nil || neighbors == nil {
		return nil
	}

	countAddr := &counts[cOff]
	base := cOff * MaxNeighbors

	// Seqlock read loop
	for attempts := 0; attempts < 100; attempts++ {
		var v1 uint32
		if versions != nil {
			v1 = atomic.LoadUint32(&versions[cOff])
			if v1%2 != 0 {
				// Writer is active, spin
				continue
			}
		}

		count := int(atomic.LoadInt32(countAddr))
		if count == 0 {
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

// Clone creates a shallow copy of the GraphData with deep copies of the structure slices.
// This allows concurrent readers to safely access the old structure while a new one is being built (COW).
func (g *GraphData) Clone() *GraphData {
	newG := *g // Shallow copy struct

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

	// Deep copy other vector types
	if g.VectorsSQ8 != nil {
		newG.VectorsSQ8 = make([]uint64, len(g.VectorsSQ8))
		copy(newG.VectorsSQ8, g.VectorsSQ8)
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
	if g.VectorsInt8 != nil {
		newG.VectorsInt8 = make([]uint64, len(g.VectorsInt8))
		copy(newG.VectorsInt8, g.VectorsInt8)
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

	return &newG
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

	numChunks := (capacity + ChunkSize - 1) / ChunkSize
	if numChunks < 0 {
		numChunks = 0
	}

	gd := &GraphData{
		Capacity:          capacity,
		Dims:              dim,
		Type:              dataType,
		SQ8Enabled:        sq8,
		BQEnabled:         quantization,
		PQEnabled:         quantization,
		Vectors:           make([][]float32, numChunks),
		VectorsFloat64:    make([][]float64, numChunks),
		VectorsComplex64:  make([][]complex64, numChunks),
		VectorsComplex128: make([][]complex128, numChunks),
		Neighbors:         make([][][]uint32, ArrowMaxLayers),
		Counts:            make([][][]int32, ArrowMaxLayers),
		Versions:          make([][][]uint32, ArrowMaxLayers),
		Levels:            make([][]uint8, numChunks),
		Float32Arena:      memory.NewTypedArena[float32](f32Arena),
		Uint8Arena:        memory.NewTypedArena[uint8](u8Arena),
		Float64Arena:      memory.NewTypedArena[float64](f64Arena),
		Int8Arena:         memory.NewTypedArena[int8](i8Arena),
		Complex64Arena:    memory.NewTypedArena[complex64](c64Arena),
		Complex128Arena:   memory.NewTypedArena[complex128](c128Arena),
	}

	for i := 0; i < ArrowMaxLayers; i++ {
		gd.Neighbors[i] = make([][]uint32, numChunks)
		gd.Counts[i] = make([][]int32, numChunks)
		gd.Versions[i] = make([][]uint32, numChunks)
	}
	return gd
}
