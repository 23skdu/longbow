package types

import (
	"fmt"
	"runtime"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

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

	// Share the per-chunk vector arrays instead of copying them (roadmap Item
	// 33). A 100k float32 dim-128 ingest cloned the vector arena once per
	// sub-batch: 756 MB of the 4.1 GB it allocated, for bytes that never
	// change.
	//
	// The outer [][]float32 is still fresh, so SetExternalVectorsChunk on one
	// snapshot replaces only that snapshot's own slot. The per-chunk arrays are
	// shared because nothing writes into them after publication: the only
	// writers replace the whole slice (SetExternalVectorsChunk,
	// SetZeroCopyMapping), and the modern write path goes through the typed
	// arenas and their offsets rather than through Vectors[i]. That is the
	// argument ShallowStructuralClone already makes, and it is now the one the
	// hot path relies on.
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
	// Complex128Magnitudes stays a copy: unlike the chunk arrays it is a flat
	// slice indexed by global id and grown as nodes are added, so two snapshots
	// sharing one array would append into each other.
	if g.Complex128Magnitudes != nil {
		newG.Complex128Magnitudes = make([]float64, len(g.Complex128Magnitudes))
		copy(newG.Complex128Magnitudes, g.Complex128Magnitudes)
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
	if g.Complex128Magnitudes != nil {
		newG.Complex128Magnitudes = make([]float64, len(g.Complex128Magnitudes))
		copy(newG.Complex128Magnitudes, g.Complex128Magnitudes)
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

// MergeFrom merges non-zero chunk offsets, arenas, and node levels from other
// into g so that publishing g via compareAndSwapData never clobbers chunks or
// levels allocated by a concurrent AddBatch on other.
func (g *GraphData) MergeFrom(other *GraphData) {
	if g == nil || other == nil || g == other {
		return
	}
	atomic.AddInt32(&other.cloneCount, 1)
	defer atomic.AddInt32(&other.cloneCount, -1)

	if other.Capacity > g.Capacity {
		numChunks := (other.Capacity + ChunkSize - 1) / ChunkSize
		g.GrowMetadataSlices(numChunks)
		g.Capacity = other.Capacity
	}

	if g.Float32Arena == nil && other.Float32Arena != nil {
		g.Float32Arena = memory.NewTypedArena[float32](other.Float32Arena.Slab())
		g.Float32Arena.Retain()
	}
	if g.Float64Arena == nil && other.Float64Arena != nil {
		g.Float64Arena = memory.NewTypedArena[float64](other.Float64Arena.Slab())
		g.Float64Arena.Retain()
	}
	if g.Uint8Arena == nil && other.Uint8Arena != nil {
		g.Uint8Arena = memory.NewTypedArena[uint8](other.Uint8Arena.Slab())
		g.Uint8Arena.Retain()
	}
	if g.Uint16Arena == nil && other.Uint16Arena != nil {
		g.Uint16Arena = memory.NewTypedArena[uint16](other.Uint16Arena.Slab())
		g.Uint16Arena.Retain()
	}
	if g.Uint32Arena == nil && other.Uint32Arena != nil {
		g.Uint32Arena = memory.NewTypedArena[uint32](other.Uint32Arena.Slab())
		g.Uint32Arena.Retain()
	}
	if g.Uint64Arena == nil && other.Uint64Arena != nil {
		g.Uint64Arena = memory.NewTypedArena[uint64](other.Uint64Arena.Slab())
		g.Uint64Arena.Retain()
	}
	if g.Int8Arena == nil && other.Int8Arena != nil {
		g.Int8Arena = memory.NewTypedArena[int8](other.Int8Arena.Slab())
		g.Int8Arena.Retain()
	}
	if g.Int16Arena == nil && other.Int16Arena != nil {
		g.Int16Arena = memory.NewTypedArena[int16](other.Int16Arena.Slab())
		g.Int16Arena.Retain()
	}
	if g.Int32Arena == nil && other.Int32Arena != nil {
		g.Int32Arena = memory.NewTypedArena[int32](other.Int32Arena.Slab())
		g.Int32Arena.Retain()
	}
	if g.Int64Arena == nil && other.Int64Arena != nil {
		g.Int64Arena = memory.NewTypedArena[int64](other.Int64Arena.Slab())
		g.Int64Arena.Retain()
	}
	if g.Float16Arena == nil && other.Float16Arena != nil {
		g.Float16Arena = memory.NewTypedArena[float16.Num](other.Float16Arena.Slab())
		g.Float16Arena.Retain()
	}
	if g.Complex64Arena == nil && other.Complex64Arena != nil {
		g.Complex64Arena = memory.NewTypedArena[complex64](other.Complex64Arena.Slab())
		g.Complex64Arena.Retain()
	}
	if g.Complex128Arena == nil && other.Complex128Arena != nil {
		g.Complex128Arena = memory.NewTypedArena[complex128](other.Complex128Arena.Slab())
		g.Complex128Arena.Retain()
	}

	mergeOffsets := func(dst, src []uint64) []uint64 {
		if len(src) == 0 {
			return dst
		}
		if len(dst) < len(src) {
			grown := make([]uint64, len(src))
			for i := range dst {
				grown[i] = atomic.LoadUint64(&dst[i])
			}
			dst = grown
		}
		for i := range src {
			if atomic.LoadUint64(&dst[i]) == 0 {
				if v := atomic.LoadUint64(&src[i]); v != 0 {
					atomic.StoreUint64(&dst[i], v)
				}
			}
		}
		return dst
	}

	if len(other.VectorsF32) > 0 {
		if len(g.VectorsF32) < len(other.VectorsF32) {
			grown := make([]uint64, len(other.VectorsF32))
			for i := range g.VectorsF32 {
				grown[i] = atomic.LoadUint64(&g.VectorsF32[i])
			}
			g.VectorsF32 = grown
		}
		for i := range other.VectorsF32 {
			hasZeroCopy := i < len(g.Vectors) && g.Vectors[i] != nil
			if !hasZeroCopy && atomic.LoadUint64(&g.VectorsF32[i]) == 0 {
				if v := atomic.LoadUint64(&other.VectorsF32[i]); v != 0 {
					atomic.StoreUint64(&g.VectorsF32[i], v)
				}
			}
		}
	}
	g.VectorsSQ8 = mergeOffsets(g.VectorsSQ8, other.VectorsSQ8)
	g.VectorsPQ = mergeOffsets(g.VectorsPQ, other.VectorsPQ)
	g.VectorsBQ = mergeOffsets(g.VectorsBQ, other.VectorsBQ)
	g.VectorsTQ = mergeOffsets(g.VectorsTQ, other.VectorsTQ)
	g.VectorsF16 = mergeOffsets(g.VectorsF16, other.VectorsF16)
	g.VectorsInt8 = mergeOffsets(g.VectorsInt8, other.VectorsInt8)
	g.VectorsInt16 = mergeOffsets(g.VectorsInt16, other.VectorsInt16)
	g.VectorsUint16 = mergeOffsets(g.VectorsUint16, other.VectorsUint16)
	g.VectorsInt32 = mergeOffsets(g.VectorsInt32, other.VectorsInt32)
	g.VectorsUint32 = mergeOffsets(g.VectorsUint32, other.VectorsUint32)
	g.VectorsInt64 = mergeOffsets(g.VectorsInt64, other.VectorsInt64)
	g.VectorsUint64 = mergeOffsets(g.VectorsUint64, other.VectorsUint64)
	g.VectorsFloat64Offsets = mergeOffsets(g.VectorsFloat64Offsets, other.VectorsFloat64Offsets)
	g.VectorsComplex64Offsets = mergeOffsets(g.VectorsComplex64Offsets, other.VectorsComplex64Offsets)
	g.VectorsComplex128Offsets = mergeOffsets(g.VectorsComplex128Offsets, other.VectorsComplex128Offsets)

	for l := 0; l < len(other.Neighbors) && l < len(g.Neighbors); l++ {
		g.Neighbors[l] = mergeOffsets(g.Neighbors[l], other.Neighbors[l])
	}
	for l := 0; l < len(other.Counts) && l < len(g.Counts); l++ {
		g.Counts[l] = mergeOffsets(g.Counts[l], other.Counts[l])
	}
	for l := 0; l < len(other.Versions) && l < len(g.Versions); l++ {
		g.Versions[l] = mergeOffsets(g.Versions[l], other.Versions[l])
	}

	if len(other.Levels) > 0 {
		if len(g.Levels) < len(other.Levels) {
			newL := make([][]uint32, len(other.Levels))
			copy(newL, g.Levels)
			g.Levels = newL
		}
		for i := range other.Levels {
			if other.Levels[i] == nil {
				continue
			}
			if g.Levels[i] == nil {
				g.Levels[i] = make([]uint32, len(other.Levels[i]))
			}
			for j := 0; j < len(other.Levels[i]) && j < len(g.Levels[i]); j++ {
				if atomic.LoadUint32(&g.Levels[i][j]) == 0 {
					if lvl := atomic.LoadUint32(&other.Levels[i][j]); lvl != 0 {
						atomic.StoreUint32(&g.Levels[i][j], lvl)
					}
				}
			}
		}
	}

	if len(other.Complex128Magnitudes) > 0 {
		if len(g.Complex128Magnitudes) < len(other.Complex128Magnitudes) {
			grown := make([]float64, len(other.Complex128Magnitudes))
			copy(grown, g.Complex128Magnitudes)
			g.Complex128Magnitudes = grown
		}
		for i, mag := range other.Complex128Magnitudes {
			if g.Complex128Magnitudes[i] == 0 && mag != 0 {
				g.Complex128Magnitudes[i] = mag
			}
		}
	}
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
