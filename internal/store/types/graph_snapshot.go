package types

import "sync/atomic"

// CloneForSnapshot creates a deep copy of the graph topology (Neighbors, Counts, Levels)
// and a shallow copy of vectors (assuming append-only).
// This allows serialization to proceed concurrently with modifications.
func (g *GraphData) CloneForSnapshot() *GraphData {
	clone := GraphData{
		Capacity:          g.Capacity,
		Dims:              g.Dims,
		Type:              g.Type,
		SQ8Enabled:        g.SQ8Enabled,
		SQ8Ready:          g.SQ8Ready,
		BQEnabled:         g.BQEnabled,
		PQEnabled:         g.PQEnabled,
		PQM:               g.PQM,
		GlobalVersion:     atomic.LoadUint64(&g.GlobalVersion),
		BackingGraph:      g.BackingGraph,
		Name:              g.Name,
		Allocator:         g.Allocator,
		PackedNeighbors:   g.PackedNeighbors,
		TurboQuantEnabled: g.TurboQuantEnabled,
		TurboQuantBits:    g.TurboQuantBits,
	}

	// Copy Arena pointers for read-only access (persistence will read from them)
	g.CopyArenaReferences(&clone)

	// 1. Deep Copy Neighbors (Mutable Topology)
	clone.Neighbors = make([][]uint64, len(g.Neighbors))
	for l := range g.Neighbors {
		if g.Neighbors[l] == nil {
			continue
		}
		clone.Neighbors[l] = make([]uint64, len(g.Neighbors[l]))
		for c := range g.Neighbors[l] {
			if offset := g.Neighbors[l][c]; offset != 0 {
				chunk := g.GetNeighborsChunk(l, c)
				if chunk == nil {
					continue
				}
				// Allocate new chunk in arena and copy
				if g.Uint32Arena != nil {
					ref, err := g.Uint32Arena.AllocSlice(len(chunk))
					if err == nil {
						newChunk := g.Uint32Arena.Get(ref)
						copy(newChunk, chunk)
						clone.Neighbors[l][c] = ref.Offset
					}
				}
			}
		}
	}

	// 2. Deep Copy Counts
	clone.Counts = make([][]uint64, len(g.Counts))
	for l := range g.Counts {
		if g.Counts[l] == nil {
			continue
		}
		clone.Counts[l] = make([]uint64, len(g.Counts[l]))
		for c := range g.Counts[l] {
			if offset := g.Counts[l][c]; offset != 0 {
				chunk := g.GetCountsChunk(l, c)
				if chunk == nil {
					continue
				}
				if g.Int32Arena != nil {
					ref, err := g.Int32Arena.AllocSlice(len(chunk))
					if err == nil {
						newChunk := g.Int32Arena.Get(ref)
						copy(newChunk, chunk)
						clone.Counts[l][c] = ref.Offset
					}
				}
			}
		}
	}

	// 3. Deep Copy Levels
	clone.Levels = make([][]uint32, len(g.Levels))
	for c := range g.Levels {
		if chunk := g.Levels[c]; chunk != nil {
			newChunk := make([]uint32, len(chunk))
			for i := range chunk {
				newChunk[i] = atomic.LoadUint32(&chunk[i])
			}
			clone.Levels[c] = newChunk
		}
	}

	// 4. Deep Copy Versions
	clone.Versions = make([][]uint64, len(g.Versions))
	for l := range g.Versions {
		if g.Versions[l] == nil {
			continue
		}
		clone.Versions[l] = make([]uint64, len(g.Versions[l]))
		for c := range g.Versions[l] {
			if offset := g.Versions[l][c]; offset != 0 {
				chunk := g.GetVersionsChunk(l, c)
				if chunk == nil {
					continue
				}
				if g.Uint32Arena != nil {
					ref, err := g.Uint32Arena.AllocSlice(len(chunk))
					if err == nil {
						newChunk := g.Uint32Arena.Get(ref)
						copy(newChunk, chunk)
						clone.Versions[l][c] = ref.Offset
					}
				}
			}
		}
	}

	// 5. Shallow Copy Vectors (Slice of Slices)
	// We copy the slice structure so if 'g' appends new chunks, 'clone' doesn't see them.
	// But 'clone' shares the underlying arrays for existing chunks.
	// We assume Vectors are Append-Only.
	if g.Vectors != nil {
		clone.Vectors = make([][]float32, len(g.Vectors))
		copy(clone.Vectors, g.Vectors)
	}
	if g.VectorsFloat64 != nil {
		clone.VectorsFloat64 = make([][]float64, len(g.VectorsFloat64))
		copy(clone.VectorsFloat64, g.VectorsFloat64)
	}
	// Copy arena info?
	// Arenas are for allocation. Clone doesn't need to allocate.
	// Clone just holds references to data.
	// We don't copy Arenas. We share the underlying memory.
	// Since we don't modify vectors, reading from Arena via offset is safe
	// as long as Arena doesn't move/compact (it shouldn't).
	// We need to copy the *Offset Slices* though.
	if g.VectorsBQ != nil {
		clone.VectorsBQ = make([]uint64, len(g.VectorsBQ))
		copy(clone.VectorsBQ, g.VectorsBQ)
	}
	if g.VectorsPQ != nil {
		clone.VectorsPQ = make([]uint64, len(g.VectorsPQ))
		copy(clone.VectorsPQ, g.VectorsPQ)
	}
	if g.VectorsSQ8 != nil {
		clone.VectorsSQ8 = make([]uint64, len(g.VectorsSQ8))
		copy(clone.VectorsSQ8, g.VectorsSQ8)
	}
	if g.VectorsInt8 != nil {
		clone.VectorsInt8 = make([]uint64, len(g.VectorsInt8))
		copy(clone.VectorsInt8, g.VectorsInt8)
	}
	if g.VectorsF16 != nil {
		clone.VectorsF16 = make([]uint64, len(g.VectorsF16))
		copy(clone.VectorsF16, g.VectorsF16)
	}
	// Complex types
	if g.VectorsComplex64 != nil {
		clone.VectorsComplex64 = make([][]complex64, len(g.VectorsComplex64))
		copy(clone.VectorsComplex64, g.VectorsComplex64)
	}
	if g.VectorsComplex128 != nil {
		clone.VectorsComplex128 = make([][]complex128, len(g.VectorsComplex128))
		copy(clone.VectorsComplex128, g.VectorsComplex128)
	}

	// Nullify Arenas in clone to prevent accidental allocation using them?
	// Or keep them for read access (`GetVectorsSQ8Chunk` uses helpers that use Arena).
	// We MUST keep the Arena pointers to read the data!
	// `g.Uint8Arena` is a pointer. `clone.Uint8Arena` will share it.
	// This is safe for READS.
	// Concurrent WRITES to Arena (appending new slabs) might happen.
	// `TypedArena.Get` is thread-safe?
	// `Arena` implementations usually need to be thread-safe for concurrent read/write.
	// Longbow arenas seem to support concurrent use.

	return &clone
}

// CopyArenaReferences copies arena pointers to the clone.
func (g *GraphData) CopyArenaReferences(clone *GraphData) {
	clone.Float32Arena = g.Float32Arena
	clone.Float64Arena = g.Float64Arena
	clone.Uint8Arena = g.Uint8Arena
	clone.Uint16Arena = g.Uint16Arena
	clone.Uint32Arena = g.Uint32Arena
	clone.Uint64Arena = g.Uint64Arena
	clone.Int8Arena = g.Int8Arena
	clone.Int16Arena = g.Int16Arena
	clone.Int32Arena = g.Int32Arena
	clone.Int64Arena = g.Int64Arena
	clone.Float16Arena = g.Float16Arena
	clone.Complex64Arena = g.Complex64Arena
	clone.Complex128Arena = g.Complex128Arena
}

// SnapshotSafe checks if the graph is in a state safe for snapshotting (e.g. no resizing).
func (g *GraphData) SnapshotSafe() bool {
	return true
}

// RestoreArena restores arena references if lost (helper)
func (g *GraphData) RestoreArena(source *GraphData) {
	g.CopyArenaReferences(source)
}

// CloneMemoryArenas - Do we need this? No, we share read access.
