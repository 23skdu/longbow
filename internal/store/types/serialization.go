package types

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	GraphSnapshotVersion = 1
	SerializationMagic   = 0x4C424752 // "LBGR" (LongBow GRaph)
)

// Serialize writes the GraphData to the writer in a portable binary format.
func (g *GraphData) Serialize(w io.Writer) error {
	// Header
	if err := binary.Write(w, binary.LittleEndian, uint32(SerializationMagic)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(GraphSnapshotVersion)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, int64(g.Capacity)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, int32(g.Dims)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint8(g.Type)); err != nil {
		return err
	}

	flags := uint32(0)
	if g.SQ8Enabled {
		flags |= 1 << 0
	}
	if g.BQEnabled {
		flags |= 1 << 1
	}
	if g.PQEnabled {
		flags |= 1 << 2
	}
	if err := binary.Write(w, binary.LittleEndian, flags); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, int32(g.PQM)); err != nil {
		return err
	}

	// 1. Levels
	// Iterate through chunks to write levels
	nodesWritten := 0
	for _, chunk := range g.Levels {
		if chunk == nil {
			// Sparse graph support: write zeros if missing?
			// capacity implies dense ID space usually. Assuming sequential fill up to Len.
			// But Capacity is allocated size.
			// We should write strictly up to Capacity.
			remaining := g.Capacity - nodesWritten
			toWrite := remaining
			if toWrite > ChunkSize {
				toWrite = ChunkSize
			}
			zeros := make([]uint8, toWrite)
			if _, err := w.Write(zeros); err != nil {
				return err
			}
			nodesWritten += toWrite
		} else {
			// Write relevant part of chunk
			remaining := g.Capacity - nodesWritten
			toWrite := len(chunk)
			if toWrite > remaining {
				toWrite = remaining
			}
			if _, err := w.Write(chunk[:toWrite]); err != nil {
				return err
			}
			nodesWritten += toWrite
		}
		if nodesWritten >= g.Capacity {
			break
		}
	}

	// 2. Vectors
	// Dependent on Type
	if err := g.serializeVectors(w); err != nil {
		return err
	}

	// 3. Adjacency (Neighbors + Counts)
	// Format: For each layer (0 to MaxLayers):
	//   For each node:
	//     Count (uint32)
	//     Neighbors ([Count]uint32)
	// Note: Standard HNSW persistence usually stores per-node: [Level, Data, Neighbors_L0, Neighbors_L1...]
	// But our structure is columnar (Layers -> Chunks).
	// To match generic import/export efficiently, we'll serialize layer-wise.

	// Write MaxLayers (although const, good for versioning)
	if err := binary.Write(w, binary.LittleEndian, uint32(ArrowMaxLayers)); err != nil {
		return err
	}

	for l := 0; l < ArrowMaxLayers; l++ {
		nodesProcessed := 0
		for cID := 0; nodesProcessed < g.Capacity; cID++ {
			// Determine chunk size for this iteration
			count := ChunkSize
			if nodesProcessed+count > g.Capacity {
				count = g.Capacity - nodesProcessed
			}

			// Get Counts and Neighbors chunks
			// Note: We access safely. If nil, means 0 neighbors.
			var counts []int32
			var neighbors []uint32

			if cID < len(g.Counts[l]) {
				counts = g.Counts[l][cID]
			}
			if cID < len(g.Neighbors[l]) {
				neighbors = g.Neighbors[l][cID]
			}

			for i := 0; i < count; i++ {
				var encodedCount uint32
				if counts != nil {
					encodedCount = uint32(counts[i])
				}
				if err := binary.Write(w, binary.LittleEndian, encodedCount); err != nil {
					return err
				}

				if encodedCount > 0 {
					base := i * MaxNeighbors
					// Sanity check
					if neighbors == nil || base+int(encodedCount) > len(neighbors) {
						// Data corruption or race? Write zeroes/dummy to maintain stream alignment
						dummy := make([]uint32, encodedCount)
						if err := binary.Write(w, binary.LittleEndian, dummy); err != nil {
							return err
						}
					} else {
						if err := binary.Write(w, binary.LittleEndian, neighbors[base:base+int(encodedCount)]); err != nil {
							return err
						}
					}
				}
			}
			nodesProcessed += count
		}
	}

	return nil
}

func (g *GraphData) serializeVectors(w io.Writer) error {
	// Base on Type
	switch g.Type {
	case VectorTypeFloat32, VectorTypeUnknown:
		return g.writeFloat32Vectors(w)
	case VectorTypeFloat16:
		if err := g.writeF16Vectors(w); err != nil {
			return err
		}
	}

	// Always write quantized vectors if enabled
	if g.SQ8Enabled {
		if err := g.writeSQ8Vectors(w); err != nil {
			return err
		}
	}
	if g.BQEnabled {
		if err := g.writeBQVectors(w); err != nil {
			return err
		}
	}
	if g.PQEnabled {
		if err := g.writePQVectors(w); err != nil {
			return err
		}
	}
	return nil
}

func (g *GraphData) writeSQ8Vectors(w io.Writer) error {
	nodesWritten := 0
	paddedDims := (g.Dims + 63) & ^63
	for i := 0; nodesWritten < g.Capacity; i++ {
		remaining := g.Capacity - nodesWritten
		toWriteNodes := ChunkSize
		if toWriteNodes > remaining {
			toWriteNodes = remaining
		}

		chunk := g.GetVectorsSQ8Chunk(i)
		if chunk == nil {
			zeros := make([]byte, toWriteNodes*paddedDims)
			if _, err := w.Write(zeros); err != nil {
				return err
			}
		} else {
			limit := toWriteNodes * paddedDims
			if _, err := w.Write(chunk[:limit]); err != nil {
				return err
			}
		}
		nodesWritten += toWriteNodes
	}
	return nil
}

func (g *GraphData) writeBQVectors(w io.Writer) error {
	nodesWritten := 0
	paddedDims := (g.Dims + 63) & ^63
	numWords := paddedDims / 64
	for i := 0; nodesWritten < g.Capacity; i++ {
		remaining := g.Capacity - nodesWritten
		toWriteNodes := ChunkSize
		if toWriteNodes > remaining {
			toWriteNodes = remaining
		}

		chunk := g.GetVectorsBQChunk(i)
		if chunk == nil {
			zeros := make([]uint64, toWriteNodes*numWords)
			if err := binary.Write(w, binary.LittleEndian, zeros); err != nil {
				return err
			}
		} else {
			limit := toWriteNodes * numWords
			if err := binary.Write(w, binary.LittleEndian, chunk[:limit]); err != nil {
				return err
			}
		}
		nodesWritten += toWriteNodes
	}
	return nil
}

func (g *GraphData) writePQVectors(w io.Writer) error {
	if g.PQM <= 0 {
		return nil
	}
	nodesWritten := 0
	m := g.PQM
	for i := 0; nodesWritten < g.Capacity; i++ {
		remaining := g.Capacity - nodesWritten
		toWriteNodes := ChunkSize
		if toWriteNodes > remaining {
			toWriteNodes = remaining
		}

		chunk := g.GetVectorsPQChunk(i)
		if chunk == nil {
			zeros := make([]byte, toWriteNodes*m)
			if _, err := w.Write(zeros); err != nil {
				return err
			}
		} else {
			limit := toWriteNodes * m
			if _, err := w.Write(chunk[:limit]); err != nil {
				return err
			}
		}
		nodesWritten += toWriteNodes
	}
	return nil
}

func (g *GraphData) writeFloat32Vectors(w io.Writer) error {
	nodesWritten := 0
	for _, chunk := range g.Vectors {
		remaining := g.Capacity - nodesWritten
		if remaining <= 0 {
			break
		}

		toWriteNodes := ChunkSize
		if toWriteNodes > remaining {
			toWriteNodes = remaining
		}

		if chunk == nil {
			// Write zeros
			zeros := make([]float32, toWriteNodes*g.Dims)
			if err := binary.Write(w, binary.LittleEndian, zeros); err != nil {
				return err
			}
		} else {
			// Chunk is []float32
			limit := toWriteNodes * g.Dims
			if len(chunk) < limit {
				limit = len(chunk) // Should not happen if strictly managed
			}
			if err := binary.Write(w, binary.LittleEndian, chunk[:limit]); err != nil {
				return err
			}
		}
		nodesWritten += toWriteNodes
	}
	return nil
}

func (g *GraphData) writeF16Vectors(w io.Writer) error {
	nodesWritten := 0
	for i := 0; nodesWritten < g.Capacity; i++ {
		remaining := g.Capacity - nodesWritten
		toWriteNodes := ChunkSize
		if toWriteNodes > remaining {
			toWriteNodes = remaining
		}

		chunk := g.GetVectorsF16Chunk(i)
		if chunk == nil {
			zeros := make([]uint16, toWriteNodes*g.Dims)
			if err := binary.Write(w, binary.LittleEndian, zeros); err != nil {
				return err
			}
		} else {
			// Chunk is []float16.Num (uint16)
			limit := toWriteNodes * g.Dims
			if len(chunk) < limit {
				limit = len(chunk)
			}
			// Cast to []uint16 for binary write optimization? float16.Num usually wraps uint16
			// binary.Write handles slices of fixed-size values fine.
			if err := binary.Write(w, binary.LittleEndian, chunk[:limit]); err != nil {
				return err
			}
		}
		nodesWritten += toWriteNodes
	}
	return nil
}

// Deserialize reads GraphData from the reader.
func DeserializeGraphData(r io.Reader) (*GraphData, error) {
	var magic uint32
	if err := binary.Read(r, binary.LittleEndian, &magic); err != nil {
		return nil, err
	}
	if magic != SerializationMagic {
		return nil, fmt.Errorf("invalid magic: 0x%x", magic)
	}

	var version uint32
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return nil, err
	}
	if version != GraphSnapshotVersion {
		return nil, fmt.Errorf("unsupported version: %d", version)
	}

	var capacity int64
	if err := binary.Read(r, binary.LittleEndian, &capacity); err != nil {
		return nil, err
	}

	var dims int32
	if err := binary.Read(r, binary.LittleEndian, &dims); err != nil {
		return nil, err
	}

	var typeCode uint8
	if err := binary.Read(r, binary.LittleEndian, &typeCode); err != nil {
		return nil, err
	}

	var flags uint32
	if err := binary.Read(r, binary.LittleEndian, &flags); err != nil {
		return nil, err
	}

	sq8 := (flags & 1) != 0
	bq := (flags & 2) != 0
	pqEnabled := (flags & 4) != 0

	var pqM int32
	if err := binary.Read(r, binary.LittleEndian, &pqM); err != nil {
		return nil, err
	}

	// Initialize GraphData
	g := NewGraphData(int(capacity), int(dims), false, false, 0, false, sq8, false, VectorDataType(typeCode), bq, pqEnabled)
	g.PQM = int(pqM)

	// 1. Levels
	// Read into chunks
	nodesRead := 0
	cID := 0
	for nodesRead < int(capacity) {
		remaining := int(capacity) - nodesRead
		toRead := ChunkSize
		if toRead > remaining {
			toRead = remaining
		}

		if err := g.EnsureChunk(cID, 0, int(dims)); err != nil {
			return nil, err
		}

		levels := g.GetLevelsChunk(cID)
		if _, err := io.ReadFull(r, levels[:toRead]); err != nil {
			return nil, err
		}
		nodesRead += toRead
		cID++
	}

	// 2. Vectors
	if err := g.deserializeVectors(r); err != nil {
		return nil, err
	}

	// 3. Adjacency
	var numLayers uint32
	if err := binary.Read(r, binary.LittleEndian, &numLayers); err != nil {
		return nil, err
	}
	if numLayers > uint32(ArrowMaxLayers) {
		numLayers = uint32(ArrowMaxLayers) // Cap if mismatch? Or error.
	}

	for l := 0; l < int(numLayers); l++ {
		nodesProcessed := 0
		cID := 0
		for nodesProcessed < int(capacity) {
			count := ChunkSize
			if nodesProcessed+count > int(capacity) {
				count = int(capacity) - nodesProcessed
			}

			// Ensure chunk exists
			// Assuming EnsureChunk already called during Levels/Vectors or lazily here
			// NOTE: EnsureChunk(cID) ensures neighbor/counts arrays too.

			countsChunk := g.GetCountsChunk(l, cID)
			neighborsChunk := g.GetNeighborsChunk(l, cID)

			for i := 0; i < count; i++ {
				var nCnt uint32
				if err := binary.Read(r, binary.LittleEndian, &nCnt); err != nil {
					return nil, err
				}

				if nCnt > 0 {
					if nCnt > MaxNeighbors {
						return nil, fmt.Errorf("corrupt neighbor count %d at node %d layer %d", nCnt, nodesProcessed+i, l)
					}

					// Read neighbors
					base := i * MaxNeighbors
					slice := neighborsChunk[base : base+int(nCnt)]
					if err := binary.Read(r, binary.LittleEndian, slice); err != nil {
						return nil, err
					}

					countsChunk[i] = int32(nCnt)
				}
			}
			nodesProcessed += count
			cID++
		}
	}

	return g, nil
}

func (g *GraphData) deserializeVectors(r io.Reader) error {
	switch g.Type {
	case VectorTypeFloat32, VectorTypeUnknown:
		if err := g.readFloat32Vectors(r); err != nil {
			return err
		}
	case VectorTypeFloat16:
		if err := g.readF16Vectors(r); err != nil {
			return err
		}
	}

	if g.SQ8Enabled {
		if err := g.readSQ8Vectors(r); err != nil {
			return err
		}
	}
	if g.BQEnabled {
		if err := g.readBQVectors(r); err != nil {
			return err
		}
	}
	if g.PQEnabled {
		if err := g.readPQVectors(r); err != nil {
			return err
		}
	}
	return nil
}

func (g *GraphData) readSQ8Vectors(r io.Reader) error {
	nodesRead := 0
	cID := 0
	paddedDims := (g.Dims + 63) & ^63
	for nodesRead < g.Capacity {
		remaining := g.Capacity - nodesRead
		toRead := ChunkSize
		if toRead > remaining {
			toRead = remaining
		}

		if err := g.EnsureChunk(cID, 0, g.Dims); err != nil {
			return err
		}
		chunk := g.GetVectorsSQ8Chunk(cID)
		limit := toRead * paddedDims
		if _, err := io.ReadFull(r, chunk[:limit]); err != nil {
			return err
		}

		nodesRead += toRead
		cID++
	}
	return nil
}

func (g *GraphData) readBQVectors(r io.Reader) error {
	nodesRead := 0
	cID := 0
	paddedDims := (g.Dims + 63) & ^63
	numWords := paddedDims / 64
	for nodesRead < g.Capacity {
		remaining := g.Capacity - nodesRead
		toRead := ChunkSize
		if toRead > remaining {
			toRead = remaining
		}

		if err := g.EnsureChunk(cID, 0, g.Dims); err != nil {
			return err
		}
		chunk := g.GetVectorsBQChunk(cID)
		limit := toRead * numWords
		if err := binary.Read(r, binary.LittleEndian, chunk[:limit]); err != nil {
			return err
		}

		nodesRead += toRead
		cID++
	}
	return nil
}

func (g *GraphData) readPQVectors(r io.Reader) error {
	if g.PQM <= 0 {
		return nil
	}
	nodesRead := 0
	cID := 0
	m := g.PQM
	for nodesRead < g.Capacity {
		remaining := g.Capacity - nodesRead
		toRead := ChunkSize
		if toRead > remaining {
			toRead = remaining
		}

		if err := g.EnsureChunk(cID, 0, g.Dims); err != nil {
			return err
		}
		chunk := g.GetVectorsPQChunk(cID)
		limit := toRead * m
		if _, err := io.ReadFull(r, chunk[:limit]); err != nil {
			return err
		}

		nodesRead += toRead
		cID++
	}
	return nil
}

func (g *GraphData) readFloat32Vectors(r io.Reader) error {
	nodesRead := 0
	cID := 0
	for nodesRead < g.Capacity {
		remaining := g.Capacity - nodesRead
		toRead := ChunkSize
		if toRead > remaining {
			toRead = remaining
		}

		chunk := g.GetVectorsChunk(cID)
		limit := toRead * g.Dims
		if err := binary.Read(r, binary.LittleEndian, chunk[:limit]); err != nil {
			return err
		}

		nodesRead += toRead
		cID++
	}
	return nil
}

func (g *GraphData) readF16Vectors(r io.Reader) error {
	nodesRead := 0
	cID := 0
	for nodesRead < g.Capacity {
		remaining := g.Capacity - nodesRead
		toRead := ChunkSize
		if toRead > remaining {
			toRead = remaining
		}

		chunk := g.GetVectorsF16Chunk(cID)
		limit := toRead * g.Dims
		if err := binary.Read(r, binary.LittleEndian, chunk[:limit]); err != nil {
			return err
		}

		nodesRead += toRead
		cID++
	}
	return nil
}
