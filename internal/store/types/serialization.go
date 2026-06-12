package types

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"
	"unsafe"
)

func writeUint32(w io.Writer, v uint32) error {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	_, err := w.Write(buf[:])
	return err
}

func writeInt32(w io.Writer, v int32) error {
	return writeUint32(w, uint32(v)) // #nosec G115
}

func writeInt64(w io.Writer, v int64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(v)) // #nosec G115
	_, err := w.Write(buf[:])
	return err
}

func writeUint32Slice(w io.Writer, v []uint32) error {
	if len(v) == 0 {
		return nil
	}
	buf := make([]byte, len(v)*4)
	for i, u := range v {
		binary.LittleEndian.PutUint32(buf[i*4:], u)
	}
	_, err := w.Write(buf)
	return err
}

func writeUint8(w io.Writer, v uint8) error {
	_, err := w.Write([]byte{v})
	return err
}

func writeFloat32Slice(w io.Writer, v []float32) error {
	if len(v) == 0 {
		return nil
	}
	data := unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), len(v)*4) // #nosec G103
	_, err := w.Write(data)
	return err
}

func writeUint64Slice(w io.Writer, v []uint64) error {
	if len(v) == 0 {
		return nil
	}
	data := unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), len(v)*8) // #nosec G103
	_, err := w.Write(data)
	return err
}

func writeUint16Slice(w io.Writer, v []uint16) error {
	if len(v) == 0 {
		return nil
	}
	data := unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), len(v)*2) // #nosec G103
	_, err := w.Write(data)
	return err
}

func writeZeros(w io.Writer, n int) error {
	const maxBlock = 64 * 1024
	for n > 0 {
		sz := n
		if sz > maxBlock {
			sz = maxBlock
		}
		if _, err := w.Write(make([]byte, sz)); err != nil {
			return err
		}
		n -= sz
	}
	return nil
}

func writeInt16Slice(w io.Writer, v []int16) error {
	if len(v) == 0 {
		return nil
	}
	data := unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), len(v)*2) // #nosec G103
	_, err := w.Write(data)
	return err
}

func readUint32(r io.Reader) (uint32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf[:]), nil
}

func readInt32(r io.Reader) (int32, error) {
	v, err := readUint32(r)
	return int32(v), err // #nosec G115
}

func readInt64(r io.Reader) (int64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(buf[:])), nil // #nosec G115
}

func readUint8(r io.Reader) (uint8, error) {
	var buf [1]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return buf[0], nil
}

const (
	GraphSnapshotVersion = 1
	SerializationMagic   = 0x4C424752 // "LBGR" (LongBow GRaph)
)

// Serialize writes the GraphData to the writer in a portable binary format.
func (g *GraphData) Serialize(w io.Writer) error {
	// Header
	if err := writeUint32(w, SerializationMagic); err != nil {
		return err
	}
	if err := writeUint32(w, GraphSnapshotVersion); err != nil {
		return err
	}
	if err := writeInt64(w, int64(g.Capacity)); err != nil {
		return err
	}
	if err := writeInt32(w, int32(g.Dims)); err != nil { // #nosec G115
		return err
	}
	if err := writeUint8(w, uint8(g.Type)); err != nil { // #nosec G115
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
	if g.TurboQuantEnabled {
		flags |= 1 << 3
	}
	// Bits for TurboQuant: 4-7
	flags |= uint32(g.TurboQuantBits&0xF) << 4
	if err := writeUint32(w, flags); err != nil {
		return err
	}
	if err := writeInt32(w, int32(g.PQM)); err != nil { // #nosec G115
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
			temp := make([]uint8, toWrite)
			for i := 0; i < toWrite; i++ {
				temp[i] = uint8(atomic.LoadUint32(&chunk[i])) // #nosec G115
			}
			if _, err := w.Write(temp); err != nil {
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
	if err := writeUint32(w, uint32(ArrowMaxLayers)); err != nil {
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

			for i := 0; i < count; i++ {
				nodeID := uint32(nodesProcessed + i)
				var encodedCount uint32
				var neighborsList []uint32

				// Prefer PackedNeighbors for serialization
				if l < len(g.PackedNeighbors) && g.PackedNeighbors[l] != nil {
					if nbs, ok := g.PackedNeighbors[l].GetNeighbors(nodeID); ok {
						neighborsList = nbs
						encodedCount = uint32(len(nbs)) // #nosec G115
					}
				}

				// Fallback to legacy chunk
				if neighborsList == nil {
					counts := g.GetCountsChunk(l, cID)
					if counts != nil {
						c := counts[i]
						if c >= 0 && c <= MaxNeighbors {
							encodedCount = uint32(c)
						}
					}
					if encodedCount > 0 {
						chunk := g.GetNeighborsChunk(l, cID)
						if chunk != nil {
							base := i * MaxNeighbors
							if base+int(encodedCount) <= len(chunk) {
								neighborsList = chunk[base : base+int(encodedCount)]
							}
						}
					}
				}

				if err := writeUint32(w, encodedCount); err != nil {
					return err
				}

				if encodedCount > 0 {
					if neighborsList != nil {
						if err := writeUint32Slice(w, neighborsList); err != nil {
							return err
						}
					} else {
						if err := writeZeros(w, int(encodedCount)*4); err != nil {
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
	case VectorTypeInt16:
		if err := g.writeInt16Vectors(w); err != nil {
			return err
		}
	case VectorTypeUint16:
		if err := g.writeUint16Vectors(w); err != nil {
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
			if err := writeUint64Slice(w, zeros); err != nil {
				return err
			}
		} else {
			limit := toWriteNodes * numWords
			if err := writeUint64Slice(w, chunk[:limit]); err != nil {
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
	for chunkID := 0; nodesWritten < g.Capacity; chunkID++ {
		remaining := g.Capacity - nodesWritten
		if remaining <= 0 {
			break
		}

		toWriteNodes := ChunkSize
		if toWriteNodes > remaining {
			toWriteNodes = remaining
		}

		chunk := g.GetVectorsChunk(chunkID)
		if chunk == nil {
			zeros := make([]float32, toWriteNodes*g.Dims)
			if err := writeFloat32Slice(w, zeros); err != nil {
				return err
			}
		} else {
			limit := toWriteNodes * g.Dims
			if len(chunk) < limit {
				limit = len(chunk)
			}
			if err := writeFloat32Slice(w, chunk[:limit]); err != nil {
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
			if err := writeUint16Slice(w, zeros); err != nil {
				return err
			}
		} else {
			paddedDims := g.GetPaddedDimsForType(VectorTypeFloat16)
			u16Chunk := unsafe.Slice((*uint16)(unsafe.Pointer(&chunk[0])), len(chunk)) // #nosec G103
			for j := 0; j < toWriteNodes; j++ {
				start := j * paddedDims
				if err := writeUint16Slice(w, u16Chunk[start:start+g.Dims]); err != nil {
					return err
				}
			}
		}
		nodesWritten += toWriteNodes
	}
	return nil
}

func (g *GraphData) writeInt16Vectors(w io.Writer) error {
	nodesWritten := 0
	for i := 0; nodesWritten < g.Capacity; i++ {
		remaining := g.Capacity - nodesWritten
		toWriteNodes := ChunkSize
		if toWriteNodes > remaining {
			toWriteNodes = remaining
		}

		chunk := g.GetVectorsInt16Chunk(i)
		if chunk == nil {
			zeros := make([]int16, toWriteNodes*g.Dims)
			if err := writeInt16Slice(w, zeros); err != nil {
				return err
			}
		} else {
			paddedDims := g.GetPaddedDimsForType(VectorTypeInt16)
			for j := 0; j < toWriteNodes; j++ {
				start := j * paddedDims
				if err := writeInt16Slice(w, chunk[start:start+g.Dims]); err != nil {
					return err
				}
			}
		}
		nodesWritten += toWriteNodes
	}
	return nil
}

func (g *GraphData) writeUint16Vectors(w io.Writer) error {
	nodesWritten := 0
	for i := 0; nodesWritten < g.Capacity; i++ {
		remaining := g.Capacity - nodesWritten
		toWriteNodes := ChunkSize
		if toWriteNodes > remaining {
			toWriteNodes = remaining
		}

		chunk := g.GetVectorsUint16Chunk(i)
		if chunk == nil {
			zeros := make([]uint16, toWriteNodes*g.Dims)
			if err := writeUint16Slice(w, zeros); err != nil {
				return err
			}
		} else {
			paddedDims := g.GetPaddedDimsForType(VectorTypeUint16)
			for j := 0; j < toWriteNodes; j++ {
				start := j * paddedDims
				if err := writeUint16Slice(w, chunk[start:start+g.Dims]); err != nil {
					return err
				}
			}
		}
		nodesWritten += toWriteNodes
	}
	return nil
}

// Deserialize reads GraphData from the reader.
func DeserializeGraphData(r io.Reader) (*GraphData, error) {
	magic, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	if magic != SerializationMagic {
		return nil, fmt.Errorf("invalid magic: 0x%x", magic)
	}

	version, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	if version != GraphSnapshotVersion {
		return nil, fmt.Errorf("unsupported version: %d", version)
	}

	capacity, err := readInt64(r)
	if err != nil {
		return nil, err
	}

	dims, err := readInt32(r)
	if err != nil {
		return nil, err
	}

	typeCode, err := readUint8(r)
	if err != nil {
		return nil, err
	}

	flags, err := readUint32(r)
	if err != nil {
		return nil, err
	}

	sq8 := (flags & (1 << 0)) != 0
	bq := (flags & (1 << 1)) != 0
	pqEnabled := (flags & (1 << 2)) != 0
	tqEnabled := (flags & (1 << 3)) != 0
	tqBits := int((flags >> 4) & 0xF)
	if tqBits == 0 && tqEnabled {
		tqBits = 8 // Default
	}

	pqM, err := readInt32(r)
	if err != nil {
		return nil, err
	}

	// Initialize GraphData
	g := NewGraphData(int(capacity), int(dims), false, false, 0, false, sq8, false, VectorDataType(typeCode), bq, pqEnabled, tqEnabled, tqBits, "test", nil, false)
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
		temp := make([]uint8, toRead)
		if _, err := io.ReadFull(r, temp); err != nil {
			return nil, err
		}
		for i := 0; i < toRead; i++ {
			levels[i] = uint32(temp[i])
		}
		nodesRead += toRead
		cID++
	}

	// 2. Vectors
	if err := g.deserializeVectors(r); err != nil {
		return nil, err
	}

	// 3. Adjacency
	numLayers, err := readUint32(r)
	if err != nil {
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

			countsChunk := g.GetCountsChunk(l, cID)
			neighborsChunk := g.GetNeighborsChunk(l, cID)

			// For upper layers, neighbor data is stored in PackedNeighbors, not
			// gd.Neighbors arena chunks. If neighborsChunk is nil, fall back to
			// reading directly into PackedNeighbors.
			neighborsFallback := neighborsChunk == nil

			for i := 0; i < count; i++ {
				nCnt, err := readUint32(r)
				if err != nil {
					return nil, err
				}

				if nCnt > 0 {
					if nCnt > MaxNeighbors {
						return nil, fmt.Errorf("corrupt neighbor count %d at node %d layer %d", nCnt, nodesProcessed+i, l)
					}

					if neighborsFallback {
						// Allocate a temporary buffer for this node's neighbors
						buf := make([]uint32, nCnt)
						if err := binary.Read(r, binary.LittleEndian, buf); err != nil {
							return nil, err
						}
						if l < len(g.PackedNeighbors) && g.PackedNeighbors[l] != nil {
							_ = g.PackedNeighbors[l].SetNeighbors(uint32(nodesProcessed+i), buf)
						}
					} else {
						base := i * MaxNeighbors
						slice := neighborsChunk[base : base+int(nCnt)]
						if err := binary.Read(r, binary.LittleEndian, slice); err != nil {
							return nil, err
						}
						if l < len(g.PackedNeighbors) && g.PackedNeighbors[l] != nil {
							_ = g.PackedNeighbors[l].SetNeighbors(uint32(nodesProcessed+i), slice)
						}
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
	case VectorTypeInt16:
		if err := g.readInt16Vectors(r); err != nil {
			return err
		}
	case VectorTypeUint16:
		if err := g.readUint16Vectors(r); err != nil {
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

		if err := g.EnsureChunk(cID, 0, g.Dims); err != nil {
			return err
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

		if err := g.EnsureChunk(cID, 0, g.Dims); err != nil {
			return err
		}
		chunk := g.GetVectorsF16Chunk(cID)
		paddedDims := g.GetPaddedDimsForType(VectorTypeFloat16)

		// Use unsafe to treat float16.Num as uint16 for binary.Read to avoid reflection panics
		// because float16.Num may have unexported fields.
		u16Chunk := unsafe.Slice((*uint16)(unsafe.Pointer(&chunk[0])), len(chunk)) // #nosec G103

		for j := 0; j < toRead; j++ {
			start := j * paddedDims
			if err := binary.Read(r, binary.LittleEndian, u16Chunk[start:start+g.Dims]); err != nil {
				return err
			}
		}

		nodesRead += toRead
		cID++
	}
	return nil
}

func (g *GraphData) readInt16Vectors(r io.Reader) error {
	nodesRead := 0
	cID := 0
	for nodesRead < g.Capacity {
		remaining := g.Capacity - nodesRead
		toRead := ChunkSize
		if toRead > remaining {
			toRead = remaining
		}

		if err := g.EnsureChunk(cID, 0, g.Dims); err != nil {
			return err
		}
		chunk := g.GetVectorsInt16Chunk(cID)
		paddedDims := g.GetPaddedDimsForType(VectorTypeInt16)
		for j := 0; j < toRead; j++ {
			start := j * paddedDims
			if err := binary.Read(r, binary.LittleEndian, chunk[start:start+g.Dims]); err != nil {
				return err
			}
		}

		nodesRead += toRead
		cID++
	}
	return nil
}

func (g *GraphData) readUint16Vectors(r io.Reader) error {
	nodesRead := 0
	cID := 0
	for nodesRead < g.Capacity {
		remaining := g.Capacity - nodesRead
		toRead := ChunkSize
		if toRead > remaining {
			toRead = remaining
		}

		if err := g.EnsureChunk(cID, 0, g.Dims); err != nil {
			return err
		}
		chunk := g.GetVectorsUint16Chunk(cID)
		paddedDims := g.GetPaddedDimsForType(VectorTypeUint16)
		for j := 0; j < toRead; j++ {
			start := j * paddedDims
			if err := binary.Read(r, binary.LittleEndian, chunk[start:start+g.Dims]); err != nil {
				return err
			}
		}

		nodesRead += toRead
		cID++
	}
	return nil
}
