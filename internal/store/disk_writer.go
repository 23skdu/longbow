package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"sort"
)

// WriteDiskGraph serializes the in-memory GraphData to a DiskGraph file.
// Includes Adjacency and SQ8 Compressed vectors.
func WriteDiskGraph(gd *GraphData, path string, maxNodeID int, sqMin, sqMax float32, entryPoint uint32, maxLevel int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	w := bufio.NewWriter(f)

	// 1. Calculate Schema
	// Find actual max level (and max node ID implicitly handled by caller via maxNodeID)
	maxLayer := 0
	for l := ArrowMaxLayers - 1; l >= 0; l-- {
		// check if any node exists at this layer
		hasNode := false
		if l < len(gd.Neighbors) {
			// Iterate efficiently: chunks are allocated sparsely?
			// Actually gd.Neighbors[l] is a slice of chunk offsets.
			for cID := 0; cID < len(gd.Neighbors[l]); cID++ {
				if gd.Neighbors[l][cID] != 0 {
					hasNode = true
					break
				}
			}
		}
		if hasNode {
			maxLayer = l
			break
		}
	}
	fileMaxLayers := uint32(maxLayer + 1)
	if fileMaxLayers == 0 {
		fileMaxLayers = 1 // Minimum 1 layer
	}

	// PQ Dims Check
	pqDims := gd.PQDims

	numNodes := uint32(maxNodeID)

	// 2. Prepare Headers
	// Main Header: 40 bytes (Base) + 8 (SQ8) + 8 (EP/MaxL) = 56 bytes.
	headerBaseSize := 56
	metaSectionSize := int(fileMaxLayers) * 8
	totalHeaderSize := headerBaseSize + metaSectionSize

	placeholder := make([]byte, totalHeaderSize)
	if _, err := w.Write(placeholder); err != nil {
		return err
	}

	currentOffset := int64(totalHeaderSize)
	// ... (skipping unchanged lines)

	// Arrays to track data offsets
	l0Offsets := make([]uint64, numNodes)
	upperLayersMeta := make([]SparseLayerIndex, fileMaxLayers)

	// Buffer for encoding counts
	scratch4 := make([]byte, 4)

	// 3. Write Adjacency Data Layer by Layer
	for l := 0; l < int(fileMaxLayers); l++ {
		var nodeIDs []uint32
		var offsets []uint64

		for id := uint32(0); id < numNodes; id++ {
			// GetNeighbors is safe:
			neighbors := gd.GetNeighbors(l, id, nil) // buffer nil is okay? signature allows.
			if neighbors == nil {
				continue
			}

			// Neighbors exists.
			if l == 0 {
				l0Offsets[id] = uint64(currentOffset)
			} else {
				nodeIDs = append(nodeIDs, id)
				offsets = append(offsets, uint64(currentOffset))
			}

			// Sort neighbors for delta encoding
			// We need a copy to not mutate the original graph if it's being used?
			// GetNeighbors returns a copy usually (slice view from atomic load, but data is in arena).
			// Wait, GetNeighbors (Memory) returns a copy?
			// GraphData.GetNeighbors returns a slice `res` from buffer or made.
			// It copies from neighborsChunk.
			// So `neighbors` here is a safe copy.
			// Sort in place.
			sort.Slice(neighbors, func(i, j int) bool { return neighbors[i] < neighbors[j] })

			// Encode Count (Uvarint)
			count := uint64(len(neighbors))
			// Re-allocate scratch buffer to be safe for up to 10 bytes
			var scratchVarint [10]byte
			n := binary.PutUvarint(scratchVarint[:], count)
			if _, err := w.Write(scratchVarint[:n]); err != nil {
				return err
			}
			currentOffset += int64(n)

			// Encode Neighbors (Delta Varint)
			last := uint32(0)
			for _, nodeID := range neighbors {
				delta := nodeID - last
				n := binary.PutUvarint(scratchVarint[:], uint64(delta))
				if _, err := w.Write(scratchVarint[:n]); err != nil {
					return err
				}
				currentOffset += int64(n)
				last = nodeID
			}
		}

		if l > 0 {
			upperLayersMeta[l] = SparseLayerIndex{
				NodeIDs: nodeIDs,
				Offsets: offsets,
			}
		}
	}

	// 4. Write Indexes
	layerIndexOffsets := make([]uint64, fileMaxLayers)

	// Write L0 Index
	layerIndexOffsets[0] = uint64(currentOffset)
	l0Buf := make([]byte, numNodes*8)
	for i := 0; i < int(numNodes); i++ {
		binary.LittleEndian.PutUint64(l0Buf[i*8:], l0Offsets[i])
	}
	if _, err := w.Write(l0Buf); err != nil {
		return err
	}
	currentOffset += int64(len(l0Buf))

	// Write Upper Layer Indices
	for l := 1; l < int(fileMaxLayers); l++ {
		idx := upperLayersMeta[l]
		if len(idx.NodeIDs) == 0 {
			layerIndexOffsets[l] = 0
			continue
		}
		layerIndexOffsets[l] = uint64(currentOffset)

		count := uint32(len(idx.NodeIDs))

		// Write Count
		binary.LittleEndian.PutUint32(scratch4, count)
		if _, err := w.Write(scratch4); err != nil {
			return err
		}
		currentOffset += 4

		// Write NodeIDs
		idsBuf := make([]byte, count*4)
		for i, id := range idx.NodeIDs {
			binary.LittleEndian.PutUint32(idsBuf[i*4:], id)
		}
		if _, err := w.Write(idsBuf); err != nil {
			return err
		}
		currentOffset += int64(len(idsBuf))

		// Write Offsets
		offsBuf := make([]byte, count*8)
		for i, off := range idx.Offsets {
			binary.LittleEndian.PutUint64(offsBuf[i*8:], off)
		}
		if _, err := w.Write(offsBuf); err != nil {
			return err
		}
		currentOffset += int64(len(offsBuf))
	}

	// 5. Write SQ8 Vectors
	sq8Offset := uint64(0)
	if gd.Dims > 0 {
		sq8Offset = uint64(currentOffset)
		dims := gd.Dims
		zeros := make([]byte, dims)

		for id := uint32(0); id < numNodes; id++ {
			// GetVectorSQ8 helper
			// Note: GetVectorSQ8 returns []byte copy or slice?
			// Refactor: GetVectorSQ8 in arrow_hnsw_graph.go returns []byte (copy).
			// Efficient enough for disk writing.
			vec := gd.GetVectorSQ8(id)
			if vec == nil {
				if _, err := w.Write(zeros); err != nil {
					return err
				}
			} else {
				if len(vec) != dims {
					return fmt.Errorf("vector dim mismatch at %d: got %d want %d", id, len(vec), dims)
				}
				if _, err := w.Write(vec); err != nil {
					return err
				}
			}
			currentOffset += int64(dims)
		}
	}

	// 5b. Write PQ Vectors
	pqOffset := uint64(0)
	if pqDims > 0 {
		pqOffset = uint64(currentOffset)
		// PQ storage is dense (NumNodes * PQDims)
		zeros := make([]byte, pqDims)

		for id := uint32(0); id < numNodes; id++ {
			// New Helper: GetVectorPQ(id, pqDims)
			vec := gd.GetVectorPQ(id)
			if vec == nil {
				if _, err := w.Write(zeros); err != nil {
					return err
				}
			} else {
				if len(vec) != pqDims {
					return fmt.Errorf("pq vector dim mismatch at %d: got %d want %d", id, len(vec), pqDims)
				}
				if _, err := w.Write(vec); err != nil {
					return err
				}
			}
			currentOffset += int64(pqDims)
		}
	}

	// 6. Finalize Header
	if err := w.Flush(); err != nil {
		return err
	}
	// Seek to beginning
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}

	// Re-write header
	headerBuf := make([]byte, totalHeaderSize)
	binary.LittleEndian.PutUint32(headerBuf[0:], DiskGraphMagic)
	binary.LittleEndian.PutUint32(headerBuf[4:], DiskGraphVersion)
	binary.LittleEndian.PutUint32(headerBuf[8:], numNodes)
	binary.LittleEndian.PutUint32(headerBuf[12:], fileMaxLayers)
	// Dims
	binary.LittleEndian.PutUint32(headerBuf[16:], uint32(gd.Dims))
	// SQ8 Offset
	binary.LittleEndian.PutUint64(headerBuf[20:], sq8Offset)
	// PQ Offset
	binary.LittleEndian.PutUint64(headerBuf[28:], pqOffset)
	// PQ Dims
	binary.LittleEndian.PutUint32(headerBuf[36:], uint32(pqDims))

	// SQ8 Params (Version 3)
	binary.LittleEndian.PutUint32(headerBuf[40:], math.Float32bits(sqMin))
	binary.LittleEndian.PutUint32(headerBuf[44:], math.Float32bits(sqMax))

	// Entry Point & Max Level (Version 3)
	binary.LittleEndian.PutUint32(headerBuf[48:], entryPoint)
	binary.LittleEndian.PutUint32(headerBuf[52:], uint32(int32(maxLevel)))

	// Offsets
	for i := 0; i < int(fileMaxLayers); i++ {
		binary.LittleEndian.PutUint64(headerBuf[56+i*8:], layerIndexOffsets[i])
	}

	if _, err := f.Write(headerBuf); err != nil {
		return err
	}

	return f.Close()
}
