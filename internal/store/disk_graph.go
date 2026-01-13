package store

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"sort"
	"syscall"
	"unsafe"
)

const (
	DiskGraphMagic   = 0x484E5357 // "HNSW"
	DiskGraphVersion = 4
)

// DiskGraph implements a read-only GraphBackend backed by a file (via mmap).
type DiskGraph struct {
	f      *os.File
	data   []byte // mmap region
	header *DiskGraphHeader

	// Layer 0 is dense: we have an offset for every NodeID up to NumNodes.
	// stored as a slice view into the mmap.
	l0Offsets []uint64

	// Upper layers are sparse. We store them as parsed slices for binary search (or view? binary search on disk view is safer/cheaper)
	// To minimize RAM, we can view the NodeIDs slice directly from mmap.
	// Layers[l] -> { [NodeID, NodeID...], [Offset, Offset...] }
	upperLayers []SparseLayerIndex
}

type SparseLayerIndex struct {
	NodeIDs []uint32 // Sorted
	Offsets []uint64
}

type DiskGraphHeader struct {
	Magic     uint32
	Version   uint32
	NumNodes  uint32
	MaxLayers uint32
	// New Fields for Vectors
	Dims      uint32
	SQ8Offset uint64 // Offset to SQ8 Data start (if > 0)
	PQOffset  uint64 // Offset to PQ Data start (if > 0)
	PQDims    uint32 // Number of bytes per PQ vector (M)

	// SQ8 Quantization Params (Version 3)
	SQ8Min float32
	SQ8Max float32

	// Graph Entry Point (Version 3)
	EntryPoint    uint32
	GraphMaxLevel int32

	// Followed by:
	// - Layer 0 Index Offset (uint64)
	// - Layer 1..N Info Offset (uint64)
}

// NewDiskGraph opens a graph file and maps it into memory.
func NewDiskGraph(path string) (*DiskGraph, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	size := fi.Size()

	if size < 64 { // Minimal header (grown)
		_ = f.Close()
		return nil, fmt.Errorf("file too small")
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("mmap failed: %v", err)
	}

	dg := &DiskGraph{
		f:    f,
		data: data,
	}

	if err := dg.parse(); err != nil {
		_ = dg.Close()
		return nil, err
	}

	return dg, nil
}

func (dg *DiskGraph) parse() error {
	// Header size check: Base was 40, added 2 floats (8 bytes) = 48.
	minHeader := 48
	if len(dg.data) < minHeader {
		return fmt.Errorf("invalid header")
	}

	magic := binary.LittleEndian.Uint32(dg.data[0:4])
	if magic != DiskGraphMagic {
		return fmt.Errorf("invalid magic: %x", magic)
	}
	version := binary.LittleEndian.Uint32(dg.data[4:8])
	if version < 2 || version > DiskGraphVersion {
		return fmt.Errorf("unsupported version: %d", version)
	}

	dg.header = &DiskGraphHeader{
		Magic:     magic,
		Version:   version,
		NumNodes:  binary.LittleEndian.Uint32(dg.data[8:12]),
		MaxLayers: binary.LittleEndian.Uint32(dg.data[12:16]),
		Dims:      binary.LittleEndian.Uint32(dg.data[16:20]),
		SQ8Offset: binary.LittleEndian.Uint64(dg.data[20:28]),
		PQOffset:  binary.LittleEndian.Uint64(dg.data[28:36]),
		PQDims:    binary.LittleEndian.Uint32(dg.data[36:40]),
	}

	metaStart := 40
	if version >= 3 {
		dg.header.SQ8Min = math.Float32frombits(binary.LittleEndian.Uint32(dg.data[40:44]))
		dg.header.SQ8Max = math.Float32frombits(binary.LittleEndian.Uint32(dg.data[44:48]))
		dg.header.EntryPoint = binary.LittleEndian.Uint32(dg.data[48:52])
		dg.header.GraphMaxLevel = int32(binary.LittleEndian.Uint32(dg.data[52:56]))
		metaStart = 56 // 48 + 4 + 4
	}

	// Read Layer Meta offsets
	// Followed by MaxLayers * 8 bytes (offsets to indexes)
	metaEnd := metaStart + int(dg.header.MaxLayers)*8
	if len(dg.data) < metaEnd {
		return fmt.Errorf("truncated layer meta")
	}

	layerIndexOffsets := make([]uint64, dg.header.MaxLayers)
	for i := 0; i < int(dg.header.MaxLayers); i++ {
		off := metaStart + i*8
		layerIndexOffsets[i] = binary.LittleEndian.Uint64(dg.data[off : off+8])
	}

	// Parse L0
	l0Off := layerIndexOffsets[0]
	if l0Off > 0 {
		// L0 Index Format: [Offset0, Offset1, ... OffsetNumNodes-1]
		// Each offset is 8 bytes.
		idxSize := int(dg.header.NumNodes) * 8
		if len(dg.data) < int(l0Off)+idxSize {
			return fmt.Errorf("truncated L0 index")
		}

		// Create slice view
		// Unsafe but standard for mmap views
		ptr := unsafe.Pointer(&dg.data[l0Off])
		// Construct the slice header
		// Go 1.17+ unsafe.Slice is available but we might be on older? Assuming 1.20+
		// dg.l0Offsets = unsafe.Slice((*uint64)(ptr), int(dg.header.NumNodes))
		// Use manual header construction for compatibility if unsure, but unsafe.Slice is cleaner.
		// Let's use manual for broad compat or verify Go version. Assuming modern Go.
		dg.l0Offsets = unsafe.Slice((*uint64)(ptr), int(dg.header.NumNodes))
	}

	// Parse Upper Layers
	dg.upperLayers = make([]SparseLayerIndex, dg.header.MaxLayers)
	for i := 1; i < int(dg.header.MaxLayers); i++ {
		off := layerIndexOffsets[i]
		if off == 0 {
			continue
		}
		// Sparse Index Format:
		// [Count: 4b]
		// [NodeID0, NodeID1...]: Count * 4b
		// [Offset0, Offset1...]: Count * 8b
		if len(dg.data) < int(off)+4 {
			return fmt.Errorf("truncated sparse index header bucket %d", i)
		}
		count := binary.LittleEndian.Uint32(dg.data[off : off+4])

		nodeIDsStart := int(off) + 4
		nodeIDsEnd := nodeIDsStart + int(count)*4
		offsetsStart := nodeIDsEnd
		offsetsEnd := offsetsStart + int(count)*8

		if len(dg.data) < offsetsEnd {
			return fmt.Errorf("truncated sparse index bucket %d", i)
		}

		nodeIDsPtr := unsafe.Pointer(&dg.data[nodeIDsStart])
		offsetsPtr := unsafe.Pointer(&dg.data[offsetsStart])

		dg.upperLayers[i] = SparseLayerIndex{
			NodeIDs: unsafe.Slice((*uint32)(nodeIDsPtr), int(count)),
			Offsets: unsafe.Slice((*uint64)(offsetsPtr), int(count)),
		}
	}

	return nil
}

// GetNeighbors implements GraphBackend.
func (dg *DiskGraph) GetNeighbors(layer int, nodeID uint32, buf []uint32) []uint32 {
	if layer >= int(dg.header.MaxLayers) {
		return nil
	}
	var dataOffset uint64

	// Resolve offset
	if layer == 0 {
		if int(nodeID) >= len(dg.l0Offsets) {
			return nil
		}
		dataOffset = dg.l0Offsets[nodeID]
	} else {
		// Sparse check
		idx := dg.upperLayers[layer]
		// Binary search
		// Since idx.NodeIDs is sorted
		// sort.Search uses closures, might be slightly overhead?
		// Implement manual lower_bound for speed if needed.
		// For now standard sort.Search.
		n := len(idx.NodeIDs)
		i := sort.Search(n, func(j int) bool {
			return idx.NodeIDs[j] >= nodeID
		})
		if i < n && idx.NodeIDs[i] == nodeID {
			dataOffset = idx.Offsets[i]
		} else {
			return nil
		}
	}

	if dataOffset == 0 {
		// Null offset means no neighbors/empty
		return nil
	}

	// Read Data
	if int(dataOffset)+1 > len(dg.data) {
		return nil
	}

	var count uint32
	var start int

	if dg.header.Version >= 4 {
		// Varint Encoding
		// Decode count
		// binary.Uvarint requires a byte slice.
		// We safely slice from dataOffset to end of data?
		// Or limit it? Uvarint stops when it sees a byte < 0x80.
		// Safe to slice until end of data for reading.
		slice := dg.data[dataOffset:]
		c, n := binary.Uvarint(slice)
		if n <= 0 {
			return nil
		}
		count = uint32(c)
		start = int(dataOffset) + n

		// Decode Deltas
		var res []uint32
		if cap(buf) >= int(count) {
			res = buf[:count]
		} else {
			res = make([]uint32, count)
		}

		last := uint32(0)
		offset := start
		for i := 0; i < int(count); i++ {
			if offset >= len(dg.data) {
				return nil
			}
			d, n := binary.Uvarint(dg.data[offset:])
			if n <= 0 {
				return nil
			}
			val := last + uint32(d)
			res[i] = val
			last = val
			offset += n
		}
		return res

	} else {
		// V3: Fixed Layout [Count:4b][N...:4b]
		if int(dataOffset)+4 > len(dg.data) {
			return nil
		}
		count = binary.LittleEndian.Uint32(dg.data[dataOffset : dataOffset+4])
		start = int(dataOffset) + 4
		// Check bounds
		end := start + int(count)*4
		if end > len(dg.data) {
			return nil
		}

		// Copy to buf
		var res []uint32
		if cap(buf) >= int(count) {
			res = buf[:count]
		} else {
			res = make([]uint32, count)
		}

		src := unsafe.Slice((*uint32)(unsafe.Pointer(&dg.data[start])), int(count))
		copy(res, src)
		return res
	}
}

func (dg *DiskGraph) GetVectorSQ8(nodeID uint32) []byte {
	if dg.header.SQ8Offset == 0 || dg.header.Dims == 0 {
		return nil
	}
	// SQ8 storage is dense: contiguous Dims-length vectors
	// Offset = Base + NodeID * Dims
	if uint32(nodeID) >= dg.header.NumNodes {
		return nil
	}

	start := dg.header.SQ8Offset + uint64(nodeID)*uint64(dg.header.Dims)
	end := start + uint64(dg.header.Dims)

	if end > uint64(len(dg.data)) {
		return nil
	}

	// Return slice view directly from mmap
	// Read-only, safe
	return dg.data[start:end]
}

func (dg *DiskGraph) GetVectorPQ(nodeID uint32) []byte {
	if dg.header.PQOffset == 0 || dg.header.PQDims == 0 {
		return nil
	}
	// PQ storage is dense: contiguous PQM-length vectors
	if uint32(nodeID) >= dg.header.NumNodes {
		return nil
	}

	start := dg.header.PQOffset + uint64(nodeID)*uint64(dg.header.PQDims)
	end := start + uint64(dg.header.PQDims)

	if end > uint64(len(dg.data)) {
		return nil
	}

	return dg.data[start:end]
}

func (dg *DiskGraph) GetCapacity() int {
	return int(dg.header.NumNodes)
}

func (dg *DiskGraph) GetLevel(nodeID uint32) int {
	// To find max level, we check from top down?
	// Or we store levels explicitly?
	// GraphBackend requires GetLevel.
	// We didn't store a "Levels" array in our format above.
	// We implicitly have it by presence in Sparse Layers.
	for l := int(dg.header.MaxLayers) - 1; l > 0; l-- {
		idx := dg.upperLayers[l]
		n := len(idx.NodeIDs)
		i := sort.Search(n, func(j int) bool {
			return idx.NodeIDs[j] >= nodeID
		})
		if i < n && idx.NodeIDs[i] == nodeID {
			return l
		}
	}
	return 0 // Every node is at least at layer 0 (if valid)
}

func (dg *DiskGraph) Size() int {
	return int(dg.header.NumNodes)
}

func (dg *DiskGraph) Close() error {
	if dg.data != nil {
		_ = syscall.Munmap(dg.data)
		dg.data = nil
	}
	if dg.f != nil {
		return dg.f.Close()
	}
	return nil
}
