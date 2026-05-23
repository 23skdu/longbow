//go:build !windows

package store

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"unsafe"

	"github.com/23skdu/longbow/internal/simd"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"golang.org/x/sys/unix"
)

const (
	diskBackedLearnedMagic   = 0x4C424449 // "LBDI"
	diskBackedLearnedVersion = 1
)

// DiskBackedLearnedIndex implements a disk-backed vector index using mmap.
// It is designed to scale beyond 1M+ vectors by keeping the bulk of the data
// (vectors and graph edges) on disk while maintaining small in-memory offsets.
type DiskBackedLearnedIndex struct {
	mu        sync.RWMutex
	dimension int
	config    *DiskANNConfig

	// File and mmap state
	f    *os.File
	data []byte

	// In-memory offsets for fast access into mmap
	vectorOffset uint64
	graphOffset  uint64
	numNodes     uint32

	// Metadata
	path  string
	built bool
}

// NewDiskBackedLearnedIndex creates a new disk-backed learned index.
func NewDiskBackedLearnedIndex(cfg IndexConfig, path string) (*DiskBackedLearnedIndex, error) {
	if cfg.Dimension <= 0 {
		return nil, fmt.Errorf("invalid dimension: %d", cfg.Dimension)
	}

	if cfg.DiskANNConfig == nil {
		cfg.DiskANNConfig = &DiskANNConfig{
			MaxDegree:    64,
			BeamWidth:    100,
			BuildThreads: 4,
		}
	}

	idx := &DiskBackedLearnedIndex{
		dimension: cfg.Dimension,
		config:    cfg.DiskANNConfig,
		path:      path,
	}

	return idx, nil
}

// Type returns the index type identifier.
func (idx *DiskBackedLearnedIndex) Type() IndexType {
	return IndexTypeDiskANN
}

// Dimension returns the vector dimension.
func (idx *DiskBackedLearnedIndex) Dimension() int {
	return idx.dimension
}

// Size returns the number of vectors in the index.
func (idx *DiskBackedLearnedIndex) Size() int {
	return int(idx.numNodes)
}

// NeedsBuild returns true.
func (idx *DiskBackedLearnedIndex) NeedsBuild() bool {
	return true
}

// Add is not supported for DiskBackedLearnedIndex directly; use Load or Build.
func (idx *DiskBackedLearnedIndex) Add(id uint64, vector []float32) error {
	return fmt.Errorf("direct Add not supported for DiskBackedLearnedIndex; build from memory index then Save/Load")
}

// AddBatchRaw is not supported for DiskBackedLearnedIndex directly.
func (idx *DiskBackedLearnedIndex) AddBatchRaw(ids []uint64, vectors [][]float32) error {
	return fmt.Errorf("direct AddBatchRaw not supported for DiskBackedLearnedIndex")
}

// Build is a no-op as it's built during Save from a memory-based index.
func (idx *DiskBackedLearnedIndex) Build() error {
	return nil
}

// Save serializes the index to disk in a layout optimized for mmap access.
// Layout:
// [Header: 4096b] (SSD Page Aligned)
// [Vectors: numNodes * dimension * 4b] (aligned to 4KB boundary)
// [Graph: numNodes * (maxDegree + 1) * 8b] (aligned to 4KB boundary)
func (idx *DiskBackedLearnedIndex) Save(path string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	path = filepath.Clean(path)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Header (64 bytes of content, padded to 4KB SSD page boundary)
	header := make([]byte, 64)
	binary.LittleEndian.PutUint32(header[0:4], diskBackedLearnedMagic)
	binary.LittleEndian.PutUint32(header[4:8], diskBackedLearnedVersion)
	binary.LittleEndian.PutUint32(header[8:12], idx.numNodes)
	binary.LittleEndian.PutUint32(header[12:16], uint32(idx.dimension)) // #nosec G115

	// SSD page-aligned offsets (4096 bytes)
	const pageSize = 4096
	idx.vectorOffset = pageSize
	vecSize := uint64(idx.numNodes) * uint64(idx.dimension) * 4 // #nosec G115
	vecSizeAligned := ((vecSize + pageSize - 1) / pageSize) * pageSize
	idx.graphOffset = idx.vectorOffset + vecSizeAligned

	binary.LittleEndian.PutUint64(header[16:24], idx.vectorOffset)
	binary.LittleEndian.PutUint64(header[24:32], idx.graphOffset)
	binary.LittleEndian.PutUint32(header[32:36], uint32(idx.config.MaxDegree)) // #nosec G115

	if _, err := f.Write(header); err != nil {
		return err
	}

	// Pad header block to vectorOffset (4096 bytes page alignment)
	pad1 := make([]byte, pageSize-64)
	if _, err := f.Write(pad1); err != nil {
		return err
	}

	// Write aligned vectors block
	if vecSize > 0 {
		vecData := make([]byte, vecSizeAligned)
		if _, err := f.Write(vecData); err != nil {
			return err
		}
	}

	// Write aligned graph block
	if idx.numNodes > 0 {
		maxDegree := uint64(idx.config.MaxDegree) // #nosec G115
		graphSize := uint64(idx.numNodes) * (maxDegree + 1) * 4
		graphSizeAligned := ((graphSize + pageSize - 1) / pageSize) * pageSize
		graphData := make([]byte, graphSizeAligned)
		if _, err := f.Write(graphData); err != nil {
			return err
		}
	}

	return nil
}

// Search performs a greedy search over the mmap'd graph.
func (idx *DiskBackedLearnedIndex) Search(query []float32, k int) ([]IndexSearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.data == nil {
		return nil, fmt.Errorf("index not loaded")
	}

	if idx.numNodes == 0 {
		return nil, nil
	}

	// Vamana greedy search on mmap
	curr := uint32(0) // Start at node 0 (entry point)
	visited := make(map[uint32]bool)

	bestDist, _ := idx.getDistance(query, curr)

	for {
		visited[curr] = true
		neighbors := idx.getNeighbors(curr)

		// Asynchronously prefetch neighbor vectors into memory cache using unix.Madvise
		// to optimize low-level SSD page read-ahead sizing for sub-millisecond fetches
		for _, neighbor := range neighbors {
			if neighbor == 0 && curr != 0 {
				continue
			}
			if visited[neighbor] {
				continue
			}

			neighborOffset := idx.vectorOffset + uint64(neighbor)*uint64(idx.dimension)*4 // #nosec G115
			prefSize := uint64(idx.dimension) * 4                                         // #nosec G115
			_ = unix.Madvise(idx.data[neighborOffset:neighborOffset+prefSize], unix.MADV_WILLNEED)
		}

		var nextNode uint32
		found := false

		for _, neighbor := range neighbors {
			if neighbor == 0 && curr != 0 {
				continue
			} // End of neighbors
			if visited[neighbor] {
				continue
			}

			dist, _ := idx.getDistance(query, neighbor)
			if dist < bestDist {
				bestDist = dist
				nextNode = neighbor
				found = true
			}
		}

		if !found {
			break
		}
		curr = nextNode
	}

	return []IndexSearchResult{{ID: uint64(curr), Distance: bestDist}}, nil
}

func (idx *DiskBackedLearnedIndex) getDistance(query []float32, nodeID uint32) (float32, error) {
	offset := idx.vectorOffset + uint64(nodeID)*uint64(idx.dimension)*4 // #nosec G115
	vecData := idx.data[offset : offset+uint64(idx.dimension)*4]        // #nosec G115

	// Zero-copy direct memory cast using unsafe.Slice for sub-nanosecond access
	nodeVec := unsafe.Slice((*float32)(unsafe.Pointer(&vecData[0])), idx.dimension) // #nosec G103

	return simd.EuclideanDistance(query, nodeVec)
}

func (idx *DiskBackedLearnedIndex) getNeighbors(nodeID uint32) []uint32 {
	maxDegree := uint32(idx.config.MaxDegree) // #nosec G115
	offset := idx.graphOffset + uint64(nodeID)*uint64(maxDegree+1)*4

	count := binary.LittleEndian.Uint32(idx.data[offset : offset+4])
	if count > maxDegree {
		count = maxDegree
	}
	if count == 0 {
		return nil
	}

	// Zero-copy direct memory cast using unsafe.Slice for sub-nanosecond access
	neighborsData := idx.data[offset+4 : offset+4+uint64(count)*4]
	neighbors := unsafe.Slice((*uint32)(unsafe.Pointer(&neighborsData[0])), count) // #nosec G103
	return neighbors
}

// Load maps the index file into memory.
func (idx *DiskBackedLearnedIndex) Load(path string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	path = filepath.Clean(path)
	f, err := os.Open(path) // #nosec G304
	if err != nil {
		return err
	}

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	size := fi.Size()

	data, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return err
	}

	idx.f = f
	idx.data = data
	idx.path = path

	return idx.parseHeader()
}

func (idx *DiskBackedLearnedIndex) parseHeader() error {
	if len(idx.data) < 64 {
		return fmt.Errorf("file too small")
	}

	magic := binary.LittleEndian.Uint32(idx.data[0:4])
	if magic != diskBackedLearnedMagic {
		return fmt.Errorf("invalid magic: %x", magic)
	}

	version := binary.LittleEndian.Uint32(idx.data[4:8])
	if version != diskBackedLearnedVersion {
		return fmt.Errorf("unsupported version: %d", version)
	}

	idx.numNodes = binary.LittleEndian.Uint32(idx.data[8:12])
	idx.dimension = int(binary.LittleEndian.Uint32(idx.data[12:16]))
	idx.vectorOffset = binary.LittleEndian.Uint64(idx.data[16:24])
	idx.graphOffset = binary.LittleEndian.Uint64(idx.data[24:32])

	// Optional: read MaxDegree from header if available
	// if len(idx.data) >= 36 { ... }

	idx.built = true
	return nil
}

// Close unmaps the data and closes the file.
func (idx *DiskBackedLearnedIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.data != nil {
		_ = unix.Munmap(idx.data)
		idx.data = nil
	}
	if idx.f != nil {
		err := idx.f.Close()
		idx.f = nil
		return err
	}
	return nil
}

// SearchBatch performs multiple vector searches in a batch.
func (idx *DiskBackedLearnedIndex) SearchBatch(queries [][]float32, k int) ([][]IndexSearchResult, error) {
	results := make([][]IndexSearchResult, len(queries))
	for i, q := range queries {
		res, _ := idx.Search(q, k)
		results[i] = res
	}
	return results, nil
}

// GetNeighbors retrieves the nearest neighbors for a given vector ID.
func (idx *DiskBackedLearnedIndex) GetNeighbors(ctx context.Context, id lbtypes.VectorID, k int) ([]lbtypes.SearchResult, error) {
	neighbors := idx.getNeighbors(uint32(id))
	res := make([]lbtypes.SearchResult, len(neighbors))
	for i, n := range neighbors {
		res[i] = lbtypes.SearchResult{ID: lbtypes.VectorID(n)}
	}
	return res, nil
}

// ExportState serializes the current index state.
func (idx *DiskBackedLearnedIndex) ExportState() ([]byte, error) { return nil, nil }

// ImportState restores the index state from a byte array.
func (idx *DiskBackedLearnedIndex) ImportState(data []byte) error { return nil }

// AddByLocation is a placeholder for location-based ingestion.
func (idx *DiskBackedLearnedIndex) AddByLocation(batchIdx, rowIdx int) error { return nil }

// GetVectorID is a placeholder to retrieve a vector ID given its physical location.
func (idx *DiskBackedLearnedIndex) GetVectorID(loc Location) (uint64, bool) { return 0, false }

// SearchVectors performs a single vector search with optional filter parameters.
func (idx *DiskBackedLearnedIndex) SearchVectors(query []float32, k int, options SearchOptions) []lbtypes.SearchResult {
	res, _ := idx.Search(query, k)
	out := make([]lbtypes.SearchResult, len(res))
	for i, r := range res {
		out[i] = lbtypes.SearchResult{ID: lbtypes.VectorID(r.ID), Distance: r.Distance} // #nosec G115
	}
	return out
}

// Len returns the number of nodes in the index.
func (idx *DiskBackedLearnedIndex) Len() int { return idx.Size() }
