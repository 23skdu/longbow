package store

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestDiskGraph_RoundTrip(t *testing.T) {
	// 1. Create a dummy GraphData
	capacity := 1024
	dims := 4
	gd := NewGraphData(capacity, dims, true, true, 2, false)

	// Populate neighbors
	// We must allocate full chunks for GraphData to work
	neighbors := make([]uint32, ChunkSize*MaxNeighbors)
	counts := make([]int32, ChunkSize)

	// Node 0: [1, 2]
	counts[0] = 2
	neighbors[0*MaxNeighbors] = 1
	neighbors[0*MaxNeighbors+1] = 2

	// Node 1: [3]
	counts[1] = 1
	neighbors[1*MaxNeighbors] = 3

	gd.StoreNeighborsChunk(0, 0, neighbors) // Chunk 0
	gd.StoreCountsChunk(0, 0, counts)

	// Populate SQ8 Vectors
	// Node 0: [10, 10, 10, 10]
	// Node 1: [20, 20, 20, 20]
	sq8 := make([]byte, ChunkSize*dims)
	for i := 0; i < dims; i++ {
		sq8[0*dims+i] = 10
		sq8[1*dims+i] = 20
	}
	gd.StoreSQ8Chunk(0, sq8)

	// Populate PQ Vectors
	// PQ M=2. Node 0: [100, 101], Node 1: [200, 201]
	pqM := 2
	pqVecs := make([]byte, ChunkSize*pqM)
	pqVecs[0*pqM+0] = 100
	pqVecs[0*pqM+1] = 101
	pqVecs[1*pqM+0] = 200
	pqVecs[1*pqM+1] = 201
	gd.StorePQChunk(0, pqVecs)

	// Allocate Versions chunks (Required for GetNeighbors)
	vers := make([]uint32, ChunkSize)
	gd.StoreVersionsChunk(0, 0, vers)

	// L1 Setup
	// Just one node (Node 0 connects to Node 1)
	neighborsL1 := make([]uint32, ChunkSize*MaxNeighbors)
	countsL1 := make([]int32, ChunkSize)

	countsL1[0] = 1
	neighborsL1[0] = 1

	gd.StoreNeighborsChunk(1, 0, neighborsL1)
	gd.StoreCountsChunk(1, 0, countsL1)

	versL1 := make([]uint32, ChunkSize)
	gd.StoreVersionsChunk(1, 0, versL1)

	// 2. Persist to Disk
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "graph.bin")

	// MaxNodeID 5
	if err := WriteDiskGraph(gd, path, 5); err != nil {
		t.Fatalf("WriteDiskGraph failed: %v", err)
	}

	// 3. Load from Disk
	dg, err := NewDiskGraph(path)
	if err != nil {
		t.Fatalf("NewDiskGraph failed: %v", err)
	}
	defer func() { _ = dg.Close() }()

	// 4. Verify
	// Check L0, Node 0
	n0 := dg.GetNeighbors(0, 0, nil)
	if !reflect.DeepEqual(n0, []uint32{1, 2}) {
		t.Errorf("L0 Node 0 mismatch: got %v", n0)
	}

	// Check L0, Node 1
	n1 := dg.GetNeighbors(0, 1, nil)
	if !reflect.DeepEqual(n1, []uint32{3}) {
		t.Errorf("L0 Node 1 mismatch: got %v", n1)
	}

	// Check SQ8 Node 0
	v0 := dg.GetVectorSQ8(0)
	if !reflect.DeepEqual(v0, []byte{10, 10, 10, 10}) {
		t.Errorf("SQ8 Node 0 mismatch: got %v", v0)
	}

	// Check SQ8 Node 1
	v1 := dg.GetVectorSQ8(1)
	if !reflect.DeepEqual(v1, []byte{20, 20, 20, 20}) {
		t.Errorf("SQ8 Node 1 mismatch: got %v", v1)
	}

	// Check SQ8 Node 3 (Empty -> Zeros)
	v3 := dg.GetVectorSQ8(3)
	if !reflect.DeepEqual(v3, []byte{0, 0, 0, 0}) {
		t.Errorf("SQ8 Node 3 mismatch: got %v", v3)
	}

	// Check PQ Node 0
	pq0 := dg.GetVectorPQ(0)
	if !reflect.DeepEqual(pq0, []byte{100, 101}) {
		t.Errorf("PQ Node 0 mismatch: got %v", pq0)
	}

	// Check PQ Node 1
	pq1 := dg.GetVectorPQ(1)
	if !reflect.DeepEqual(pq1, []byte{200, 201}) {
		t.Errorf("PQ Node 1 mismatch: got %v", pq1)
	}

	// Check PQ Node 3 (Empty -> Zeros)
	pq3 := dg.GetVectorPQ(3)
	if !reflect.DeepEqual(pq3, []byte{0, 0}) {
		t.Errorf("PQ Node 3 mismatch: got %v", pq3)
	}
}
