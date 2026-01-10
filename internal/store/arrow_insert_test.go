package store

import (
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestLevelGenerator(t *testing.T) {
	lg := NewLevelGenerator(1.44269504089) // 1/ln(2)

	// Generate 1000 levels and check distribution
	levels := make(map[int]int)
	for i := 0; i < 1000; i++ {
		level := lg.Generate()
		levels[level]++

		if level < 0 || level >= ArrowMaxLayers {
			t.Errorf("level %d out of bounds [0, %d)", level, ArrowMaxLayers)
		}
	}

	// Most should be at level 0 (exponential decay)
	if levels[0] < 400 {
		t.Errorf("expected >400 at level 0, got %d", levels[0])
	}

	t.Logf("Level distribution: %v", levels)
}

func TestInsert_SingleNode(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	listB := b.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)

	// Append [1.0, 2.0]
	listB.Append(true)
	valB.AppendValues([]float32{1.0, 2.0}, nil)

	rec := b.NewRecordBatch()
	defer rec.Release()

	dataset := &Dataset{
		Schema:  schema,
		Records: []arrow.RecordBatch{rec},
	}

	config := DefaultArrowHNSWConfig()
	index := NewArrowHNSW(dataset, config, nil)

	// Insert ID 0 -> Batch 0, Row 0
	id, err := index.AddByLocation(0, 0)
	if err != nil {
		t.Fatalf("AddByLocation failed: %v", err)
	}

	if id != 0 {
		t.Errorf("expected ID 0, got %d", id)
	}

	if index.Size() != 1 {
		t.Errorf("expected size 1, got %d", index.Size())
	}
}

func TestInsert_MultipleNodes(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	listB := b.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)

	// Append 10 vectors
	for i := 0; i < 10; i++ {
		listB.Append(true)
		valB.AppendValues([]float32{float32(i), float32(i)}, nil)
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	dataset := &Dataset{
		Schema:  schema,
		Records: []arrow.RecordBatch{rec},
	}

	config := DefaultArrowHNSWConfig()
	index := NewArrowHNSW(dataset, config, nil)

	// Insert 10 vectors
	for i := 0; i < 10; i++ {
		_, err := index.AddByLocation(0, i)
		if err != nil {
			t.Fatalf("AddByLocation(%d) failed: %v", i, err)
		}
	}

	if index.Size() != 10 {
		t.Errorf("expected size 10, got %d", index.Size())
	}
}

func TestAddConnection(t *testing.T) {
	dataset := &Dataset{Name: "test"}
	config := DefaultArrowHNSWConfig()
	index := NewArrowHNSW(dataset, config, nil)

	// Initialize GraphData manually
	data := NewGraphData(0, 64, false, false, 0, false, false)
	index.data.Store(data)

	// Allocate chunks
	// Allocate chunks
	// Manually ensure chunks for testing
	numChunks := (64 + ChunkSize - 1) / ChunkSize
	for i := 0; i < numChunks; i++ {
		data = index.ensureChunk(data, uint32(i), 0, 64) // dim not critical here?
	}

	// Must have search context for pruning
	ctx := index.searchPool.Get().(*ArrowSearchContext)
	defer index.searchPool.Put(ctx)

	// Add connection 0 -> 1 at layer 0
	index.AddConnection(ctx, data, 0, 1, 0, 10)

	// Check count
	cID := chunkID(0)
	cOff := chunkOffset(0)
	counts := data.GetCountsChunk(0, cID)
	count := int32(0)
	if counts != nil {
		count = atomic.LoadInt32(&counts[cOff])
	}
	if count != 1 {
		t.Errorf("expected 1 neighbor, got %d", count)
	}

	// Check neighbor
	neighbors := data.GetNeighborsChunk(0, 0)
	if len(neighbors) > 0 && neighbors[0] != 1 {
		t.Errorf("expected neighbor 1, got %d", neighbors[0])
	}

	// Adding same connection again should be idempotent
	index.AddConnection(ctx, data, 0, 1, 0, 10)

	if counts != nil {
		count = atomic.LoadInt32(&counts[cOff])
	}
	if count != 1 {
		t.Errorf("expected 1 neighbor after duplicate add, got %d", count)
	}
}

func TestPruneConnections(t *testing.T) {
	dataset := &Dataset{Name: "test"}
	config := DefaultArrowHNSWConfig()
	// Strict alpha to force pruning based on distance
	config.Alpha = 1.0
	index := NewArrowHNSW(dataset, config, nil)

	// Initialize GraphData manually
	data := NewGraphData(20, 11, false, false, 0, false, false)
	index.data.Store(data)

	// Allocate chunks
	// Allocate chunks
	numChunks := (20 + ChunkSize - 1) / ChunkSize
	for i := 0; i < numChunks; i++ {
		data = index.ensureChunk(data, uint32(i), 0, 11)
	}

	// Setup vectors for distance calculation
	// Node 0 at Origin [0...]
	// Neighbors 1..10 are orthogonal unit vectors [1,0..], [0,1..]
	// Dist(0, i) = 1.0
	// Dist(i, j) = sqrt(2) = 1.41

	dim := 11
	index.dims.Store(int32(dim))
	vecs := make([][]float32, 11)

	// Vec 0: Origin
	vecs[0] = make([]float32, dim)

	// Vecs 1..10: Orthogonal basis
	for i := 1; i <= 10; i++ {
		vecs[i] = make([]float32, dim)
		vecs[i][i-1] = 1.0 // Orthogonal
	}

	// Point VectorPtrs to these slices
	// Copy vectors to Dense Storage
	for i := 0; i <= 10; i++ {
		cID := chunkID(uint32(i))
		cOff := chunkOffset(uint32(i))
		vecChunk := data.GetVectorsChunk(cID)
		if vecChunk != nil {
			copy(vecChunk[int(cOff)*dim:], vecs[i])
		}
	}
	// Add 10 connections to Node 0
	// Neighbors are 1..10. All dist 1.0. Sorted by ID (impl detail).
	// Node 0 is at chunk 0, offset 0
	baseIdx := 0 // Node 0
	neighbors := data.GetNeighborsChunk(0, 0)
	if neighbors != nil {
		for i := 1; i <= 10; i++ {
			idx := baseIdx + (i - 1)
			// Access Chunk 0 of Neighbors
			neighbors[idx] = uint32(i)
		}
	}

	counts := data.GetCountsChunk(0, 0)
	if counts != nil {
		atomic.StoreInt32(&counts[0], 10)
	}

	// Prune to 5
	// HNSW Heuristic:
	// Select 1 (Dist 1).
	// Check 2. Dist(2,1)=1.41. Dist(2,0)=1. 1.41 * 1.0 > 1. Keep!
	// So orthogonal neighbors should be preserved up to M.

	ctx := index.searchPool.Get().(*ArrowSearchContext)
	defer index.searchPool.Put(ctx)

	index.PruneConnections(ctx, data, 0, 5, 0)

	count := int32(0)
	if counts != nil {
		count = atomic.LoadInt32(&counts[0])
	}
	if count != 5 {
		t.Errorf("expected 5 neighbors after pruning, got %d", count)
	}

	// Check idempotency - count should still be 5 after pruning again
	index.PruneConnections(ctx, data, 0, 5, 0)
	count2 := int32(0)
	if counts != nil {
		count2 = atomic.LoadInt32(&counts[0])
	}
	if count2 != 5 {
		t.Errorf("expected 5 neighbors after idempotent pruning, got %d", count2)
	}
}

func BenchmarkInsert(b *testing.B) {
	// Create dataset with enough records
	dim := 2
	recordCount := b.N

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	listB := builder.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)

	// Pre-generate data to avoid benchmarking generation
	for i := 0; i < recordCount; i++ {
		listB.Append(true)
		valB.AppendValues([]float32{float32(i), float32(i)}, nil)
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()

	dataset := &Dataset{
		Schema:  schema,
		Records: []arrow.RecordBatch{rec},
	}

	config := DefaultArrowHNSWConfig()
	index := NewArrowHNSW(dataset, config, nil)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := index.AddByLocation(0, i)
		if err != nil {
			b.Fatalf("AddByLocation failed: %v", err)
		}
	}
}
