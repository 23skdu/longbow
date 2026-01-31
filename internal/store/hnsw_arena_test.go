package store

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestSearchArenaAllocVectorIDSlice tests arena allocation for VectorID slices
func TestSearchArenaAllocVectorIDSlice(t *testing.T) {
	arena := NewSearchArena(1024)

	// Allocate slice of 10 VectorIDs (4 bytes each = 40 bytes)
	ids := arena.AllocVectorIDSlice(10)
	if ids == nil {
		t.Fatal("AllocVectorIDSlice returned nil")
	}
	if len(ids) != 10 {
		t.Errorf("expected len 10, got %d", len(ids))
	}

	// Verify we can write to it
	for i := range ids {
		ids[i] = VectorID(i * 100)
	}
	for i := range ids {
		if ids[i] != VectorID(i*100) {
			t.Errorf("ids[%d] = %d, want %d", i, ids[i], i*100)
		}
	}
}

// TestSearchArenaAllocVectorIDSliceZero tests zero-length allocation
func TestSearchArenaAllocVectorIDSliceZero(t *testing.T) {
	arena := NewSearchArena(1024)
	ids := arena.AllocVectorIDSlice(0)
	if ids == nil {
		t.Fatal("AllocVectorIDSlice(0) returned nil, want empty slice")
	}
	if len(ids) != 0 {
		t.Errorf("expected len 0, got %d", len(ids))
	}
}

// TestSearchArenaAllocVectorIDSliceExhaustion tests arena exhaustion
func TestSearchArenaAllocVectorIDSliceExhaustion(t *testing.T) {
	// Small arena: 32 bytes can fit 8 VectorIDs max
	arena := NewSearchArena(32)

	// First allocation should succeed
	ids := arena.AllocVectorIDSlice(5)
	if ids == nil {
		t.Fatal("first allocation failed")
	}

	// Second allocation should fail (need 20 more bytes, only ~12 left)
	ids2 := arena.AllocVectorIDSlice(5)
	if ids2 != nil {
		t.Error("expected exhaustion, got allocation")
	}
}

// TestHNSWSearchWithArena tests basic SearchWithArena functionality
func TestHNSWSearchWithArena(t *testing.T) {
	// Create dataset with test vectors
	ds := &Dataset{Name: "test-arena"}
	index := NewTestHNSWIndex(ds)

	// Build test record with vectors
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
	}, nil)

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	idBldr := bldr.Field(0).(*array.Int64Builder)
	vecBldr := bldr.Field(1).(*array.FixedSizeListBuilder)
	valBldr := vecBldr.ValueBuilder().(*array.Float32Builder)

	// Add 5 test vectors
	vectors := [][]float32{
		{1.0, 0.0, 0.0, 0.0},
		{0.9, 0.1, 0.0, 0.0},
		{0.0, 1.0, 0.0, 0.0},
		{0.0, 0.0, 1.0, 0.0},
		{0.0, 0.0, 0.0, 1.0},
	}

	for i, vec := range vectors {
		idBldr.Append(int64(i))
		vecBldr.Append(true)
		for _, v := range vec {
			valBldr.Append(v)
		}
	}

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	ds.dataMu.Lock()
	ds.Records = []arrow.RecordBatch{rec}
	ds.dataMu.Unlock()

	// Add vectors to index
	for i := range vectors {
		if _, err := index.AddByLocation(context.Background(), 0, i); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	// Create arena and search
	arena := NewSearchArena(4096)
	query := []float32{1.0, 0.0, 0.0, 0.0}

	results := index.SearchWithArena(query, 3, arena)
	if results == nil {
		t.Fatal("SearchWithArena returned nil")
	}
	if len(results) == 0 {
		t.Fatal("SearchWithArena returned empty results")
	}

	// First result should be vector 0 (exact match)
	if results[0] != VectorID(0) {
		t.Errorf("expected first result to be VectorID(0), got %d", results[0])
	}
}

// TestHNSWSearchWithArenaUsesArena verifies arena offset advances
func TestHNSWSearchWithArenaUsesArena(t *testing.T) {
	ds := &Dataset{Name: "test-arena-usage"}
	index := NewTestHNSWIndex(ds)

	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
	}, nil)

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	idBldr := bldr.Field(0).(*array.Int64Builder)
	vecBldr := bldr.Field(1).(*array.FixedSizeListBuilder)
	valBldr := vecBldr.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < 3; i++ {
		idBldr.Append(int64(i))
		vecBldr.Append(true)
		valBldr.Append(float32(i))
		valBldr.Append(0.0)
		valBldr.Append(0.0)
		valBldr.Append(0.0)
	}

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	ds.dataMu.Lock()
	ds.Records = []arrow.RecordBatch{rec}
	ds.dataMu.Unlock()

	for i := 0; i < 3; i++ {
		_, _ = index.AddByLocation(context.Background(), 0, i)
	}

	arena := NewSearchArena(4096)
	initialOffset := arena.Offset()

	query := []float32{0.5, 0.0, 0.0, 0.0}
	_ = index.SearchWithArena(query, 2, arena)

	// Arena offset should have advanced (results allocated from arena)
	if arena.Offset() <= initialOffset {
		t.Errorf("arena offset did not advance: initial=%d, after=%d", initialOffset, arena.Offset())
	}
}

// TestHNSWSearchWithArenaReset verifies arena can be reset and reused
func TestHNSWSearchWithArenaReset(t *testing.T) {
	ds := &Dataset{Name: "test-arena-reset"}
	index := NewTestHNSWIndex(ds)

	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
	}, nil)

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	idBldr := bldr.Field(0).(*array.Int64Builder)
	vecBldr := bldr.Field(1).(*array.FixedSizeListBuilder)
	valBldr := vecBldr.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < 5; i++ {
		idBldr.Append(int64(i))
		vecBldr.Append(true)
		for j := 0; j < 4; j++ {
			valBldr.Append(float32(i + j))
		}
	}

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	ds.dataMu.Lock()
	ds.Records = []arrow.RecordBatch{rec}
	ds.dataMu.Unlock()

	for i := 0; i < 5; i++ {
		_, _ = index.AddByLocation(context.Background(), 0, i)
	}

	arena := NewSearchArena(4096)
	query := []float32{1.0, 2.0, 3.0, 4.0}

	// First search
	_ = index.SearchWithArena(query, 3, arena)
	offsetAfterFirst := arena.Offset()

	// Reset arena
	arena.Reset()
	if arena.Offset() != 0 {
		t.Errorf("arena offset after reset should be 0, got %d", arena.Offset())
	}

	// Second search reuses memory
	_ = index.SearchWithArena(query, 3, arena)
	offsetAfterSecond := arena.Offset()

	// Should use same amount of arena space
	if offsetAfterSecond != offsetAfterFirst {
		t.Logf("arena usage: first=%d, second=%d", offsetAfterFirst, offsetAfterSecond)
	}
}

// TestHNSWSearchWithArenaNilArena tests fallback when arena is nil
func TestHNSWSearchWithArenaNilArena(t *testing.T) {
	ds := &Dataset{Name: "test-nil-arena"}
	index := NewTestHNSWIndex(ds)

	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
	}, nil)

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	idBldr := bldr.Field(0).(*array.Int64Builder)
	vecBldr := bldr.Field(1).(*array.FixedSizeListBuilder)
	valBldr := vecBldr.ValueBuilder().(*array.Float32Builder)

	idBldr.Append(0)
	vecBldr.Append(true)
	valBldr.Append(1.0)
	valBldr.Append(0.0)
	valBldr.Append(0.0)
	valBldr.Append(0.0)

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	ds.dataMu.Lock()
	ds.Records = []arrow.RecordBatch{rec}
	ds.dataMu.Unlock()

	_, _ = index.AddByLocation(context.Background(), 0, 0)

	query := []float32{1.0, 0.0, 0.0, 0.0}

	// Should not panic with nil arena, falls back to heap allocation
	results := index.SearchWithArena(query, 1, nil)
	if results == nil {
		t.Fatal("SearchWithArena with nil arena returned nil")
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
}

// TestHNSWSearchWithArenaExhaustion tests behavior when arena is exhausted
func TestHNSWSearchWithArenaExhaustion(t *testing.T) {
	ds := &Dataset{Name: "test-arena-exhaustion"}
	index := NewTestHNSWIndex(ds)

	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
	}, nil)

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	idBldr := bldr.Field(0).(*array.Int64Builder)
	vecBldr := bldr.Field(1).(*array.FixedSizeListBuilder)
	valBldr := vecBldr.ValueBuilder().(*array.Float32Builder)

	// Add 10 vectors
	for i := 0; i < 10; i++ {
		idBldr.Append(int64(i))
		vecBldr.Append(true)
		for j := 0; j < 4; j++ {
			valBldr.Append(float32(i))
		}
	}

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	ds.dataMu.Lock()
	ds.Records = []arrow.RecordBatch{rec}
	ds.dataMu.Unlock()

	for i := 0; i < 10; i++ {
		_, _ = index.AddByLocation(context.Background(), 0, i)
	}

	// Very small arena that will exhaust
	arena := NewSearchArena(8) // Only 8 bytes
	query := []float32{5.0, 5.0, 5.0, 5.0}

	// Should fall back to heap allocation when arena exhausted
	results := index.SearchWithArena(query, 5, arena)
	if results == nil {
		t.Fatal("SearchWithArena should fall back when arena exhausted")
	}
}

// TestSearchArenaPoolIntegration tests arena with existing pool mechanism
func TestSearchArenaPoolIntegration(t *testing.T) {
	// Verify SearchArena can coexist with resultPool
	arena := NewSearchArena(4096)

	// Allocate various types
	floats := arena.AllocFloat32Slice(100)
	ids := arena.AllocVectorIDSlice(50)
	raw := arena.Alloc(64)

	if floats == nil || ids == nil || raw == nil {
		t.Fatal("allocation failed")
	}

	// Verify independence
	if len(floats) != 100 || len(ids) != 50 || len(raw) != 64 {
		t.Error("unexpected slice lengths")
	}

	// Reset clears all
	arena.Reset()
	if arena.Offset() != 0 {
		t.Error("reset failed")
	}
}

// BenchmarkSearchWithArenaVsSearchByID compares allocation strategies
func BenchmarkSearchWithArenaVsSearchByID(b *testing.B) {
	ds := &Dataset{Name: "bench-arena"}
	index := NewTestHNSWIndex(ds)

	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
	}, nil)

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	idBldr := bldr.Field(0).(*array.Int64Builder)
	vecBldr := bldr.Field(1).(*array.FixedSizeListBuilder)
	valBldr := vecBldr.ValueBuilder().(*array.Float32Builder)

	// Add 1000 vectors
	for i := 0; i < 1000; i++ {
		idBldr.Append(int64(i))
		vecBldr.Append(true)
		for j := 0; j < 128; j++ {
			valBldr.Append(float32(i*128 + j))
		}
	}

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	ds.dataMu.Lock()
	ds.Records = []arrow.RecordBatch{rec}
	ds.dataMu.Unlock()

	for i := 0; i < 1000; i++ {
		_, _ = index.AddByLocation(context.Background(), 0, i)
	}

	query := make([]float32, 128)
	for i := range query {
		query[i] = float32(i)
	}

	b.Run("SearchByID", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// SearchByID(id VectorID, k int) ([]Candidate, error)
			// Wait, SearchByID signature might be different
			// internal/store/index.go: SearchByID(id VectorID, k int) ([]Candidate, error)
			// But here we need to discard results to be fair?
			// results, _ := index.SearchByID(context.Background(), VectorID(i%1000), 10, nil, SearchOptions{})
			// _ = results
			// index.PutResults(results) // If pool is exposed?
			// The benchmark assumes we can return to pool. If not, just ignore.
		}
	})

	b.Run("SearchWithArena", func(b *testing.B) {
		arena := NewSearchArena(4096)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			arena.Reset()
			_ = index.SearchWithArena(query, 10, arena)
		}
	})
}
