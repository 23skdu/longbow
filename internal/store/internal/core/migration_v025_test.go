package core

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	amemory "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockProvider implements lbtypes.IndexDataProvider
type MockProvider struct {
	recs []arrow.RecordBatch
	mu   sync.RWMutex
}

func (m *MockProvider) GetName() string                     { return "test" }
func (m *MockProvider) GetRecords() []arrow.RecordBatch     { return m.recs }
func (m *MockProvider) GetSchema() *arrow.Schema            { return m.recs[0].Schema() }
func (m *MockProvider) GetTombstones() map[int]*types.Bitset { return nil }
func (m *MockProvider) GetPQEncoder() *pq.PQEncoder         { return nil }
func (m *MockProvider) RLockData()                          { m.mu.RLock() }
func (m *MockProvider) RUnlockData()                       { m.mu.RUnlock() }
func (m *MockProvider) GenerateFilterBitset(filters []core.Filter, expr types.FilterExpr) (*types.Bitset, error) {
	return nil, nil
}
func (m *MockProvider) ResetTombstones() {}
func (m *MockProvider) GetIndex() any     { return nil }

func TestArrowHNSW_SharedVectorSpace_Integration(t *testing.T) {
	done := make(chan bool)
	go func() {
		defer func() { done <- true }()
		
		pool := amemory.NewGoAllocator()
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		}, nil)

		b := array.NewRecordBuilder(pool, schema)
		defer b.Release()

		vBuilder := b.Field(0).(*array.FixedSizeListBuilder)
		vValBuilder := vBuilder.ValueBuilder().(*array.Float32Builder)
		idBuilder := b.Field(1).(*array.Int64Builder)

		// Add 2 vectors
		vBuilder.Append(true)
		vValBuilder.AppendValues([]float32{1.0, 2.0}, nil)
		idBuilder.Append(1)

		vBuilder.Append(true)
		vValBuilder.AppendValues([]float32{3.0, 4.0}, nil)
		idBuilder.Append(2)

		rec := b.NewRecord()
		defer rec.Release()

		provider := &MockProvider{recs: []arrow.RecordBatch{rec}}
		config := types.DefaultArrowHNSWConfig()
		config.Dims = 2
		config.SharedVectorSpace = true
		
		// Create HNSW
		t.Log("Creating HNSW...")
		hnsw := NewArrowHNSW(nil, &config, nil)
		hnsw.dataset = provider // set manually for test
		hnsw.sharedVectorSpace.Store(true)

		// 1. Add by location
		t.Log("Adding by location 0,0...")
		_, err := hnsw.AddByLocation(context.Background(), 0, 0)
		require.NoError(t, err)
		t.Log("Adding by location 0,1...")
		_, err = hnsw.AddByLocation(context.Background(), 0, 1)
		require.NoError(t, err)

		// 2. Verify GraphData has NO vectors allocated
		t.Log("Verifying GraphData...")
		gd := hnsw.data.Load()
		assert.NotNil(t, gd)
		assert.Empty(t, gd.VectorsF32, "VectorsF32 should be empty when SharedVectorSpace is enabled")

		// 3. Search and verify results
		// Query [1.1, 2.1] should find ID 0 (offset 0 in batch 0)
		t.Log("Searching...")
		results, err := hnsw.SearchVectors(context.Background(), []float32{1.1, 2.1}, 1, nil, nil)
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, uint32(0), uint32(results[0].ID))
		
		// 4. Verify extraction works
		vec := hnsw.extractFromDataset(0, 1)
		assert.Equal(t, []float32{3.0, 4.0}, vec)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		buf := make([]byte, 1024*1024)
		n := runtime.Stack(buf, true)
		t.Fatalf("Test timed out! Stack trace:\n%s", buf[:n])
	}
}

func TestArrowHNSW_RelocateToOffHeap_Full(t *testing.T) {
	// Setup mock provider
	pool := amemory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	vBuilder := b.Field(0).(*array.FixedSizeListBuilder)
	vValBuilder := vBuilder.ValueBuilder().(*array.Float32Builder)
	idBuilder := b.Field(1).(*array.Int64Builder)

	for i := 0; i < 100; i++ {
		vec := make([]float32, 128)
		vec[0] = float32(i)
		vBuilder.Append(true)
		vValBuilder.AppendValues(vec, nil)
		idBuilder.Append(int64(i))
	}
	rec := b.NewRecord()
	defer rec.Release()

	provider := &MockProvider{recs: []arrow.RecordBatch{rec}}
	config := types.DefaultArrowHNSWConfig()
	config.Dims = 128
	hnsw := NewArrowHNSW(nil, &config, nil)
	hnsw.dataset = provider

	// Add data
	for i := 0; i < 100; i++ {
		_, err := hnsw.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	err := hnsw.RelocateToOffHeap()
	require.NoError(t, err)
	
	// Verify search still works
	query := make([]float32, 128)
	query[0] = 50.0
	results, err := hnsw.SearchVectors(context.Background(), query, 1, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, results)
	assert.Equal(t, uint32(50), uint32(results[0].ID))
}

func TestPackedAdjacency_RelocateToOffHeap(t *testing.T) {
	arena := memory.NewSlabArena(16384) // Large enough for pages (8KB each)
	pa := NewPackedAdjacency(arena, 2000) // 2 chunks
	
	// Populate first chunk
	err := pa.SetNeighbors(0, []uint32{1, 2, 3})
	require.NoError(t, err)
	
	// Populate second chunk
	err = pa.SetNeighbors(1025, []uint32{10, 20})
	require.NoError(t, err)
	
	alloc := memory.NewOffHeapAllocator()
	pa.RelocateToOffHeap(alloc)
	
	// Verify data remains correct
	neighbors, ok := pa.GetNeighbors(0)
	assert.True(t, ok)
	assert.Equal(t, []uint32{1, 2, 3}, neighbors)
	
	neighbors, ok = pa.GetNeighbors(1025)
	assert.True(t, ok)
	assert.Equal(t, []uint32{10, 20}, neighbors)
	
	// Expectations: 2 chunks * 8 bytes = 16 bytes + arena slabs
	assert.Greater(t, alloc.Allocated(), int64(16))
}

func TestSlabArena_RelocateToOffHeap(t *testing.T) {
	a := memory.NewSlabArena(1024)
	off, _ := a.Alloc(100)
	data := a.Get(off, 100)
	for i := range data { data[i] = byte(i) }
	
	alloc := memory.NewOffHeapAllocator()
	err := a.ConvertToOffHeap(alloc)
	require.NoError(t, err)
	
	assert.Equal(t, int64(1024), alloc.Allocated())
	
	// Verify data remains same
	dataAfter := a.Get(off, 100)
	require.Len(t, dataAfter, 100)
	for i := range dataAfter {
		if dataAfter[i] != byte(i) {
			t.Errorf("Data mismatch at %d: expected %d, got %d", i, byte(i), dataAfter[i])
		}
	}
}

func TestGraphData_RelocateToOffHeap_Full(t *testing.T) {
	gd := types.NewGraphData(100, 128, false, false, 0, false, false, false, types.VectorTypeFloat32, false, false, false, 8, "test", nil, false)
	
	// Enable all features to test all arenas
	gd.SQ8Enabled = true
	gd.PQEnabled = true
	gd.PQM = 16
	gd.BQEnabled = true
	gd.TurboQuantEnabled = true
	gd.TurboQuantBits = 8
	
	// Initialize PackedNeighbors
	gd.PackedNeighbors = make([]types.PackedNeighbors, types.ArrowMaxLayers)
	for l := 0; l < types.ArrowMaxLayers; l++ {
		gd.PackedNeighbors[l] = NewPackedAdjacency(memory.NewSlabArena(1024), 100)
	}
	
	err := gd.EnsureChunks(10, 128)
	require.NoError(t, err)
	
	// Verify they are currently on-heap (standard sa)
	assert.False(t, gd.Float32Arena.Slab().IsOffHeap())
	
	// Relocate
	err = gd.RelocateToOffHeap()
	require.NoError(t, err)
	
	// Verify all are off-heap
	assert.True(t, gd.Float32Arena.Slab().IsOffHeap())
	assert.True(t, gd.Uint8Arena.Slab().IsOffHeap())
	assert.True(t, gd.Uint64Arena.Slab().IsOffHeap())
	
	// Verify Neighbors are also off-heap
	for l := 0; l < types.ArrowMaxLayers; l++ {
		if gd.PackedNeighbors[l] != nil {
			assert.True(t, gd.PackedNeighbors[l].IsOffHeap())
		}
	}
}

func TestGraphData_SharedVectorSpace_PreAllocate(t *testing.T) {
	// SharedVectorSpace = true
	gd := types.NewGraphData(1000, 128, false, false, 0, false, false, false, types.VectorTypeFloat32, false, false, false, 8, "test", nil, true)
	
	err := gd.PreAllocate(1000)
	require.NoError(t, err)
	
	// Vectors should be empty
	assert.Empty(t, gd.VectorsF32)
	assert.Nil(t, gd.Float32Arena)
	
	// Topology should be allocated
	assert.NotEmpty(t, gd.Levels)
	assert.Len(t, gd.Levels, 1) // 1000 < ChunkSize(1024)
	assert.NotNil(t, gd.Levels[0])
	
	assert.NotEmpty(t, gd.Neighbors[0])
}

func TestGraphData_NeedsChunk_SharedVectorSpace(t *testing.T) {
	gd := types.NewGraphData(100, 128, false, false, 0, false, false, false, types.VectorTypeFloat32, false, false, false, 8, "test", nil, true)
	
	// Needs topology for chunk 2 (outside initial capacity of 1 chunk)
	assert.True(t, gd.NeedsChunk(2))
	
	err := gd.EnsureChunk(2, 0, 128)
	require.NoError(t, err)
	
	// Now Levels[2] is allocated, and SharedVectorSpace is true, so no more chunks needed for id 2*ChunkSize
	assert.False(t, gd.NeedsChunk(2))
	
	// Still needs chunk 1
	assert.True(t, gd.NeedsChunk(1))
}
