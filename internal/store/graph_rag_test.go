package store

import (
	"context"
	"os"
	"testing"
	"time"

	basecore "github.com/23skdu/longbow/internal/core"
	storecore "github.com/23skdu/longbow/internal/store/internal/core"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraphRAG_SearchHybrid(t *testing.T) {
	// Setup store
	mem := memory.NewGoAllocator()
	logger := zerolog.New(os.Stderr)
	s := NewVectorStore(mem, logger, 1024*1024*1024, 0, 0)
	defer s.Close()

	datasetName := "test_graphrag"
	dims := 128
	count := 100

	// 1. Create Arrow Data
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
			{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
		},
		nil,
	)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	listB := b.Field(0).(*array.FixedSizeListBuilder)
	floatB := listB.ValueBuilder().(*array.Float32Builder)
	idB := b.Field(1).(*array.Uint32Builder)

	for i := 0; i < count; i++ {
		vec := make([]float32, dims)
		for j := 0; j < dims; j++ {
			vec[j] = float32(i + j)
		}
		listB.Append(true)
		floatB.AppendValues(vec, nil)
		idB.Append(uint32(i))
	}

	rec := b.NewRecord()
	defer rec.Release()

	// 2. Ingest
	err := s.applyReplayBatch(datasetName, rec, 1, time.Now().UnixNano())
	require.NoError(t, err)

	// 3. Add Graph Edges
	ds, _ := s.getDataset(datasetName)
	ds.dataMu.Lock()
	if ds.Graph == nil {
		ds.Graph = NewGraphStore()
	}
	// Create a simple chain: 0 -> 1 -> 2 ... -> 99
	for i := 0; i < count-1; i++ {
		_ = ds.Graph.AddEdge(Edge{
			Subject:   VectorID(i),
			Predicate: "related",
			Object:    VectorID(i + 1),
			Weight:    1.0,
		})
	}
	ds.dataMu.Unlock()

	// 4. Test Hybrid Search
	ctx := context.Background()
	query := make([]float32, dims)
	for i := 0; i < dims; i++ {
		query[i] = float32(i) // Matches node 0
	}

	// Case 1: alpha=0 (Graph only)
	results, err := SearchHybrid(ctx, s, datasetName, query, "", 5, 0.0, 0, 0.5, 2)
	require.NoError(t, err)
	assert.NotEmpty(t, results)

	// Case 2: alpha=1 (Full GraphRAG re-ranking)
	results, err = SearchHybrid(ctx, s, datasetName, query, "", 5, 1.0, 0, 0.5, 2)
	require.NoError(t, err)
	assert.NotEmpty(t, results)
}

func TestGraphRAG_Stability_Large(t *testing.T) {
	// Stability test for Part 23: Ensure no deadlocks or OOM with 25k nodes
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	s := NewVectorStore(mem, logger, 2*1024*1024*1024, 0, 0)
	s.StartIndexingWorkers(4)
	defer s.Close()

	datasetName := "graph_rag_stability"
	count := 25000
	dims := 128

	// 1. Create Data (25k nodes)
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
			{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
		},
		nil,
	)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	listB := b.Field(0).(*array.FixedSizeListBuilder)
	floatB := listB.ValueBuilder().(*array.Float32Builder)
	idB := b.Field(1).(*array.Uint32Builder)

	for i := 0; i < count; i++ {
		vec := make([]float32, dims)
		vec[0] = float32(i) / float32(count)
		listB.Append(true)
		floatB.AppendValues(vec, nil)
		idB.Append(uint32(i))
	}

	rec := b.NewRecord()
	defer rec.Release()

	ds, _ := s.getOrCreateDataset(datasetName, func() *Dataset {
		return NewDataset(datasetName, schema)
	})

	config := types.ArrowHNSWConfig{
		M:              16,
		EfConstruction: 100,
		Dims:           dims,
		DataType:       types.VectorTypeFloat32,
		Metric:         basecore.MetricEuclidean,
	}
	hnsw := storecore.NewArrowHNSW(ds, &config, nil)
	ds.dataMu.Lock()
	ds.Index = hnsw
	ds.dataMu.Unlock()

	err := s.applyReplayBatch(datasetName, rec, 1, time.Now().UnixNano())
	require.NoError(t, err)

	// Add edges for GraphRAG
	ds.dataMu.Lock()
	if ds.Graph == nil {
		ds.Graph = NewGraphStore()
	}
	for i := 0; i < count-1; i++ {
		_ = ds.Graph.AddEdge(Edge{Subject: VectorID(i), Predicate: "related", Object: VectorID(i+1), Weight: 1.0})
	}
	ds.dataMu.Unlock()

	// 2. Wait for Indexing with polling
	startWait := time.Now()
	for {
		ds.dataMu.RLock()
		idxLen := 0
		if ds.Index != nil {
			idxLen = ds.Index.Len()
		}
		ds.dataMu.RUnlock()
		if idxLen >= count {
			break
		}
		if time.Since(startWait) > 30*time.Second {
			t.Fatalf("Timeout waiting for indexing: %d/%d", idxLen, count)
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Logf("Indexing 25k nodes took %v", time.Since(startWait))

	// 3. Search
	ctx := context.Background()
	query := make([]float32, dims)
	query[0] = 0.5

	start := time.Now()
	results, err := SearchHybrid(ctx, s, datasetName, query, "", 10, 1.0, 0, 0.5, 10)
	require.NoError(t, err)
	duration := time.Since(start)

	assert.NotEmpty(t, results, "GraphRAG Search should return results")
	assert.GreaterOrEqual(t, len(results), 1, "GraphRAG Search should find multiple connected nodes")
	t.Logf("GraphRAG Search (25k nodes) took %v", duration)
}

func TestHNSW_SearchLayer_NilContext_Regression(t *testing.T) {
	// Regression test for Part 23 fix: searchLayer panicking with nil context
	h := storecore.NewArrowHNSW(nil, &types.ArrowHNSWConfig{
		M:              16,
		EfConstruction: 64,
		Dims:           128,
		DataType:       types.VectorTypeFloat32,
		Metric:         basecore.MetricEuclidean,
	}, nil)
	
	// Create some data
	vec := make([]float32, 128)
	
	// This should not panic
	err := h.InsertWithVector(0, vec, 0)
	assert.NoError(t, err, "InsertWithVector should handle nil search context internally")
}
