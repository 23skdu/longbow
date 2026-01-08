package store

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraphRAG_Scoring(t *testing.T) {
	// Setup Store
	mem := memory.NewGoAllocator()
	logger := zerolog.New(zerolog.NewConsoleWriter())
	s := NewVectorStore(mem, logger, 1024*1024, 1024*1024, 0)
	defer func() { _ = s.Close() }()

	datasetName := "graph_rag_test"
	dims := 4

	// Create 3 vectors
	// A: [1, 0, 0, 0] (Target Match)
	// B: [0, 1, 0, 0] (Orthogonal)
	// C: [0, 0, 1, 0] (Orthogonal)
	vectors := [][]float32{
		{1, 0, 0, 0}, // ID 0 (A)
		{0, 1, 0, 0}, // ID 1 (B)
		{0, 0, 1, 0}, // ID 2 (C)
	}

	// Create Arrow Record
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
		{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	listB := b.Field(0).(*array.FixedSizeListBuilder)
	floatB := listB.ValueBuilder().(*array.Float32Builder)
	idB := b.Field(1).(*array.Uint32Builder)

	for i, vec := range vectors {
		listB.Append(true)
		floatB.AppendValues(vec, nil)
		idB.Append(uint32(i))
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	err := s.ApplyDelta(datasetName, rec, 1, time.Now().UnixNano())
	require.NoError(t, err)

	// Wait for indexing
	time.Sleep(200 * time.Millisecond)

	// Add Graph Edges
	// A (0) -> connected to -> B (1)
	// A (0) -> connected to -> C (2)
	// We expect query matching A to pull in B and C via graph expansion.
	ds, err := s.GetDataset(datasetName)
	require.NoError(t, err)

	ds.dataMu.Lock()
	if ds.Graph == nil {
		ds.Graph = NewGraphStore()
	}
	_ = ds.Graph.AddEdge(Edge{Subject: 0, Predicate: "related", Object: 1, Weight: 1.0})
	_ = ds.Graph.AddEdge(Edge{Subject: 0, Predicate: "related", Object: 2, Weight: 0.5})
	ds.dataMu.Unlock()

	// 1. Standard Vector Search (Alpha=1.0, GraphAlpha=0.0)
	// Query: [1, 0, 0, 0] should match A(0) perfectly. B(1) and C(2) should be 0 or low.
	ctx := context.Background()
	query := []float32{1, 0, 0, 0}
	results, err := SearchHybrid(ctx, s, datasetName, query, "", 10, 1.0, 0, 0.0, 0)
	require.NoError(t, err)

	// Expect A to be top
	require.GreaterOrEqual(t, len(results), 1)
	assert.Equal(t, VectorID(0), results[0].ID)
	// Even if it returns B and C due to k=10, their scores should be low (Dot product 0).
	// Actually, HNSW might effectively return them but with score 0 depending on distance metric.
	// We used default metric (L2 or Dot). If Dot, 0.

	// 2. GraphRAG Search (GraphAlpha=0.5)
	// A is seed. It spreads to B and C.
	// B should get score ~ A.Score * 1.0
	// C should get score ~ A.Score * 0.5
	resultsGraph, err := SearchHybrid(ctx, s, datasetName, query, "", 10, 1.0, 0, 0.5, 2)
	require.NoError(t, err)

	// Should contain A, B, C
	foundB := false
	foundC := false
	var scoreB, scoreC float32

	for _, r := range resultsGraph {
		if r.ID == 1 {
			foundB = true
			scoreB = r.Score
		}
		if r.ID == 2 {
			foundC = true
			scoreC = r.Score
		}
	}

	assert.True(t, foundB, "Graph expansion should include Node B")
	assert.True(t, foundC, "Graph expansion should include Node C")

	// Verify Ranking: A > B > C (since weight A->B=1.0 > A->C=0.5)
	// Note: Final score is mixed with vector score.
	// VecScores: A=1.0, B=0, C=0 (normalized)
	// GraphScores: A=0 (it's seed, does it self-loop in current logic? No), B=High, C=Med
	// Wait, does 'processNeighbor' visit 'next'. 'A' is current. Next are B, C.
	// Does A get graph score?
	// Logic says: graphScores[target] += seedScore...
	// So targets B and C get graph scores. A does NOT get graph score unless there is a cycle back to A.
	//
	// Final = (1-0.5)*Vec + 0.5*Graph
	// A: 0.5*1.0 + 0.5*0 = 0.5
	// B: 0.5*0 + 0.5*(1.0 * decay) > 0
	// C: 0.5*0 + 0.5*(0.5 * decay) > 0
	//
	// So A might actually drop below B if Graph Score for B is very high (normalized).
	// MaxGraphScore normalization happens.
	// B score raw = 1.0 * decay. MaxGraph = B score. So Normalized B = 1.0.
	// Final B = 0.5*0 + 0.5*1.0 = 0.5.
	// Final A = 0.5.
	// Tie?
	// Let's verify presence first.

	t.Logf("Results: %+v", resultsGraph)
	assert.Greater(t, scoreB, float32(0), "B should have non-zero score")
	assert.Greater(t, scoreC, float32(0), "C should have non-zero score")
	assert.Greater(t, scoreB, scoreC, "B should be ranked higher than C due to edge weight")
}
