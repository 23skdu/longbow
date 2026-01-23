package store

import (
	"context"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHNSWCompaction_Functional(t *testing.T) {
	mem := memory.NewGoAllocator()
	dim := 16
	numNodes := 1200
	r := rand.New(rand.NewSource(42))

	// 1. Setup Arrow Dataset
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	listB := builder.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < numNodes; i++ {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = r.Float32()
		}
		listB.Append(true)
		valB.AppendValues(vec, nil)
	}

	record := builder.NewRecordBatch()
	defer record.Release()

	ds := &Dataset{
		Name:    "test_compaction",
		Schema:  schema,
		Records: []arrow.RecordBatch{record},
	}

	config := DefaultArrowHNSWConfig()
	config.Dims = dim
	config.PackedAdjacencyEnabled = true // Enable it back
	h := NewArrowHNSW(ds, config, nil)
	defer h.Close()

	// 2. Insert all nodes
	for i := 0; i < numNodes; i++ {
		_, err := h.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	require.Equal(t, numNodes, h.Size())

	// 3. Record baseline search
	query := make([]float32, dim)
	for i := range query {
		query[i] = r.Float32()
	}
	_, err := h.Search(context.Background(), query, 10, 100, nil)
	require.NoError(t, err)

	// 4. Delete random 50%
	deleted := make(map[uint32]bool)
	for len(deleted) < numNodes/2 {
		id := uint32(r.Intn(numNodes))
		if !deleted[id] {
			deleted[id] = true
			_ = h.Delete(id)
		}
	}

	// 5. Verify NeedsCompaction
	require.True(t, h.NeedsCompaction())

	// 6. Run Compaction
	stats, err := h.CompactGraph(context.Background())
	require.NoError(t, err)
	require.Equal(t, numNodes-len(deleted), stats.NewNodeCount)

	// 7. Verify Post-Compaction State
	require.Equal(t, numNodes-len(deleted), h.Size())
	require.Equal(t, uint64(0), h.deleted.Count())

	// 8. Verify Search Integrity
	postCompResults, err := h.Search(context.Background(), query, 10, 200, nil)
	require.NoError(t, err)
	require.NotEmpty(t, postCompResults)

	for _, res := range postCompResults {
		loc, ok := h.GetLocation(VectorID(res.ID))
		require.True(t, ok)
		require.False(t, deleted[uint32(loc.RowIdx)], "Should not find deleted row %d", loc.RowIdx)
	}
}

func TestHNSWCompaction_Empty(t *testing.T) {
	config := DefaultArrowHNSWConfig()
	config.Dims = 4
	h := NewArrowHNSW(nil, config, nil)
	defer h.Close()

	stats, err := h.CompactGraph(context.Background())
	require.NoError(t, err)
	assert.Zero(t, stats.OldNodeCount)
}

func TestHNSWCompaction_NoDeletions(t *testing.T) {
	mem := memory.NewGoAllocator()
	dim := 4
	numNodes := 100

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	listB := builder.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < numNodes; i++ {
		listB.Append(true)
		valB.AppendValues([]float32{float32(i), 0, 0, 0}, nil)
	}

	record := builder.NewRecordBatch()
	defer record.Release()

	ds := &Dataset{Records: []arrow.RecordBatch{record}}
	config := DefaultArrowHNSWConfig()
	h := NewArrowHNSW(ds, config, nil)
	defer h.Close()

	for i := 0; i < numNodes; i++ {
		_, _ = h.AddByLocation(context.Background(), 0, i)
	}

	stats, err := h.CompactGraph(context.Background())
	require.NoError(t, err)
	require.Equal(t, numNodes, stats.NewNodeCount)
	require.Equal(t, 0, stats.NodesRemoved)
}
