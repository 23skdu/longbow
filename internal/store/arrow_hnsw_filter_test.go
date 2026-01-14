package store

import (
	"os"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowHNSW_VectorizedFilter(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "filter_test")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	config := storage.StorageConfig{
		DataPath:         tmpDir,
		SnapshotInterval: 10 * time.Minute,
		WALCompression:   false,
	}

	mem := memory.NewGoAllocator()
	logger := zerolog.New(os.Stdout)
	store := NewVectorStore(mem, logger, 1024*1024*1024, 0, 0)
	err = store.InitPersistence(config)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	// 1. Create Dataset with Metadata
	schemaName := "filter_dataset"
	dim := 8
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
		{Name: "score", Type: arrow.PrimitiveTypes.Float32},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	listB := builder.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)
	catB := builder.Field(1).(*array.Int64Builder)
	scoreB := builder.Field(2).(*array.Float32Builder)

	// Add 100 items
	// Category: 0 or 1
	// Score: 0.0 to 1.0 (buckets of 0.1)
	for i := 0; i < 100; i++ {
		listB.Append(true)
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = float32(i) // distinct vectors
		}
		valB.AppendValues(vec, nil)

		catB.Append(int64(i % 2)) // 0 or 1
		scoreB.Append(float32(float64(i) / 100.0))
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()

	err = store.ApplyDelta(schemaName, rec, 1, time.Now().UnixNano())
	require.NoError(t, err)
	store.WaitForIndexing(schemaName)

	// Get Index
	ds, err := store.GetDataset(schemaName)
	require.NoError(t, err)
	require.NotNil(t, ds.Index)

	// 2. Search with Filter (Category = 1)
	qVec := make([]float32, dim) // matches vector 0 (cat=0) best
	for j := range qVec {
		qVec[j] = 0.0
	}

	// Filter: category = 1 (should exclude vector 0)
	filters := []query.Filter{
		{Field: "category", Operator: "=", Value: "1"},
	}

	results, err := ds.Index.SearchVectors(qVec, 10, filters, SearchOptions{})
	require.NoError(t, err)

	t.Logf("Found %d results for cat=1", len(results))
	for _, res := range results {
		// Verify
		// Get RowIdx? SearchResult only has ID/Score.
		// We trust ID corresponds to row.
		id := int(res.ID)
		assert.Equal(t, 1, id%2, "Expected category 1 (id%%2==1)")
	}
	assert.Greater(t, len(results), 0)

	// 3. Search with Float Filter (score > 0.5)
	filters2 := []query.Filter{
		{Field: "score", Operator: ">", Value: "0.5"},
	}

	results2, err := ds.Index.SearchVectors(qVec, 10, filters2, SearchOptions{})
	require.NoError(t, err)

	t.Logf("Found %d results for score > 0.5", len(results2))
	for _, res := range results2 {
		id := int(res.ID)
		score := float32(float64(id) / 100.0)
		assert.Greater(t, score, float32(0.5))
	}
	assert.Greater(t, len(results2), 0)
}
