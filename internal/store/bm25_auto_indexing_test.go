package store

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/rs/zerolog"

	"github.com/23skdu/longbow/internal/metrics"
)

// TestBM25AutoIndexingDuringDoPut verifies text columns are auto-indexed when hybrid is enabled
func TestBM25AutoIndexingDuringDoPut(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()

	store, err := NewVectorStoreWithHybridConfig(mem, logger, HybridSearchConfig{
		Enabled:     true,
		TextColumns: []string{"description"},
		BM25:        BM25Config{K1: 1.2, B: 0.75},
		RRFk:        60,
	})
	require.NoError(t, err)

	batch := createBM25TestBatch(t, mem, "description", []string{
		"the quick brown fox jumps over the lazy dog",
		"machine learning algorithms for vector search",
		"natural language processing with transformers",
	})
	defer batch.Release()

	err = store.StoreRecordBatch(context.Background(), "test-dataset", batch)
	require.NoError(t, err)

	bm25 := store.GetBM25Index()
	require.NotNil(t, bm25, "BM25 index should exist")

	results := bm25.SearchBM25("fox", 10)
	assert.NotEmpty(t, results, "BM25 search should return results for indexed term 'fox'")
}

// TestBM25AutoIndexingMultipleBatches verifies incremental indexing across batches
func TestBM25AutoIndexingMultipleBatches(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()

	store, err := NewVectorStoreWithHybridConfig(mem, logger, HybridSearchConfig{
		Enabled:     true,
		TextColumns: []string{"title"},
		BM25:        BM25Config{K1: 1.2, B: 0.75},
		RRFk:        60,
	})
	require.NoError(t, err)

	batch1 := createBM25TestBatch(t, mem, "title", []string{"alpha beta gamma"})
	defer batch1.Release()
	err = store.StoreRecordBatch(context.Background(), "ds", batch1)
	require.NoError(t, err)

	batch2 := createBM25TestBatch(t, mem, "title", []string{"delta epsilon zeta"})
	defer batch2.Release()
	err = store.StoreRecordBatch(context.Background(), "ds", batch2)
	require.NoError(t, err)

	bm25 := store.GetBM25Index()
	require.NotNil(t, bm25)

	resultsAlpha := bm25.SearchBM25("alpha", 10)
	resultsDelta := bm25.SearchBM25("delta", 10)

	assert.NotEmpty(t, resultsAlpha, "First batch term should be searchable")
	assert.NotEmpty(t, resultsDelta, "Second batch term should be searchable")
}

// TestBM25NoIndexingWhenDisabled verifies no indexing when hybrid is disabled
func TestBM25NoIndexingWhenDisabled(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()

	store := NewVectorStore(mem, logger, 1<<30, 0, 0)

	batch := createBM25TestBatch(t, mem, "description", []string{"test content"})
	defer batch.Release()

	err := store.StoreRecordBatch(context.Background(), "ds", batch)
	require.NoError(t, err)

	bm25 := store.GetBM25Index()
	assert.Nil(t, bm25, "BM25 index should be nil when hybrid disabled")
}

// TestBM25MultipleTextColumns verifies multiple text columns are indexed
func TestBM25MultipleTextColumns(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()

	store, err := NewVectorStoreWithHybridConfig(mem, logger, HybridSearchConfig{
		Enabled:     true,
		TextColumns: []string{"title", "body"},
		BM25:        BM25Config{K1: 1.2, B: 0.75},
		RRFk:        60,
	})
	require.NoError(t, err)

	batch := createBM25MultiColBatch(t, mem,
		[]string{"title", "body"},
		[][]string{
			{"uniquetitleterm"},
			{"uniquebodyterm"},
		})
	defer batch.Release()

	err = store.StoreRecordBatch(context.Background(), "ds", batch)
	require.NoError(t, err)

	bm25 := store.GetBM25Index()
	require.NotNil(t, bm25)

	resultsTitle := bm25.SearchBM25("uniquetitleterm", 10)
	resultsBody := bm25.SearchBM25("uniquebodyterm", 10)

	assert.NotEmpty(t, resultsTitle, "Title column should be indexed")
	assert.NotEmpty(t, resultsBody, "Body column should be indexed")
}

// TestBM25IndexingMetrics verifies Prometheus metrics are emitted
func TestBM25IndexingMetrics(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()

	initialCount := testutil.ToFloat64(metrics.BM25DocumentsIndexedTotal)

	store, err := NewVectorStoreWithHybridConfig(mem, logger, HybridSearchConfig{
		Enabled:     true,
		TextColumns: []string{"text"},
		BM25:        BM25Config{K1: 1.2, B: 0.75},
		RRFk:        60,
	})
	require.NoError(t, err)

	batch := createBM25TestBatch(t, mem, "text", []string{"doc1", "doc2", "doc3"})
	defer batch.Release()

	err = store.StoreRecordBatch(context.Background(), "ds", batch)
	require.NoError(t, err)

	finalCount := testutil.ToFloat64(metrics.BM25DocumentsIndexedTotal)
	assert.Equal(t, initialCount+3, finalCount, "Should have indexed 3 documents")
}

// nolint:unparam
func createBM25TestBatch(t *testing.T, mem memory.Allocator, colName string, texts []string) arrow.RecordBatch {
	t.Helper()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: colName, Type: arrow.BinaryTypes.String},
	}, nil)

	builder := array.NewStringBuilder(mem)
	defer builder.Release()

	for _, text := range texts {
		builder.Append(text)
	}

	arr := builder.NewArray()
	defer arr.Release()

	return array.NewRecordBatch(schema, []arrow.Array{arr}, int64(len(texts)))
}

func createBM25MultiColBatch(t *testing.T, mem memory.Allocator, colNames []string, textsPerCol [][]string) arrow.RecordBatch {
	t.Helper()

	fields := make([]arrow.Field, len(colNames))
	for i, name := range colNames {
		fields[i] = arrow.Field{Name: name, Type: arrow.BinaryTypes.String}
	}
	schema := arrow.NewSchema(fields, nil)

	arrays := make([]arrow.Array, len(colNames))
	for i, texts := range textsPerCol {
		builder := array.NewStringBuilder(mem)
		for _, text := range texts {
			builder.Append(text)
		}
		arrays[i] = builder.NewArray()
		builder.Release()
	}

	numRows := int64(len(textsPerCol[0]))
	batch := array.NewRecordBatch(schema, arrays, numRows)

	for _, arr := range arrays {
		arr.Release()
	}

	return batch
}
