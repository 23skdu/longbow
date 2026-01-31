package store

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	defer func() { _ = store.Close() }()
	store.StartIngestionWorkers(1)

	batch := createBM25TestBatch(t, mem, "description", []string{
		"the quick brown fox jumps over the lazy dog",
		"machine learning algorithms for vector search",
		"natural language processing with transformers",
	})
	defer batch.Release()

	err = store.StoreRecordBatch(context.Background(), "test-dataset", batch)
	require.NoError(t, err)

	var bm25 *BM25InvertedIndex
	assert.Eventually(t, func() bool {
		bm25 = store.GetBM25Index("test-dataset")
		return bm25 != nil
	}, 2*time.Second, 100*time.Millisecond, "BM25 index should eventually exist")

	assert.Eventually(t, func() bool {
		results := bm25.SearchBM25("fox", 10, nil)
		return len(results) > 0
	}, 2*time.Second, 100*time.Millisecond, "BM25 search should return results for indexed term 'fox'")
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
	defer func() { _ = store.Close() }()
	store.StartIngestionWorkers(1)

	batch1 := createBM25TestBatch(t, mem, "title", []string{"alpha beta gamma"})
	defer batch1.Release()
	err = store.StoreRecordBatch(context.Background(), "ds", batch1)
	require.NoError(t, err)

	batch2 := createBM25TestBatch(t, mem, "title", []string{"delta epsilon zeta"})
	defer batch2.Release()
	err = store.StoreRecordBatch(context.Background(), "ds", batch2)
	require.NoError(t, err)

	var bm25 *BM25InvertedIndex
	assert.Eventually(t, func() bool {
		bm25 = store.GetBM25Index("ds")
		return bm25 != nil
	}, 2*time.Second, 100*time.Millisecond)

	assert.Eventually(t, func() bool {
		return len(bm25.SearchBM25("alpha", 10, nil)) > 0
	}, 2*time.Second, 100*time.Millisecond, "First batch term should be searchable")

	assert.Eventually(t, func() bool {
		return len(bm25.SearchBM25("delta", 10, nil)) > 0
	}, 2*time.Second, 100*time.Millisecond, "Second batch term should be searchable")
}

// TestBM25NoIndexingWhenDisabled verifies no indexing when hybrid is disabled
func TestBM25NoIndexingWhenDisabled(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()

	store := NewVectorStore(mem, logger, 1<<30, 0, 0)
	defer func() { _ = store.Close() }()
	store.StartIngestionWorkers(1)

	batch := createBM25TestBatch(t, mem, "description", []string{"test content"})
	defer batch.Release()

	err := store.StoreRecordBatch(context.Background(), "ds", batch)
	require.NoError(t, err)

	bm25 := store.GetBM25Index("ds")
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
	defer func() { _ = store.Close() }()
	store.StartIngestionWorkers(1)

	batch := createBM25MultiColBatch(t, mem,
		[]string{"title", "body"},
		[][]string{
			{"uniquetitleterm"},
			{"uniquebodyterm"},
		})
	defer batch.Release()

	err = store.StoreRecordBatch(context.Background(), "ds", batch)
	require.NoError(t, err)

	var bm25 *BM25InvertedIndex
	assert.Eventually(t, func() bool {
		bm25 = store.GetBM25Index("ds")
		return bm25 != nil
	}, 2*time.Second, 100*time.Millisecond)

	assert.Eventually(t, func() bool {
		return len(bm25.SearchBM25("uniquetitleterm", 10, nil)) > 0
	}, 2*time.Second, 100*time.Millisecond, "Title column should be indexed")

	assert.Eventually(t, func() bool {
		return len(bm25.SearchBM25("uniquebodyterm", 10, nil)) > 0
	}, 2*time.Second, 100*time.Millisecond, "Body column should be indexed")
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
	defer func() { _ = store.Close() }()
	store.StartIngestionWorkers(1)

	batch := createBM25TestBatch(t, mem, "text", []string{"doc1", "doc2", "doc3"})
	defer batch.Release()

	err = store.StoreRecordBatch(context.Background(), "ds", batch)
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		finalCount := testutil.ToFloat64(metrics.BM25DocumentsIndexedTotal)
		return finalCount == initialCount+3
	}, 2*time.Second, 100*time.Millisecond, "Should have indexed 3 documents")
}

// nolint:unparam
func createBM25TestBatch(t *testing.T, mem memory.Allocator, colName string, texts []string) arrow.RecordBatch {
	t.Helper()

	dim := 4
	schema := arrow.NewSchema([]arrow.Field{
		{Name: colName, Type: arrow.BinaryTypes.String},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)

	bld := array.NewRecordBuilder(mem, schema)
	defer bld.Release()

	// Text
	textBld := bld.Field(0).(*array.StringBuilder)
	for _, text := range texts {
		textBld.Append(text)
	}

	// Vector
	vecBld := bld.Field(1).(*array.FixedSizeListBuilder)
	valBld := vecBld.ValueBuilder().(*array.Float32Builder)
	for range texts {
		vecBld.Append(true)
		for i := 0; i < dim; i++ {
			valBld.Append(0.0)
		}
	}

	return bld.NewRecordBatch()
}

func createBM25MultiColBatch(t *testing.T, mem memory.Allocator, colNames []string, textsPerCol [][]string) arrow.RecordBatch {
	t.Helper()

	dim := 4
	fields := make([]arrow.Field, len(colNames)+1)
	for i, name := range colNames {
		fields[i] = arrow.Field{Name: name, Type: arrow.BinaryTypes.String}
	}
	fields[len(colNames)] = arrow.Field{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)}
	schema := arrow.NewSchema(fields, nil)

	bld := array.NewRecordBuilder(mem, schema)
	defer bld.Release()

	numRows := len(textsPerCol[0])

	// Text columns
	for i, texts := range textsPerCol {
		textBld := bld.Field(i).(*array.StringBuilder)
		for _, text := range texts {
			textBld.Append(text)
		}
	}

	// Vector column
	vecBld := bld.Field(len(colNames)).(*array.FixedSizeListBuilder)
	valBld := vecBld.ValueBuilder().(*array.Float32Builder)
	for i := 0; i < numRows; i++ {
		vecBld.Append(true)
		for j := 0; j < dim; j++ {
			valBld.Append(0.0)
		}
	}

	return bld.NewRecordBatch()
}
