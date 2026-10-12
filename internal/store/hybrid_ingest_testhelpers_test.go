package store

import (
	"testing"

	"github.com/23skdu/longbow/internal/store/index"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmemory "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// minimalVectorSchema is the smallest schema applyBatchToMemory accepts.
func minimalVectorSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
	}, nil)
}

// titleVectorSchema adds a text column for hybrid indexing tests.
func titleVectorSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "title", Type: arrow.BinaryTypes.String},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
	}, nil)
}

// newHybridTestStore builds a store with hybrid search enabled for one column.
func newHybridTestStore(t *testing.T, textColumn string) *VectorStore {
	t.Helper()

	store, err := NewVectorStoreWithHybridConfig(arrowmemory.NewGoAllocator(), zerolog.Nop(), HybridSearchConfig{
		Enabled:     true,
		TextColumns: []string{textColumn},
		BM25:        index.DefaultBM25Config(),
		RRFk:        60,
	})
	require.NoError(t, err)
	return store
}

// makeTextRecord builds a record with one text cell per entry and a matching
// number of zero vectors.
func makeTextRecord(t *testing.T, texts []string) arrow.RecordBatch {
	t.Helper()
	return makeTextRecordWith(arrowmemory.NewGoAllocator(), texts)
}

// makeTextRecordWith is makeTextRecord with an explicit allocator, for tests
// that assert on Arrow buffer ownership.
func makeTextRecordWith(mem arrowmemory.Allocator, texts []string) arrow.RecordBatch {

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "title", Type: arrow.BinaryTypes.String},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
	}, nil)

	bld := array.NewRecordBuilder(mem, schema)
	defer bld.Release()

	strBld := bld.Field(0).(*array.StringBuilder)
	listBld := bld.Field(1).(*array.FixedSizeListBuilder)
	valBld := listBld.ValueBuilder().(*array.Float32Builder)
	for _, s := range texts {
		strBld.Append(s)
		listBld.Append(true)
		for range 4 {
			valBld.Append(0)
		}
	}

	return bld.NewRecordBatch()
}

// makeCountingRecord builds a record with exactly rows rows of float32 vectors.
// The values are irrelevant; only the row count matters to the batch-list
// bookkeeping under test.
func makeCountingRecord(t *testing.T, rows int64) arrow.RecordBatch {
	t.Helper()

	const dim = 4
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(dim, arrow.PrimitiveTypes.Float32)},
	}, nil)

	bld := array.NewRecordBuilder(arrowmemory.NewGoAllocator(), schema)
	defer bld.Release()

	listBld := bld.Field(0).(*array.FixedSizeListBuilder)
	valBld := listBld.ValueBuilder().(*array.Float32Builder)
	for range rows {
		listBld.Append(true)
		for range dim {
			valBld.Append(0)
		}
	}

	return bld.NewRecordBatch()
}
