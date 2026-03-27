package store

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestStore_IPCEmptyRecords(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	s := NewVectorStore(mem, logger, 1024*1024*100, 0, 0)
	defer s.Close()

	ctx := context.Background()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String},
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	s.PrewarmDataset("test_empty", schema)

	// Test Case 1: Store nil record (should be handled by callers or guarded)
	// We test the internal flushPutBatch or the higher level DoPut if possible,
	// but here we just verify StoreRecordBatch doesn't panic on Nil.
	err := s.StoreRecordBatch(ctx, "test_empty", nil)
	assert.Error(t, err, "StoreRecordBatch should return error for nil record")

	// Test Case 2: Record with zero rows
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	rec := b.NewRecord()
	defer rec.Release()

	err = s.StoreRecordBatch(ctx, "test_empty", rec)
	assert.NoError(t, err, "Record with zero rows should be handled gracefully")
}

func TestStore_IPCReliability(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	s := NewVectorStore(mem, logger, 1024*1024*100, 0, 0)
	defer s.Close()

	ctx := context.Background()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String},
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	datasetName := "test_reliability"
	s.PrewarmDataset(datasetName, schema)

	// Search on empty dataset (SearchHybrid)
	query := make([]float32, 128)
	// SearchHybrid(ctx, name, query, textQuery, k, alpha, rrfK, graphAlpha, graphDepth)
	results, err := s.SearchHybrid(ctx, datasetName, query, "", 10, 1.0, 60, 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(results), "Search on empty dataset should return zero results")
}
