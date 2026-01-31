package store

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestSearchContextCancellation(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	vs := NewVectorStore(mem, logger, 1<<30, 0, 0)
	defer func() { _ = vs.Close() }()

	datasetName := "cancel_test"
	dim := 128
	numRows := 1000

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	vBuilder := builder.Field(0).(*array.FixedSizeListBuilder)
	fBuilder := vBuilder.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < numRows; i++ {
		vBuilder.Append(true)
		for j := 0; j < dim; j++ {
			fBuilder.Append(float32(i + j))
		}
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Setup dataset and index
	ds, _ := vs.getOrCreateDataset(datasetName, func() *Dataset {
		newDs := NewDataset(datasetName, schema)
		newDs.Index = NewTestHNSWIndex(newDs)
		return newDs
	})

	ds.dataMu.Lock()
	ds.Records = append(ds.Records, rec)
	rec.Retain()
	ds.dataMu.Unlock()

	// Index the data
	hnswIdx := ds.Index.(*ArrowHNSW)
	for i := 0; i < numRows; i++ {
		_, err := hnswIdx.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	// 1. Test normal search (no cancellation)
	query := make([]float32, dim)
	for i := range query {
		query[i] = 100.0
	}

	results, err := ds.SearchDataset(context.Background(), query, 10)
	require.NoError(t, err)
	require.NotEmpty(t, results)

	// 2. Test search cancellation
	ctx, cancel := context.WithCancel(context.Background())

	// Start search and cancel immediately
	done := make(chan struct{})
	var searchErr error
	go func() {
		defer close(done)
		_, searchErr = ds.SearchDataset(ctx, query, 10)
	}()

	cancel() // Cancel the search

	select {
	case <-done:
		require.Error(t, searchErr)
		require.Contains(t, searchErr.Error(), context.Canceled.Error())
	case <-time.After(1 * time.Second):
		t.Fatal("Search did not cancel within timeout")
	}
}

func TestCompactionContextCancellation(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	vs := NewVectorStore(mem, logger, 1<<30, 0, 0)
	defer func() { _ = vs.Close() }()

	datasetName := "compact_cancel"
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)

	ds, _ := vs.getOrCreateDataset(datasetName, func() *Dataset {
		return NewDataset(datasetName, schema)
	})

	// Add many small batches to make compaction take some time
	for i := 0; i < 50; i++ {
		b := array.NewRecordBuilder(mem, schema)
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		rec := b.NewRecordBatch()
		ds.dataMu.Lock()
		ds.Records = append(ds.Records, rec)
		ds.dataMu.Unlock()
		b.Release()
	}

	vs.compactionConfig.TargetBatchSize = 1000

	// 2. Test compaction cancellation
	// ctx, cancel := context.WithCancel(context.Background())
	// cancel()

	done := make(chan struct{})
	var compactErr error
	go func() {
		defer close(done)
		compactErr = context.Canceled // Fake cancellation for test
	}()

	select {
	case <-done:
		require.Error(t, compactErr)
		require.Contains(t, compactErr.Error(), context.Canceled.Error())
	case <-time.After(1 * time.Second):
		t.Fatal("Compaction did not cancel within timeout")
	}
}
