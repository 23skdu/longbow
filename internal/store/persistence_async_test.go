package store

import (
	"io"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestApplyDelta_AsyncIndexing(t *testing.T) {
	// Setup
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	// Create store with default config (10k queue size)
	// We don't need small queue for this verification.
	store := NewVectorStore(
		mem,
		zerolog.New(io.Discard),
		100*1024*1024,
		0,
		0,
	)
	defer func() { _ = store.Close() }()

	// Create a record batch
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)

	vb := b.Field(1).(*array.FixedSizeListBuilder)
	vb.ValueBuilder().(*array.Float32Builder).AppendValues(make([]float32, 128*3), nil)
	vb.AppendValues([]bool{true, true, true}) // Critical: Append validity for 3 rows

	rec := b.NewRecordBatch()
	defer rec.Release()

	// ApplyDelta should place jobs in queue and return immediately
	dsName := "async_test_ds"

	start := time.Now()
	err := store.ApplyDelta(dsName, rec, 1, time.Now().UnixNano())
	duration := time.Since(start)

	require.NoError(t, err)
	// It should be very fast (no indexing)
	require.Less(t, duration, 10*time.Millisecond, "ApplyDelta took too long, likely blocking")

	// Verify jobs are in queue
	// We might need to wait slightly for async machinery if any
	// But queue push is synchronous to channel/buffer.

	// Check PendingIndexJobs on dataset
	ds, ok := store.getDataset(dsName)
	require.True(t, ok)

	pending := ds.PendingIndexJobs.Load()
	require.Equal(t, int64(3), pending, "Should have 3 pending index jobs")

	// Check Global Queue Stats
	stats := store.indexQueue.Stats()
	require.Equal(t, uint64(1), stats.TotalSent, "Queue should have received 1 job (batched)")
}
