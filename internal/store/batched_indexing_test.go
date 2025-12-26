package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/rs/zerolog"
)

func TestBatchedIndexing(t *testing.T) {
	pool := memory.NewGoAllocator()

	// 1. Create a dataset with HNSW index
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
			{Name: "text", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	ds := NewDataset("batched_test", schema)
	ds.Index = NewHNSWIndex(ds)

	store := &VectorStore{
		datasets:   make(map[string]*Dataset),
		indexQueue: NewIndexJobQueue(DefaultIndexJobQueueConfig()),
		logger:     zerolog.Nop(),
	}
	store.datasets[ds.Name] = ds

	// Start worker
	go store.runIndexWorker(pool)

	// 2. Add vectors in batches
	numVectors := 200
	ds.dataMu.Lock()
	ds.Records = make([]arrow.RecordBatch, numVectors)
	ds.dataMu.Unlock()

	for i := 0; i < numVectors; i++ {
		b := array.NewRecordBuilder(pool, schema)
		vBuilder := b.Field(0).(*array.FixedSizeListBuilder)
		vValuesBuilder := vBuilder.ValueBuilder().(*array.Float32Builder)
		tBuilder := b.Field(1).(*array.StringBuilder)

		vec := make([]float32, 128)
		vec[0] = float32(i)
		vBuilder.Append(true)
		vValuesBuilder.AppendValues(vec, nil)
		tBuilder.Append(fmt.Sprintf("text_%d", i))

		rec := b.NewRecordBatch()
		ds.dataMu.Lock()
		ds.Records[i] = rec
		ds.dataMu.Unlock()

		rec.Retain() // One for dataset, one for job
		store.indexQueue.Send(IndexJob{
			DatasetName: ds.Name,
			Record:      rec,
			BatchIdx:    i,
			CreatedAt:   time.Now(),
		})

		b.Release()
	}

	// Wait for processing
	time.Sleep(500 * time.Millisecond)

	// 3. Verify
	assert.Equal(t, numVectors, ds.Index.Len())

	// Check search
	query := make([]float32, 128)
	query[0] = 50.0
	results, err := ds.Index.SearchVectors(query, 5, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, results)
	assert.Equal(t, VectorID(50), results[0].ID)
}
