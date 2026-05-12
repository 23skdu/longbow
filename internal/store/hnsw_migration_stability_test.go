package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrationStability(t *testing.T) {
	// Create a dataset with AutoShardingIndex
	config := DefaultAutoShardingConfig()
	config.ShardThreshold = 10
	
	// Define schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	ds := NewDataset("test_migration", schema)
	idx := NewAutoShardingIndex(ds, config)
	asIdx := idx.(*AutoShardingIndex)
	ds.Index = asIdx

	dim := 128
	ctx := context.Background()

	// Create dummy vectors
	pool := memory.NewGoAllocator()
	builder := array.NewFixedSizeListBuilder(pool, 128, arrow.PrimitiveTypes.Float32)
	defer builder.Release()
	
	valBuilder := builder.ValueBuilder().(*array.Float32Builder)
	for i := 0; i < 50; i++ {
		builder.Append(true)
		for j := 0; j < 128; j++ {
			valBuilder.Append(float32(i + j))
		}
	}
	vecArr := builder.NewArray()
	defer vecArr.Release()
	
	// Correct order: NewRecordBatch(schema, columns, numRows)
	rec := array.NewRecordBatch(schema, []arrow.Array{vecArr}, 50)
	defer rec.Release()
	
	// Add record to dataset safely
	oldRecords := ds.Records.Read()
	newRecords := append(oldRecords, rec)
	ds.Records.Update(newRecords)

	// Ingest some data to the monolithic index
	for i := 0; i < 50; i++ {
		_, err := asIdx.AddByRecord(ctx, rec, i, 0)
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	
	// Start concurrent searches
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				q := make([]float32, dim)
				_, _ = asIdx.SearchVectors(ctx, q, 5, nil, nil)
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	// Trigger migration
	done := make(chan struct{})
	go func() {
		asIdx.migrateToSharded()
		close(done)
	}()

	// Wait for migration with timeout
	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("Migration timed out")
	}

	close(stop)
	wg.Wait()

	// Verify index is sharded
	assert.True(t, asIdx.IsSharded(), "Index should be sharded after migration")
}
