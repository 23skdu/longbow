package store

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestShouldUsePipeline(t *testing.T) {
	assert.False(t, ShouldUsePipeline(999), "Should not use pipeline for < 1000")
	assert.True(t, ShouldUsePipeline(1000), "Should use pipeline for >= 1000")
	assert.True(t, ShouldUsePipeline(5000), "Should use pipeline for large batches")
}

func TestDoGetPipeline_ProcessRecords(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	// Create dummy records
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	numBatches := 5
	var records []arrow.RecordBatch
	for i := 0; i < numBatches; i++ {
		b := array.NewInt64Builder(mem)
		b.Append(int64(i))
		arr := b.NewArray()
		b.Release()
		rec := array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
		arr.Release()
		records = append(records, rec)
	}

	// Verify cleanup
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	pipeline := NewDoGetPipeline(2)
	ctx := context.Background()

	// Process
	outCh := pipeline.ProcessRecords(ctx, records, nil, nil, nil)

	// Consume
	count := 0
	for stage := range outCh {
		assert.NoError(t, stage.Err)
		assert.NotNil(t, stage.Record)
		assert.Equal(t, count, stage.BatchIdx)
		count++
	}

	assert.Equal(t, numBatches, count)
}
