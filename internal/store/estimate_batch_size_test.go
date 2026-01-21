package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestEstimateBatchSize(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
	}, nil)

	bld := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bld.Release()

	idBld := bld.Field(0).(*array.Uint32Builder)
	vecBld := bld.Field(1).(*array.FixedSizeListBuilder)
	valBld := vecBld.ValueBuilder().(*array.Float32Builder)

	numRows := 1000
	for i := 0; i < numRows; i++ {
		idBld.Append(uint32(i))
		vecBld.Append(true)
		for j := 0; j < 128; j++ {
			valBld.Append(float32(j))
		}
	}

	rec := bld.NewRecord()
	defer rec.Release()

	size := estimateBatchSize(rec)

	// Expected size:
	// - id column: 1000 * 4 bytes = 4000 bytes (+ validity bitmap ~125 bytes)
	// - vector column: 1000 * 128 * 4 bytes = 512000 bytes (+ validity bitmap ~125 bytes)
	// Total should be around 516KB, not 76MB

	expectedMin := int64(512000) // At least the vector data
	expectedMax := int64(600000) // With overhead

	t.Logf("Estimated size: %d bytes (%.2f KB)", size, float64(size)/1024)
	assert.True(t, size >= expectedMin, "Size %d should be >= %d", size, expectedMin)
	assert.True(t, size <= expectedMax, "Size %d should be <= %d", size, expectedMax)
}
