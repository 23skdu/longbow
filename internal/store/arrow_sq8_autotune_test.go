package store

import (
	"math/rand"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQ8AutoTuning(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(16, arrow.PrimitiveTypes.Float32)},
	}, nil)
	ds := NewDataset("test_sq8_autotune", schema)

	cfg := DefaultArrowHNSWConfig()
	cfg.M = 16
	cfg.EfConstruction = 50
	cfg.SQ8Enabled = true
	cfg.SQ8TrainingThreshold = 10 // Low threshold for testing

	idx := NewArrowHNSW(ds, cfg, nil)

	// 1. Insert 50 vectors (half threshold)
	// These should be buffered and NOT SQ8 encoded yet.
	n1 := 50
	vecs1 := make([][]float32, n1)
	builder := array.NewRecordBuilder(mem, schema)
	vecBuilder := builder.Field(0).(*array.FixedSizeListBuilder)
	floatBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < n1; i++ {
		vec := make([]float32, 16)
		for j := range vec {
			vec[j] = rng.Float32()*20 - 10
		}
		vecs1[i] = vec
		vecBuilder.Append(true)
		floatBuilder.AppendValues(vec, nil)
	}
	rec1 := builder.NewRecordBatch()
	ds.Records = append(ds.Records, rec1)
	rec1.Retain()

	ids1, err := idx.AddBatch([]arrow.RecordBatch{rec1}, makeRangeHelper(n1), make([]int, n1))
	require.NoError(t, err)

	// Verify Trained (Threshold 10 < 50)
	assert.True(t, idx.quantizer.IsTrained(), "Quantizer should be trained after 50 vectors (threshold 10)")

	// Verify SQ8 data is ENCODED
	data := idx.data.Load()
	sq8Chunk := data.LoadSQ8Chunk(chunkID(ids1[0]))
	// We expect NON-ZERO bytes now
	cOff := chunkOffset(ids1[0])
	dims := 16
	off := int(cOff) * dims
	firstVecSQ8 := (*sq8Chunk)[off : off+dims]
	allZero := true
	for _, b := range firstVecSQ8 {
		if b != 0 {
			allZero = false
			break
		}
	}
	assert.False(t, allZero, "Vector should be encoded (non-zero) after training and backfill")

	// 2. Insert 50 more vectors (reaches threshold 100)
	// This should trigger training and backfill.
	builder.Release() // Reset builder
	builder = array.NewRecordBuilder(mem, schema)
	vecBuilder = builder.Field(0).(*array.FixedSizeListBuilder)
	floatBuilder = vecBuilder.ValueBuilder().(*array.Float32Builder) // Re-cast

	n2 := 50
	vecs2 := make([][]float32, n2)
	for i := 0; i < n2; i++ {
		vec := make([]float32, 16)
		for j := range vec {
			vec[j] = rng.Float32()*20 - 10
		}
		vecs2[i] = vec
		vecBuilder.Append(true)
		floatBuilder.AppendValues(vec, nil)
	}
	rec2 := builder.NewRecordBatch()
	ds.Records = append(ds.Records, rec2)
	rec2.Retain()

	// Use batch helper for 1s
	batchIdxs := make([]int, n2)
	for k := range batchIdxs {
		batchIdxs[k] = 1
	}
	ids2, err := idx.AddBatch([]arrow.RecordBatch{rec2}, makeRangeHelper(n2), batchIdxs)
	require.NoError(t, err)

	// Verify Trained
	assert.True(t, idx.quantizer.IsTrained(), "Quantizer should be trained now")

	// Verify Backfill (First vector should now be encoded)
	firstVecSQ8 = (*sq8Chunk)[off : off+dims]
	allZero = true
	for _, b := range firstVecSQ8 {
		if b != 0 {
			allZero = false
			break
		}
	}
	assert.False(t, allZero, "Vector should be encoded (non-zero) after backfill")

	// Verify New Vectors Encoded
	cOff2 := chunkOffset(ids2[0])
	off2 := int(cOff2) * dims
	// ids2[0] might be in same chunk or next. 50+0 = 50. Same chunk.
	secondVecSQ8 := (*sq8Chunk)[off2 : off2+dims]
	allZero2 := true
	for _, b := range secondVecSQ8 {
		if b != 0 {
			allZero2 = false
			break
		}
	}
	assert.False(t, allZero2, "New vectors should be encoded immediately")
}
