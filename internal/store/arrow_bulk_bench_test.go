package store

import (
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkAddBatch_SQ8(b *testing.B) {
	benchmarkAddBatch(b, true)
}

func BenchmarkAddBatch_Float32(b *testing.B) {
	benchmarkAddBatch(b, false)
}

func benchmarkAddBatch(b *testing.B, sq8 bool) {
	dims := 128 // Smaller dims to focus on graph ops not just memory copy
	blockSize := 1000
	numBlocks := 5 // 5000 vectors per iteration

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	// Prepare data blocks
	batches := make([]arrow.RecordBatch, numBlocks)
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < numBlocks; i++ {
		bldr := array.NewFixedSizeListBuilder(pool, int32(dims), arrow.PrimitiveTypes.Float32)
		vb := bldr.ValueBuilder().(*array.Float32Builder)
		bldr.Reserve(blockSize)
		vb.Reserve(blockSize * dims)

		for j := 0; j < blockSize; j++ {
			bldr.Append(true)
			vec := make([]float32, dims)
			for k := 0; k < dims; k++ {
				vec[k] = rng.Float32()
			}
			vb.AppendValues(vec, nil)
		}
		arr := bldr.NewArray()
		batches[i] = array.NewRecordBatch(schema, []arrow.Array{arr}, int64(blockSize))
		bldr.Release()
		arr.Release()
	}
	defer func() {
		for _, batch := range batches {
			batch.Release()
		}
	}()

	ds := &Dataset{
		Name:    "bench_dataset",
		Schema:  schema,
		Records: batches, // Not strictly used by AddBatch logic but implicitly correct
	}

	config := DefaultArrowHNSWConfig()
	config.M = 16
	config.EfConstruction = 100
	config.SQ8Enabled = sq8
	config.SQ8TrainingThreshold = 500 // Should trigger at batch 0 or 1
	config.Dims = dims
	config.InitialCapacity = blockSize * numBlocks

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Re-create index to simulate fresh bulk load
		idx := NewArrowHNSW(ds, config, nil)
		b.StartTimer()

		// Bulk Load
		rowIdxs := make([]int, blockSize)
		batchIdxs := make([]int, blockSize)
		for k := 0; k < blockSize; k++ {
			rowIdxs[k] = k
			batchIdxs[k] = 0
		} // Mock batch idx

		for j := 0; j < numBlocks; j++ {
			// We use AddBatch
			// AddBatch signature: (recs []arrow.RecordBatch, rowIdxs, batchIdxs []int)
			// Wait, AddBatch takes SLICE of batches and SLICE of RowIdxs.
			// It assumes RowIdx corresponds to ONE of the batches?
			// The signature is: AddBatch(recs []arrow.RecordBatch, rowIdxs, batchIdxs []int)
			// rowIdxs is parallel to... what?
			// AddBatch usage usually implies flattening?

			// Actually `AddBatch` looks like:
			/*
				func (h *ArrowHNSW) AddBatch(recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error)
			*/
			// It iterates 0..n (len(rowIdxs)).
			// For each i, it pulls `batchIdxs[i]` and `rowIdxs[i]`.
			// So we can pass ALL batches, and indices pointing to them.

			// For this benchmark, let's call AddBatch per block for simplicity?
			// Or pass all blocks?
			// Passing all blocks is "True Bulk Load".

			// Fill indices
			// Wait, I am doing this inside the loop `for j`.
			// Let's do PER BLOCK inside `AddBatch`.
			// idx.AddBatch([]arrow.RecordBatch{batches[j]}, rowIdxs, batchIdxs)

			_, _ = idx.AddBatch([]arrow.RecordBatch{batches[j]}, rowIdxs, batchIdxs)
		}

		b.StopTimer()
		_ = idx.Close()
		b.StartTimer()
	}
}
