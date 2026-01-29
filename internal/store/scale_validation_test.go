package store_test

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestScale50k(t *testing.T) {
	// Skipping standard test run, use -run TestScale50k explicitly
	if testing.Short() {
		t.Skip("Skipping scale test in short mode")
	}

	dimensions := []int{128, 768, 1536}
	// Types unrolled in loop

	// Re-reading how Complex is handled in tests.
	// TestAddBatch_Bulk_Typed uses specialized logic.
	// We will implement specific runners for each type to handle builder differences.

	runScaleTest(t, 50000, dimensions)
}

func runScaleTest(t *testing.T, numVecs int, dims []int) {
	fmt.Printf("| Type | Dims | Ingestion (vec/s) | Search p50 (ms) | Search p99 (ms) |\n")
	fmt.Printf("|---|---|---|---|---|\n")

	for _, d := range dims {
		// Float32
		runType(t, "Float32", d, numVecs, func(b *array.RecordBuilder, vBuilder array.Builder, rng *rand.Rand) any {
			fb := vBuilder.(*array.Float32Builder)
			vec := make([]float32, d)
			for i := range vec {
				vec[i] = rng.Float32()
			}
			fb.AppendValues(vec, nil)
			return vec
		}, arrow.PrimitiveTypes.Float32)

		// Float64
		runType(t, "Float64", d, numVecs, func(b *array.RecordBuilder, vBuilder array.Builder, rng *rand.Rand) any {
			fb := vBuilder.(*array.Float64Builder)
			vec := make([]float64, d)
			for i := range vec {
				vec[i] = rng.Float64()
			}
			fb.AppendValues(vec, nil)
			return vec
		}, arrow.PrimitiveTypes.Float64)

		// Int8
		runType(t, "Int8", d, numVecs, func(b *array.RecordBuilder, vBuilder array.Builder, rng *rand.Rand) any {
			ib := vBuilder.(*array.Int8Builder)
			vec := make([]int8, d)
			for i := range vec {
				vec[i] = int8(rng.Intn(256) - 128)
			}
			ib.AppendValues(vec, nil)
			return vec
		}, arrow.PrimitiveTypes.Int8)

		// Complex64 (Stored as Float32 x 2)
		runType(t, "Complex64", d, numVecs, func(b *array.RecordBuilder, vBuilder array.Builder, rng *rand.Rand) any {
			// Complex64 is stored as 2x Float32 elements in Arrow usually for this project's mapping
			fb := vBuilder.(*array.Float32Builder)
			vec := make([]float32, d*2)
			cVec := make([]complex64, d)
			for i := range cVec {
				r := rng.Float32()
				im := rng.Float32()
				cVec[i] = complex(r, im)
				vec[i*2] = r
				vec[i*2+1] = im
			}
			fb.AppendValues(vec, nil)
			return cVec
		}, arrow.PrimitiveTypes.Float32, true) // true = double dim for validation? Actually builder handles it
	}
}

func runType(t *testing.T, typeName string, dim, numVecs int, genFunc func(*array.RecordBuilder, array.Builder, *rand.Rand) any, arrowType arrow.DataType, isComplex ...bool) {
	mem := memory.NewGoAllocator()

	buildDim := dim
	if len(isComplex) > 0 && isComplex[0] {
		buildDim = dim * 2
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(buildDim), arrowType)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	listBuilder := builder.Field(0).(*array.FixedSizeListBuilder)
	valBuilder := listBuilder.ValueBuilder()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	var queryVec any

	// start unused removed

	// Batch size 1000
	batchSize := 1000
	batches := numVecs / batchSize

	batchesArr := make([]arrow.RecordBatch, 0, batches)

	for i := 0; i < batches; i++ {
		for j := 0; j < batchSize; j++ {
			listBuilder.Append(true)
			v := genFunc(builder, valBuilder, rng)
			if i == 0 && j == 0 {
				queryVec = v
			}
		}
		rec := builder.NewRecordBatch()
		batchesArr = append(batchesArr, rec)
	}

	// Ingestion
	cfg := store.DefaultArrowHNSWConfig()
	cfg.Dims = dim // Logical dims

	// Set correct DataType based on arrowType
	switch arrowType {
	case arrow.PrimitiveTypes.Float32:
		if len(isComplex) > 0 && isComplex[0] {
			cfg.DataType = store.VectorTypeComplex64 // Mapped from Float32
		} else {
			cfg.DataType = store.VectorTypeFloat32
		}
	case arrow.PrimitiveTypes.Float64:
		cfg.DataType = store.VectorTypeFloat64
	case arrow.PrimitiveTypes.Int8:
		cfg.DataType = store.VectorTypeInt8
	}

	ds := store.NewDataset("bench_"+typeName, schema)
	idx := store.NewArrowHNSW(ds, &cfg)

	ingestStart := time.Now()

	// Insert batches
	// We need addBatch wrapper
	// Insert batches
	// AddBatch expects rowIdxs and batchIdxs
	for _, rec := range batchesArr {
		count := int(rec.NumRows())
		rowIdxs := make([]int, count)
		batchIdxs := make([]int, count)

		for k := 0; k < count; k++ {
			rowIdxs[k] = k
			batchIdxs[k] = 0 // We pass only 1 batch in the slice
		}

		ds.Records = append(ds.Records, rec)
		rec.Retain()

		_, err := idx.AddBatch(context.Background(), []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
		require.NoError(t, err)
		rec.Release()
	}

	ingestDuration := time.Since(ingestStart)
	ingestRate := float64(numVecs) / ingestDuration.Seconds()

	// Search
	// Warmup
	_, _ = idx.Search(context.Background(), queryVec, 10, nil)

	// Measure
	numQueries := 100
	latencies := make([]float64, 0, numQueries)

	for i := 0; i < numQueries; i++ {
		t0 := time.Now()
		_, err := idx.Search(context.Background(), queryVec, 10, nil)
		require.NoError(t, err)
		latencies = append(latencies, float64(time.Since(t0).Milliseconds()))
	}

	sort.Float64s(latencies)
	p50 := latencies[len(latencies)/2]
	p99 := latencies[int(float64(len(latencies))*0.99)]

	fmt.Printf("| %s | %d | %.0f | %.2f | %.2f |\n", typeName, dim, ingestRate, p50, p99)
}
