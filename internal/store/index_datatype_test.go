package store

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestArrowHNSW_DataTypes runs a comprehensive verify for all supported vector data types
func TestArrowHNSW_DataTypes(t *testing.T) {
	mem := memory.NewGoAllocator()
	dims := 128
	count := 100 // Sufficient for validation

	tests := []struct {
		name       string
		dataType   VectorDataType
		dims       int
		makeRecord func(count, dims int) (arrow.RecordBatch, [][]float32)
	}{
		{
			name:     "Float32",
			dataType: VectorTypeFloat32,
			dims:     dims,
			makeRecord: func(count, dims int) (arrow.RecordBatch, [][]float32) {
				return makeTestRecordFloat32(mem, count, dims)
			},
		},
		{
			name:     "Float64",
			dataType: VectorTypeFloat64,
			dims:     dims,
			makeRecord: func(count, dims int) (arrow.RecordBatch, [][]float32) {
				// t.Skip can't be used here easily, but we can do it in the loop
				return makeTestRecordFloat64(mem, count, dims)
			},
		},
		{
			name:     "Float16",
			dataType: VectorTypeFloat16,
			dims:     dims,
			makeRecord: func(count, dims int) (arrow.RecordBatch, [][]float32) {
				return makeTestRecordFloat16(mem, count, dims)
			},
		},
		{
			name:     "Complex128",
			dataType: VectorTypeComplex128,
			dims:     dims, // Logical dims
			makeRecord: func(count, dims int) (arrow.RecordBatch, [][]float32) {
				return makeTestRecordComplex128(mem, count, dims)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// 1. Setup
			rec, originalVecs := tt.makeRecord(count, tt.dims)
			defer rec.Release()

			ds := &Dataset{
				Name:    fmt.Sprintf("test_dataset_%s", tt.name),
				Records: []arrow.RecordBatch{rec},
				dataMu:  sync.RWMutex{},
			}

			// Configure Index
			config := DefaultArrowHNSWConfig()
			config.Dims = tt.dims
			config.DataType = tt.dataType
			config.M = 16
			config.EfConstruction = 64

			// For Float16 type, we must enable it explicitly
			if tt.dataType == VectorTypeFloat16 {
				config.Float16Enabled = true
			}

			locStore := NewChunkedLocationStore()
			idx := NewArrowHNSW(ds, config, locStore)
			defer func() { _ = idx.Close() }()

			// 2. Ingestion (AddBatch)
			// rowIdxs: 0..count-1
			// batchIdxs: all 0
			rowIdxs := make([]int, count)
			batchIdxs := make([]int, count)
			for i := 0; i < count; i++ {
				rowIdxs[i] = i
				batchIdxs[i] = 0
			}

			ids, err := idx.AddBatch([]arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
			require.NoError(t, err, "AddBatch failed")
			assert.Equal(t, count, len(ids))
			assert.Equal(t, count, idx.Len())

			// 3. Search Verification
			// We search for the first vector. Ideally it finds itself as top result.
			queryVec := originalVecs[0] // originalVecs has float32 version of logical vector

			// NOTE: For Complex128, originalVecs contains the FLATTENED float32 representation (2*dims)
			// queryVec should be passed as is involved in distance calc.

			res, err := idx.Search(context.Background(), queryVec, 10, 50, nil)
			require.NoError(t, err, "Search failed")
			require.NotEmpty(t, res, "Search returned 0 results")

			// Check first result
			assert.Equal(t, VectorID(ids[0]), VectorID(res[0].ID), "Top result should be self")

			// Thresholds: Float32 (1e-5), Float16 (0.1), Others (1e-4)
			threshold := float32(1e-4)
			if tt.dataType == VectorTypeFloat16 {
				threshold = 0.5 // Relaxed for F16
			}
			assert.Less(t, res[0].Score, threshold, "Distance to self should be small")

			// 4. Verify Dimension Metadata (if applicable)
			assert.Equal(t, uint32(tt.dims), idx.GetDimension())
		})
	}
}

// Helpers

func generateRandomVec(dims int) []float32 {
	v := make([]float32, dims)
	for i := 0; i < dims; i++ {
		v[i] = rand.Float32()
	}
	return v
}

func makeTestRecordFloat32(mem memory.Allocator, count, dims int) (rec arrow.RecordBatch, floatVecs [][]float32) {
	vecs := make([][]float32, count)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	listB := b.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < count; i++ {
		v := generateRandomVec(dims)
		vecs[i] = v
		listB.Append(true)
		valB.AppendValues(v, nil)
	}

	return b.NewRecordBatch(), vecs
}

func makeTestRecordFloat64(mem memory.Allocator, count, dims int) (rec arrow.RecordBatch, floatVecs [][]float32) {
	vecsFloat32 := make([][]float32, count)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float64)},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	listB := b.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float64Builder)

	for i := 0; i < count; i++ {
		v := generateRandomVec(dims)
		vecsFloat32[i] = v

		v64 := make([]float64, dims)
		for j := 0; j < dims; j++ {
			v64[j] = float64(v[j])
		}

		listB.Append(true)
		valB.AppendValues(v64, nil)
	}

	return b.NewRecordBatch(), vecsFloat32
}

func makeTestRecordFloat16(mem memory.Allocator, count, dims int) (rec arrow.RecordBatch, floatVecs [][]float32) {
	vecsFloat32 := make([][]float32, count)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.FixedWidthTypes.Float16)},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	listB := b.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float16Builder)

	for i := 0; i < count; i++ {
		v := generateRandomVec(dims)
		vecsFloat32[i] = v

		v16 := make([]float16.Num, dims)
		for j := 0; j < dims; j++ {
			v16[j] = float16.New(v[j])
		}

		listB.Append(true)
		valB.AppendValues(v16, nil)
	}

	return b.NewRecordBatch(), vecsFloat32
}

func makeTestRecordComplex128(mem memory.Allocator, count, dims int) (rec arrow.RecordBatch, floatVecs [][]float32) {
	// Complex128 logic in Longbow:
	// Logical dim = N (e.g. 128)
	// Physical storage = FixedSizeList<Float64> length 2*N
	// Metadata "longbow.complex": "true" (implied/used by inference, though here we explicitly set DataType in config)

	physicalDims := dims * 2
	vecsFloat32 := make([][]float32, count)

	// Create schema with metadata
	md := arrow.NewMetadata([]string{"longbow.vector_type"}, []string{"complex128"})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(physicalDims), arrow.PrimitiveTypes.Float64)},
	}, &md) // Pass pointer to md

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	listB := b.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float64Builder)

	for i := 0; i < count; i++ {
		// Generate 2*N floats (Real, Imag interleaved)
		v := generateRandomVec(physicalDims)
		vecsFloat32[i] = v // Use flattened representation for search verification

		v64 := make([]float64, physicalDims)
		for j := 0; j < physicalDims; j++ {
			v64[j] = float64(v[j])
		}

		listB.Append(true)
		valB.AppendValues(v64, nil)
	}

	return b.NewRecordBatch(), vecsFloat32
}
