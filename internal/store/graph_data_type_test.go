package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGraphData_PolymorphicAllocation(t *testing.T) {
	types := []VectorDataType{
		VectorTypeInt8, VectorTypeUint8, VectorTypeInt16, VectorTypeUint16,
		VectorTypeInt32, VectorTypeUint32, VectorTypeInt64, VectorTypeUint64,
		VectorTypeFloat32, VectorTypeFloat64, VectorTypeComplex64, VectorTypeComplex128,
	}

	dims := 128
	capacity := 2048

	hnswConfig := DefaultArrowHNSWConfig()
	hnsw := NewArrowHNSW(nil, hnswConfig, nil)

	for _, dt := range types {
		t.Run(dt.String(), func(t *testing.T) {
			gd := NewGraphData(capacity, dims, false, false, 0, false, false, false, dt)
			assert.Equal(t, dt, gd.Type)

			// Ensure chunk works for all types
			gd = hnsw.ensureChunk(gd, 0, 0, dims)

			offset := gd.Vectors[0]
			assert.NotEqual(t, uint64(0), offset, "Vector offset should be allocated")

			// Verify retrieval doesn't panic and returns nil for wrong types
			switch dt {
			case VectorTypeInt8:
				assert.NotNil(t, gd.GetVectorsInt8Chunk(0))
				assert.Nil(t, gd.GetVectorsChunk(0))
			case VectorTypeInt16:
				assert.NotNil(t, gd.GetVectorsInt16Chunk(0))
			case VectorTypeFloat32:
				assert.NotNil(t, gd.GetVectorsChunk(0))
			case VectorTypeFloat64:
				assert.NotNil(t, gd.GetVectorsFloat64Chunk(0))
			case VectorTypeComplex128:
				assert.NotNil(t, gd.GetVectorsComplex128Chunk(0))
			}
		})
	}
}
