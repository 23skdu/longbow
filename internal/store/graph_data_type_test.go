package store

import (
	"testing"

	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/assert"
)

func TestGraphData_PolymorphicAllocation(t *testing.T) {
	types := []VectorDataType{
		lbtypes.VectorTypeInt8, lbtypes.VectorTypeUint8, lbtypes.VectorTypeInt16, lbtypes.VectorTypeUint16,
		lbtypes.VectorTypeInt32, lbtypes.VectorTypeUint32, lbtypes.VectorTypeInt64, lbtypes.VectorTypeUint64,
		lbtypes.VectorTypeFloat32, lbtypes.VectorTypeFloat64, lbtypes.VectorTypeComplex64, lbtypes.VectorTypeComplex128,
	}

	dims := 128
	capacity := 2048

	hnswConfig := DefaultArrowHNSWConfig()
	hnsw := NewArrowHNSW(nil, hnswConfig)

	for _, dt := range types {
		t.Run(dt.String(), func(t *testing.T) {
			gd := lbtypes.NewGraphData(capacity, dims, false, false, 0, false, false, false, dt)
			assert.Equal(t, dt, gd.Type)

			// Ensure chunk works for all types
			gd, _ = hnsw.ensureChunk(gd, 0, 0, dims)

			offset := gd.Vectors[0]
			assert.NotEqual(t, uint64(0), offset, "Vector offset should be allocated")

			// Verify retrieval doesn't panic and returns nil for wrong types
			switch dt {
			case lbtypes.VectorTypeInt8:
				assert.NotNil(t, gd.GetVectorsInt8Chunk(0))
				assert.Nil(t, gd.GetVectorsChunk(0))
			case lbtypes.VectorTypeInt16:
				assert.NotNil(t, gd.GetVectorsInt16Chunk(0))
			case lbtypes.VectorTypeFloat32:
				assert.NotNil(t, gd.GetVectorsChunk(0))
			case lbtypes.VectorTypeFloat64:
				assert.NotNil(t, gd.GetVectorsFloat64Chunk(0))
			case lbtypes.VectorTypeComplex128:
				assert.NotNil(t, gd.GetVectorsComplex128Chunk(0))
			}
		})
	}
}
