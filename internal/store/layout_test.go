package store

import (
	"testing"

	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/assert"
)

func TestGraphData_CacheLinePadding(t *testing.T) {
	tests := []struct {
		name          string
		dims          int
		dataType      VectorDataType
		expectedBytes int // Expected size of one vector in bytes (padded)
	}{
		{
			name:          "128 dims float32 (512 bytes) - Aligned",
			dims:          128,
			dataType:      VectorTypeFloat32,
			expectedBytes: 512, // 128 * 4 = 512 (divisible by 64)
		},
		{
			name:          "129 dims float32 (516 bytes) - Unaligned",
			dims:          129,
			dataType:      VectorTypeFloat32,
			expectedBytes: 576, // 129 * 4 = 516. Next 64-byte boundary is 576 (512 + 64)
		},
		{
			name:          "384 dims float32 (1536 bytes) - Aligned",
			dims:          384,
			dataType:      VectorTypeFloat32,
			expectedBytes: 1536, // 384 * 4 = 1536 (24 * 64)
		},
		{
			name:          "10 dims int8 (10 bytes) - Unaligned",
			dims:          10,
			dataType:      VectorTypeInt8,
			expectedBytes: 64, // 10 * 1 = 10. Next 64 boundary is 64.
		},
		{
			name:          "3072 dims float32 (12288 bytes) - Aligned",
			dims:          3072,
			dataType:      VectorTypeFloat32,
			expectedBytes: 12288, // 3072 * 4 = 12288 (192 * 64)
		},
		{
			name:          "7 dims float16 (14 bytes) - Unaligned",
			dims:          7,
			dataType:      VectorTypeFloat16,
			expectedBytes: 64, // 7 * 2 = 14. Next 64 boundary is 64.
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gd := lbtypes.NewGraphData(100, tt.dims, false, false, 0, false, false, false, tt.dataType)

			// Verify PaddedDims
			elementSize := tt.dataType.ElementSize()
			expectedPaddedDims := tt.expectedBytes / elementSize

			assert.Equal(t, expectedPaddedDims, gd.GetPaddedDims(), "PaddedDims should match expected padded byte size")
			assert.Equal(t, tt.dims, gd.Dims, "Original dims should be preserved")

			// Verify actual byte alignment
			paddedBytes := gd.GetPaddedDims() * elementSize
			assert.Equal(t, 0, paddedBytes%64, "Vector size in bytes must be multiple of 64")
		})
	}
}
