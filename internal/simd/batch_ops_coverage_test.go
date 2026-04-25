package simd

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchOperations_Extended(t *testing.T) {
	t.Run("ToFloat32", func(t *testing.T) {
		v64 := []float64{1.1, 2.2, 3.3}
		v32 := ToFloat32(v64)
		assert.Equal(t, 3, len(v32))
		assert.InDelta(t, 1.1, v32[0], 1e-6)
	})

	t.Run("EuclideanDistanceVerticalBatch", func(t *testing.T) {
		query := []float32{1.0, 2.0, 3.0} // dim=3, not 128/384
		vectors := [][]float32{
			{1.0, 2.0, 3.0},
			{4.0, 5.0, 6.0},
		}
		results := make([]float32, 2)
		err := EuclideanDistanceVerticalBatch(query, vectors, results)
		require.NoError(t, err)
		assert.Equal(t, float32(0.0), results[0])
		assert.Greater(t, results[1], float32(0.0))
		
		// Error case
		err = EuclideanDistanceVerticalBatch(query, vectors, make([]float32, 1))
		assert.Error(t, err)
	})

	t.Run("EuclideanDistanceF16Batch", func(t *testing.T) {
		q := []float16.Num{float16.New(1.0), float16.New(2.0)}
		v := [][]float16.Num{{float16.New(1.0), float16.New(2.0)}}
		res := make([]float32, 1)
		err := EuclideanDistanceF16Batch(q, v, res)
		require.NoError(t, err)
		assert.Equal(t, float32(0.0), res[0])
	})

	t.Run("EuclideanDistanceSQ8Batch", func(t *testing.T) {
		q := []byte{100, 100}
		v := [][]byte{{100, 100}}
		res := make([]float32, 1)
		err := EuclideanDistanceSQ8Batch(q, v, res)
		require.NoError(t, err)
		assert.Equal(t, float32(0.0), res[0])
	})

	t.Run("ADCDistanceBatch", func(t *testing.T) {
		m := 2
		table := make([]float32, m*256)
		table[0*256+10] = 0.5
		table[1*256+20] = 0.5
		
		codes := []byte{10, 20}
		res := make([]float32, 1)
		err := ADCDistanceBatch(table, codes, m, res)
		require.NoError(t, err)
		assert.InDelta(t, 1.0, res[0], 1e-6) // sqrt(0.5+0.5) = 1.0
		
		// Error cases
		assert.Error(t, ADCDistanceBatch(nil, codes, m, res))
		assert.Error(t, ADCDistanceBatch(table, codes, 0, res))
	})
}

func TestBatchOperations_CosineDot_Extended(t *testing.T) {
	query := []float32{1.0, 0.0}
	vectors := [][]float32{{1.0, 0.0}, {0.0, 1.0}}
	results := make([]float32, 2)

	t.Run("CosineDistanceBatch", func(t *testing.T) {
		err := CosineDistanceBatch(query, vectors, results)
		require.NoError(t, err)
		assert.InDelta(t, 0.0, results[0], 1e-6)
		assert.InDelta(t, 1.0, results[1], 1e-6)
		
		assert.Error(t, CosineDistanceBatch(query, vectors, make([]float32, 1)))
	})

	t.Run("DotProductBatch", func(t *testing.T) {
		err := DotProductBatch(query, vectors, results)
		require.NoError(t, err)
		assert.InDelta(t, 1.0, results[0], 1e-6)
		assert.InDelta(t, 0.0, results[1], 1e-6)
		
		assert.Error(t, DotProductBatch(query, vectors, make([]float32, 1)))
	})
}
