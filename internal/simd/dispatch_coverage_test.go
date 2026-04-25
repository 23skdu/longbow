package simd

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/stretchr/testify/assert"
)

func TestDispatchDistance_AllTypes(t *testing.T) {
	// Initialize dispatch to ensure registry is populated
	initializeDispatch()

	t.Run("Float32", func(t *testing.T) {
		a := []float32{1.0, 2.0, 3.0}
		b := []float32{4.0, 5.0, 6.0}
		d, err := DispatchDistance(MetricEuclidean, a, b)
		assert.NoError(t, err)
		assert.Greater(t, d, float32(0))
	})

	t.Run("Float16", func(t *testing.T) {
		a := []float16.Num{float16.New(1.0), float16.New(2.0)}
		b := []float16.Num{float16.New(3.0), float16.New(4.0)}
		d, err := DispatchDistance(MetricEuclidean, a, b)
		assert.NoError(t, err)
		assert.Greater(t, d, float32(0))
	})

	t.Run("Int8", func(t *testing.T) {
		a := []int8{1, 2, 3}
		b := []int8{4, 5, 6}
		d, err := DispatchDistance(MetricEuclidean, a, b)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, d, float32(0))
	})

	t.Run("Uint8", func(t *testing.T) {
		a := []uint8{1, 2, 3}
		b := []uint8{4, 5, 6}
		d, err := DispatchDistance(MetricEuclidean, a, b)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, d, float32(0))
	})

	t.Run("Int16", func(t *testing.T) {
		a := []int16{1, 2, 3}
		b := []int16{4, 5, 6}
		d, err := DispatchDistance(MetricEuclidean, a, b)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, d, float32(0))
	})

	t.Run("Int32", func(t *testing.T) {
		a := []int32{1, 2, 3}
		b := []int32{4, 5, 6}
		d, err := DispatchDistance(MetricEuclidean, a, b)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, d, float32(0))
	})

	t.Run("Float64", func(t *testing.T) {
		a := []float64{1.0, 2.0}
		b := []float64{3.0, 4.0}
		d, err := DispatchDistance(MetricEuclidean, a, b)
		assert.NoError(t, err)
		assert.Greater(t, d, float32(0))
	})

	t.Run("Complex64", func(t *testing.T) {
		a := []complex64{complex(1, 1), complex(2, 2)}
		b := []complex64{complex(3, 3), complex(4, 4)}
		d, err := DispatchDistance(MetricEuclidean, a, b)
		assert.NoError(t, err)
		assert.Greater(t, d, float32(0))
	})
}

func TestDispatchDistance_Metrics(t *testing.T) {
	initializeDispatch()
	a := []float32{1.0, 0.0}
	b := []float32{0.0, 1.0}

	metrics := []MetricType{MetricEuclidean, MetricCosine, MetricDotProduct}
	for _, m := range metrics {
		t.Run(m.String(), func(t *testing.T) {
			d, err := DispatchDistance(m, a, b)
			assert.NoError(t, err)
			t.Logf("%s distance: %f", m, d)
		})
	}
}

func TestDispatchDistance_Errors(t *testing.T) {
	initializeDispatch()

	t.Run("DimensionMismatch", func(t *testing.T) {
		a := []float32{1.0, 2.0}
		b := []float32{1.0, 2.0, 3.0}
		_, err := DispatchDistance(MetricEuclidean, a, b)
		assert.Error(t, err)
	})

	t.Run("EmptyVectors", func(t *testing.T) {
		var a, b []float32
		d, err := DispatchDistance(MetricEuclidean, a, b)
		assert.NoError(t, err)
		assert.Equal(t, float32(0), d)
	})

	t.Run("NoKernel", func(t *testing.T) {
		// Try a combination that might not have a kernel if we didn't register it
		// (though we registered almost everything)
		// We can't easily trigger this without bypassing Registry.Register
	})
}

func TestGetSIMDDataType_Extended(t *testing.T) {
	assert.Equal(t, DataTypeFloat32, GetSIMDDataType[float32]())
	assert.Equal(t, DataTypeFloat16, GetSIMDDataType[float16.Num]())
	assert.Equal(t, DataTypeInt8, GetSIMDDataType[int8]())
	assert.Equal(t, DataTypeUint8, GetSIMDDataType[uint8]())
	assert.Equal(t, DataTypeInt16, GetSIMDDataType[int16]())
	assert.Equal(t, DataTypeUint16, GetSIMDDataType[uint16]())
	assert.Equal(t, DataTypeInt32, GetSIMDDataType[int32]())
	assert.Equal(t, DataTypeUint32, GetSIMDDataType[uint32]())
	assert.Equal(t, DataTypeInt64, GetSIMDDataType[int64]())
	assert.Equal(t, DataTypeUint64, GetSIMDDataType[uint64]())
	assert.Equal(t, DataTypeFloat64, GetSIMDDataType[float64]())
	assert.Equal(t, DataTypeComplex64, GetSIMDDataType[complex64]())
	assert.Equal(t, DataTypeComplex128, GetSIMDDataType[complex128]())
	
	type custom struct{}
	assert.Equal(t, DataTypeFloat32, GetSIMDDataType[custom](), "Should fallback to float32")
}

func TestRegistry_Specialized(t *testing.T) {
	initializeDispatch()
	
	// Test specialized dimensions (128, 384, 768, 1536)
	dims := []int{128, 384, 768, 1536}
	for _, d := range dims {
		t.Run(fmt.Sprintf("%d", d), func(t *testing.T) {
			a := make([]float32, d)
			b := make([]float32, d)
			_, err := DispatchDistance(MetricEuclidean, a, b)
			assert.NoError(t, err)
		})
	}
}
