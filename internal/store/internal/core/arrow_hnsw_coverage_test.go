package core

import (
	"sync/atomic"
	"testing"

	types "github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowHNSW_Coverage_ExtractVectorByIDForParallel(t *testing.T) {
	dims := 4
	config := types.DefaultArrowHNSWConfig()
	config.Dims = dims
	h := NewArrowHNSW(nil, &config)

	testCases := []struct {
		name       string
		dataType   types.VectorDataType
		vectorDims int
		vector     any
		expected   []float32
	}{
		{
			name:       "Float32",
			dataType:   types.VectorTypeFloat32,
			vectorDims: 4,
			vector:     []float32{1.0, 2.0, 3.0, 4.0},
			expected:   []float32{1.0, 2.0, 3.0, 4.0},
		},
		{
			name:       "Float64",
			dataType:   types.VectorTypeFloat64,
			vectorDims: 4,
			vector:     []float64{1.0, 2.0, 3.0, 4.0},
			expected:   []float32{1.0, 2.0, 3.0, 4.0},
		},
		{
			name:       "Float16",
			dataType:   types.VectorTypeFloat16,
			vectorDims: 4,
			vector: []float16.Num{
				float16.New(1.0), float16.New(2.0), float16.New(3.0), float16.New(4.0),
			},
			expected: []float32{1.0, 2.0, 3.0, 4.0},
		},
		{
			name:       "Int8",
			dataType:   types.VectorTypeInt8,
			vectorDims: 4,
			vector:     []int8{1, 2, 3, 4},
			expected:   []float32{1, 2, 3, 4},
		},
		{
			name:       "Uint8",
			dataType:   types.VectorTypeUint8,
			vectorDims: 4,
			vector:     []uint8{1, 2, 3, 4},
			expected:   []float32{1, 2, 3, 4},
		},
		{
			name:       "Complex64",
			dataType:   types.VectorTypeComplex64,
			vectorDims: 2,
			vector:     []complex64{complex(1.0, 2.0), complex(3.0, 4.0)},
			expected:   []float32{1.0, 2.0, 3.0, 4.0},
		},
		{
			name:       "Complex128",
			dataType:   types.VectorTypeComplex128,
			vectorDims: 2,
			vector:     []complex128{complex(1.0, 2.0), complex(3.0, 4.0)},
			expected:   []float32{1.0, 2.0, 3.0, 4.0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h = NewArrowHNSW(nil, &config)
			h.dims.Store(int32(tc.vectorDims))

			gd := types.NewGraphData(10, tc.vectorDims, false, false, 0, false, false, false, tc.dataType, false, false, false, 8)
			atomic.StoreUint32(&gd.SQ8Ready, 1) // For Int8/Uint8 GetVector
			h.data.Store(gd)

			err := gd.EnsureChunk(0, 0, tc.vectorDims)
			require.NoError(t, err)

			err = gd.SetVector(0, tc.vector)
			require.NoError(t, err)

			res, err := h.ExtractVectorByIDForParallel(0)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, res)
		})
	}
}

func TestArrowHNSW_Coverage_ComputeSingle(t *testing.T) {
	config := types.DefaultArrowHNSWConfig()

	testCases := []struct {
		name     string
		dataType types.VectorDataType
		dims     int
		query    []float32
		vector   any
		expected float32
	}{
		{
			name:     "Float32",
			dataType: types.VectorTypeFloat32,
			dims:     4,
			query:    []float32{1.0, 1.0, 1.0, 1.0},
			vector:   []float32{1.0, 1.0, 1.0, 1.0},
			expected: 0.0,
		},
		{
			name:     "Complex64",
			dataType: types.VectorTypeComplex64,
			dims:     2,
			query:    []float32{1.0, 0.0, 1.0, 0.0},
			vector:   []complex64{complex(1.0, 0.0), complex(1.0, 0.0)},
			expected: 0.0,
		},
		{
			name:     "Complex128",
			dataType: types.VectorTypeComplex128,
			dims:     2,
			query:    []float32{1.0, 0.0, 1.0, 0.0},
			vector:   []complex128{complex(1.0, 0.0), complex(1.0, 0.0)},
			expected: 0.0,
		},
		{
			name:     "Float64",
			dataType: types.VectorTypeFloat64,
			dims:     4,
			query:    []float32{1.0, 1.0, 1.0, 1.0},
			vector:   []float64{1.0, 1.0, 1.0, 1.0},
			expected: 0.0,
		},
		{
			name:     "Int8",
			dataType: types.VectorTypeInt8,
			dims:     4,
			query:    []float32{1.0, 2.0, 3.0, 4.0},
			vector:   []int8{1, 2, 3, 4},
			expected: 0.0,
		},
		{
			name:     "Uint8",
			dataType: types.VectorTypeUint8,
			dims:     4,
			query:    []float32{1.0, 2.0, 3.0, 4.0},
			vector:   []uint8{1, 2, 3, 4},
			expected: 0.0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewArrowHNSW(nil, &config)
			h.dims.Store(int32(tc.dims))
			gd := types.NewGraphData(10, tc.dims, false, false, 0, false, false, false, tc.dataType, false, false, false, 8)
			atomic.StoreUint32(&gd.SQ8Ready, 1)
			h.data.Store(gd)

			err := gd.EnsureChunk(0, 0, tc.dims)
			require.NoError(t, err)

			err = gd.SetVector(0, tc.vector)
			require.NoError(t, err)

			computer := &float32Computer{
				h:    h,
				q:    tc.query,
				data: gd,
			}

			dist, err := computer.ComputeSingle(0)
			assert.NoError(t, err)
			assert.InDelta(t, tc.expected, dist, 1e-5)
		})
	}
}

func TestComplexComputers(t *testing.T) {
	dims := 2
	config := types.DefaultArrowHNSWConfig()
	config.Dims = dims
	h := NewArrowHNSW(nil, &config)
	gd := types.NewGraphData(10, dims, false, false, 0, false, false, false, types.VectorTypeComplex64, false, false, false, 8)
	h.data.Store(gd)

	err := gd.EnsureChunk(0, 0, dims)
	require.NoError(t, err)
	vec64 := []complex64{complex(1, 2), complex(3, 4)}
	err = gd.SetVector(0, vec64)
	require.NoError(t, err)

	c64Comp := &complex64Computer{
		data: gd,
		q:    []complex64{complex(1, 2), complex(3, 4)},
		dims: 2,
		h:    h,
	}
	dist, err := c64Comp.ComputeSingle(0)
	assert.NoError(t, err)
	assert.Equal(t, float32(0), dist)

	dists := make([]float32, 1)
	err = c64Comp.Compute([]uint32{0}, dists)
	assert.NoError(t, err)
	assert.Equal(t, float32(0), dists[0])

	gd128 := types.NewGraphData(10, dims, false, false, 0, false, false, false, types.VectorTypeComplex128, false, false, false, 8)
	h.data.Store(gd128)
	err = gd128.EnsureChunk(0, 0, dims)
	require.NoError(t, err)
	vec128 := []complex128{complex(1, 2), complex(3, 4)}
	err = gd128.SetVector(1, vec128)
	require.NoError(t, err)

	c128Comp := &complex128Computer{
		data: gd128,
		q:    []complex128{complex(1, 2), complex(3, 4)},
		dims: 2,
		h:    h,
	}
	dist, err = c128Comp.ComputeSingle(1)
	assert.NoError(t, err)
	assert.Equal(t, float32(0), dist)

	dists = make([]float32, 1)
	err = c128Comp.Compute([]uint32{1}, dists)
	assert.NoError(t, err)
	assert.Equal(t, float32(0), dists[0])
}

func TestMoreComputers(t *testing.T) {
	dims := 4
	config := types.DefaultArrowHNSWConfig()
	config.Dims = dims
	h := NewArrowHNSW(nil, &config)
	gd := types.NewGraphData(10, dims, false, false, 0, false, false, false, types.VectorTypeFloat64, false, false, false, 8)
	atomic.StoreUint32(&gd.SQ8Ready, 1)
	h.data.Store(gd)

	// float64Computer
	err := gd.EnsureChunk(0, 0, dims)
	require.NoError(t, err)
	vec64 := []float64{1.0, 2.0, 3.0, 4.0}
	err = gd.SetVector(0, vec64)
	require.NoError(t, err)

	f64Comp := &float64Computer{
		data: gd,
		q:    []float64{1.0, 2.0, 3.0, 4.0},
		dims: 4,
		h:    h,
	}
	dist, err := f64Comp.ComputeSingle(0)
	assert.NoError(t, err)
	assert.Equal(t, float32(0), dist)

	dists := make([]float32, 1)
	err = f64Comp.Compute([]uint32{0}, dists)
	assert.NoError(t, err)
	assert.Equal(t, float32(0), dists[0])

	// int8Computer
	gdInt8 := types.NewGraphData(10, dims, false, false, 0, false, false, false, types.VectorTypeInt8, false, false, false, 8)
	atomic.StoreUint32(&gdInt8.SQ8Ready, 1)
	h.data.Store(gdInt8)
	err = gdInt8.EnsureChunk(0, 0, dims)
	require.NoError(t, err)
	vecInt8 := []int8{1, 2, 3, 4}
	err = gdInt8.SetVector(0, vecInt8)
	require.NoError(t, err)

	i8Comp := &int8Computer{
		h:     h,
		data:  gdInt8,
		q:     []uint8{1, 2, 3, 4},
		qInt8: []int8{1, 2, 3, 4},
		dims:  4,
	}
	dist, err = i8Comp.ComputeSingle(0)
	assert.NoError(t, err)
	assert.Equal(t, float32(0), dist)

	// int8Computer with Float32 vector
	gdF32 := types.NewGraphData(10, dims, false, false, 0, false, false, false, types.VectorTypeFloat32, false, false, false, 8)
	h.data.Store(gdF32)
	err = gdF32.EnsureChunk(0, 0, dims)
	require.NoError(t, err)
	vecF32 := []float32{1.0, 2.0, 3.0, 4.0}
	err = gdF32.SetVector(0, vecF32)
	require.NoError(t, err)
	dist, err = i8Comp.ComputeSingle(0)
	assert.NoError(t, err)
	assert.Equal(t, float32(0), dist)
}

func TestSQ8Computers(t *testing.T) {
	dims := 16
	config := types.DefaultArrowHNSWConfig()
	config.Dims = dims
	config.SQ8Enabled = true
	h := NewArrowHNSW(nil, &config)

	h.quantizer = NewScalarQuantizerFromParams(dims, 0.0, 1.0)
	h.sq8Ready.Store(true)

	gd := types.NewGraphData(10, dims, true, false, 0, false, false, false, types.VectorTypeFloat32, false, false, false, 8)
	gd.SQ8Enabled = true
	atomic.StoreUint32(&gd.SQ8Ready, 1)
	h.data.Store(gd)

	err := gd.EnsureChunk(0, 0, dims)
	require.NoError(t, err)

	// Create a quantized vector (SQ8 stores in VectorsSQ8 as []byte)
	// byte value 127 should be roughly 0.5 with min=0, max=1
	qvec := make([]byte, dims)
	for i := range qvec {
		qvec[i] = 127
	}
	err = gd.SetVector(0, qvec)
	require.NoError(t, err)

	// test float32Computer SQ8 path
	f32Comp := &float32Computer{
		h:    h,
		data: gd,
		q:    make([]float32, dims),
	}
	for i := range f32Comp.q {
		f32Comp.q[i] = 0.5
	}
	dist, err := f32Comp.ComputeSingle(0)
	assert.NoError(t, err)
	// With 127/255.0, it should be very close to 0.5
	assert.InDelta(t, 0.0, dist, 0.1)

	// test int8Computer SQ8 path
	i8Comp := &int8Computer{
		h:    h,
		data: gd,
		q:    make([]uint8, dims),
	}
	for i := range i8Comp.q {
		i8Comp.q[i] = 127
	}
	dist, err = i8Comp.ComputeSingle(0)
	assert.NoError(t, err)
	assert.InDelta(t, 0.0, dist, 0.1)

	// test ExtractVectorByIDForParallel SQ8 path
	res, err := h.ExtractVectorByIDForParallel(0)
	assert.NoError(t, err)
	assert.Len(t, res, dims)
	assert.InDelta(t, 0.5, res[0], 0.1)
}
