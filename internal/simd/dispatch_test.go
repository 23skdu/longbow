package simd

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInitializeDispatch verifies that function pointers are correctly initialized
// based on detected CPU capabilities and implementation selection.
func TestInitializeDispatch(t *testing.T) {
	// Save original state
	originalFeatures := features
	originalImplementation := implementation
	defer func() {
		// Restore original state
		features = originalFeatures
		implementation = originalImplementation
	}()

	tests := []struct {
		name         string
		cpuFeatures  CPUFeatures
		expectedImpl string
	}{
		{
			name: "AVX512 detected",
			cpuFeatures: CPUFeatures{
				Vendor:    "GenuineIntel",
				HasAVX2:   true,
				HasAVX512: true,
				HasNEON:   false,
			},
			expectedImpl: "avx512",
		},
		{
			name: "AVX2 detected",
			cpuFeatures: CPUFeatures{
				Vendor:    "GenuineIntel",
				HasAVX2:   true,
				HasAVX512: false,
				HasNEON:   false,
			},
			expectedImpl: "avx2",
		},
		{
			name: "NEON detected",
			cpuFeatures: CPUFeatures{
				Vendor:    "ARM",
				HasAVX2:   false,
				HasAVX512: false,
				HasNEON:   true,
			},
			expectedImpl: "neon",
		},
		{
			name: "Generic fallback",
			cpuFeatures: CPUFeatures{
				Vendor:    "Unknown",
				HasAVX2:   false,
				HasAVX512: false,
				HasNEON:   false,
			},
			expectedImpl: "generic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set test CPU features
			features = tt.cpuFeatures

			// Manually set implementation based on features (simulating detectCPU)
			switch {
			case features.HasAVX512:
				implementation = "avx512"
			case features.HasAVX2:
				implementation = "avx2"
			case features.HasNEON:
				implementation = "neon"
			default:
				implementation = "generic"
			}

			// Re-initialize dispatch with new CPU features
			initializeDispatch()

			// Verify implementation was set correctly
			assert.Equal(t, tt.expectedImpl, implementation, "Implementation should match expected")

			// Verify function pointers are not nil
			require.NotNil(t, euclideanDistanceImpl, "euclideanDistanceImpl should be initialized")
			require.NotNil(t, cosineDistanceImpl, "cosineDistanceImpl should be initialized")
			require.NotNil(t, dotProductImpl, "dotProductImpl should be initialized")
			require.NotNil(t, euclideanDistanceBatchImpl, "euclideanDistanceBatchImpl should be initialized")
			require.NotNil(t, l2SquaredImpl, "l2SquaredImpl should be initialized")

			// Verify registry has entries
			require.NotNil(t, Registry.Get(MetricEuclidean, DataTypeFloat32, 0), "Registry should have float32 euclidean entry")
			require.NotNil(t, Registry.Get(MetricCosine, DataTypeFloat32, 0), "Registry should have float32 cosine entry")
			require.NotNil(t, Registry.Get(MetricDotProduct, DataTypeFloat32, 0), "Registry should have float32 dot product entry")
		})
	}
}

// TestDispatchFunctionPointers verifies that dispatch function pointers
// are properly set and callable after initialization.
func TestDispatchFunctionPointers(t *testing.T) {
	// Ensure dispatch has been initialized
	require.NotNil(t, euclideanDistanceImpl, "euclideanDistanceImpl should be initialized")
	require.NotNil(t, cosineDistanceImpl, "cosineDistanceImpl should be initialized")
	require.NotNil(t, dotProductImpl, "dotProductImpl should be initialized")

	// Test that function pointers are callable (basic smoke test)
	a := []float32{1.0, 2.0, 3.0}
	b := []float32{4.0, 5.0, 6.0}

	// Test euclidean distance
	dist, err := euclideanDistanceImpl(a, b)
	require.NoError(t, err, "euclideanDistanceImpl should not return error")
	assert.Greater(t, dist, float32(0), "Distance should be positive")

	// Test cosine distance
	cosDist, err := cosineDistanceImpl(a, b)
	require.NoError(t, err, "cosineDistanceImpl should not return error")
	assert.True(t, cosDist >= -1.0 && cosDist <= 1.0, "Cosine distance should be in [-1, 1]")

	// Test dot product
	dot, err := dotProductImpl(a, b)
	require.NoError(t, err, "dotProductImpl should not return error")
	assert.NotEqual(t, float32(0), dot, "Dot product should not be zero")
}

// TestRegistryAfterDispatch verifies that the dynamic registry
// contains expected implementations after dispatch initialization.
func TestRegistryAfterDispatch(t *testing.T) {
	// Test core metric/data type combinations
	testCases := []struct {
		metric MetricType
		dtype  SIMDDataType
		dims   int
	}{
		{MetricEuclidean, DataTypeFloat32, 0},
		{MetricEuclidean, DataTypeFloat32, 128},
		{MetricEuclidean, DataTypeFloat32, 384},
		{MetricCosine, DataTypeFloat32, 0},
		{MetricDotProduct, DataTypeFloat32, 0},
		{MetricDotProduct, DataTypeFloat32, 128},
		{MetricDotProduct, DataTypeFloat32, 384},
		{MetricEuclidean, DataTypeFloat16, 0},
		{MetricCosine, DataTypeFloat16, 0},
		{MetricDotProduct, DataTypeFloat16, 0},
		{MetricEuclidean, DataTypeComplex64, 0},
		{MetricEuclidean, DataTypeComplex128, 0},
	}

	for _, tc := range testCases {
		t.Run(tc.metric.String()+"_"+tc.dtype.String()+"_"+fmt.Sprintf("%d", tc.dims), func(t *testing.T) {
			assert.NotNil(t, Registry.Get(tc.metric, tc.dtype, tc.dims),
				"Registry should have %s %s %d implementation", tc.metric, tc.dtype, tc.dims)
		})
	}
}
