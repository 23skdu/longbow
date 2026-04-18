package store

import (
	"math"
	"testing"

	"github.com/23skdu/longbow/internal/store/internal/core"
	"github.com/23skdu/longbow/internal/store/types"

	"github.com/23skdu/longbow/internal/pq"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQuantizerInterface verifies a generic quantizer works correctly
func TestQuantizerInterface(t *testing.T) {
	// Test SQ8 quantizer interface
	t.Run("SQ8", func(t *testing.T) {
		vectors := [][]float32{
			{1.0, 2.0, 3.0, 4.0},
			{5.0, 6.0, 7.0, 8.0},
		}
		encoder, err := core.TrainSQ8Encoder(vectors)
		require.NoError(t, err)
		require.NotNil(t, encoder)
		assert.Equal(t, 4, encoder.Dims())
	})

	// Test BQ quantizer interface
	t.Run("BQ", func(t *testing.T) {
		encoder := types.NewBQEncoder(64)
		require.NotNil(t, encoder)
		assert.Equal(t, 1, encoder.CodeSize()) // 64 dims = 1 uint64
	})

	// Test PQ quantizer interface
	t.Run("PQ", func(t *testing.T) {
		encoder, err := pq.NewPQEncoder(128, 16, 256) // 128 dims, 16 subspaces, 256 centroids each
		require.NoError(t, err)
		require.NotNil(t, encoder)
		assert.Equal(t, 16, encoder.CodeSize()) // M = 16 bytes
	})
}

// TestQuantizer_EncodeDecode_RoundTrip tests that encode/decode preserves data
func TestQuantizer_EncodeDecode_RoundTrip(t *testing.T) {
	t.Run("SQ8", func(t *testing.T) {
		// Use different min/max values for proper training
		vectors := [][]float32{
			{0.0, 1.0, 2.0, 3.0, 4.0},
			{5.0, 6.0, 7.0, 8.0, 9.0},
		}
		encoder, err := core.TrainSQ8Encoder(vectors)
		require.NoError(t, err)

		original := []float32{2.5, 3.5, 4.5, 5.5, 6.5}
		codes := encoder.Encode(original)
		require.Equal(t, 5, len(codes))

		decoded := encoder.Decode(codes)
		require.Equal(t, len(original), len(decoded))

		// Check quantization error is bounded (SQ8 error should be within quantization step)
		// With range [0, 9], step = 9/255 ≈ 0.035
		for i := range original {
			assert.InDelta(t, original[i], decoded[i], 0.2, "Quantization error should be bounded")
		}
	})

	t.Run("BQ", func(t *testing.T) {
		encoder := types.NewBQEncoder(64)
		original := make([]float32, 64)
		for i := range original {
			if i%2 == 0 {
				original[i] = 1.0
			} else {
				original[i] = -1.0
			}
		}

		codes := encoder.Encode(original)
		require.Equal(t, 1, len(codes))

		decoded := encoder.Decode(codes)
		require.Equal(t, 64, len(decoded))

		// Check binary values are preserved
		for i := range original {
			if original[i] > 0 {
				assert.Equal(t, float32(1.0), decoded[i])
			} else {
				assert.Equal(t, float32(-1.0), decoded[i])
			}
		}
	})

	t.Run("PQ", func(t *testing.T) {
		encoder, err := pq.NewPQEncoder(64, 8, 4) // Use fewer centroids for small training set
		require.NoError(t, err)

		// Train the encoder with sufficient data
		trainingData := make([][]float32, 10)
		for i := range trainingData {
			trainingData[i] = make([]float32, 64)
			for j := range trainingData[i] {
				trainingData[i][j] = float32(i) * float32(j) * 0.01
			}
		}
		err = encoder.Train(trainingData)
		require.NoError(t, err)

		original := make([]float32, 64)
		for i := range original {
			original[i] = float32(i) * 0.1
		}

		codes, err := encoder.Encode(original)
		require.NoError(t, err)
		require.Equal(t, 8, len(codes)) // M = 8

		decoded, err := encoder.Decode(codes)
		require.NoError(t, err)
		require.Equal(t, len(original), len(decoded))
	})
}

// TestQuantizer_EncodeMultipleVectors tests batch encoding
func TestQuantizer_EncodeMultipleVectors(t *testing.T) {
	t.Run("SQ8", func(t *testing.T) {
		vectors := [][]float32{
			{1.0, 2.0, 3.0, 4.0},
			{5.0, 6.0, 7.0, 8.0},
			{9.0, 10.0, 11.0, 12.0},
		}
		encoder, err := core.TrainSQ8Encoder(vectors)
		require.NoError(t, err)

		// Encode each vector
		for _, vec := range vectors {
			codes := encoder.Encode(vec)
			assert.Equal(t, 4, len(codes))
		}
	})

	t.Run("BQ", func(t *testing.T) {
		encoder := types.NewBQEncoder(128)
		vectors := [][]float32{
			make([]float32, 128),
			make([]float32, 128),
		}

		// Encode each vector
		for _, vec := range vectors {
			codes := encoder.Encode(vec)
			assert.Equal(t, 2, len(codes)) // 128 dims = 2 uint64s
		}
	})
}

// TestQuantizer_EncodeHandlesTypeConversion tests conversion between vector types
func TestQuantizer_EncodeHandlesTypeConversion(t *testing.T) {
	// For now, our encoders only support float32
	// This test documents the expected behavior

	t.Run("SQ8_Float32Only", func(t *testing.T) {
		vectors := [][]float32{{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}}
		encoder, err := core.TrainSQ8Encoder(vectors)
		require.NoError(t, err)

		// Works with float32
		codes := encoder.Encode([]float32{1.5, 2.5, 3.5})
		assert.Equal(t, 3, len(codes))
	})
}

// TestQuantizer_DecodeEmptyInput handles empty code arrays
func TestQuantizer_DecodeEmptyInput(t *testing.T) {
	t.Run("SQ8", func(t *testing.T) {
		vectors := [][]float32{{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}}
		encoder, err := core.TrainSQ8Encoder(vectors)
		require.NoError(t, err)

		decoded := encoder.Decode([]uint8{})
		assert.Equal(t, 0, len(decoded))
	})

	t.Run("BQ", func(t *testing.T) {
		encoder := types.NewBQEncoder(64)
		decoded := encoder.Decode([]uint64{})
		assert.Equal(t, 64, len(decoded))
		// Returns default values (all -1.0) for empty input
	})
}

// TestGenericQuantizer_Float32ToUint8 tests float32 -> uint8 quantization (SQ8 style)
func TestGenericQuantizer_Float32ToUint8(t *testing.T) {
	vectors := [][]float32{
		{0.0, 0.0, 0.0},
		{10.0, 10.0, 10.0},
	}
	encoder, err := core.TrainSQ8Encoder(vectors)
	require.NoError(t, err)

	vec := []float32{5.0, 5.0, 5.0}
	codes := encoder.Encode(vec)
	decoded := encoder.Decode(codes)

	require.Equal(t, len(vec), len(codes), "Code length should match vector length")
	require.Equal(t, len(vec), len(decoded), "Decoded vector length should match code length")

	for i := range vec {
		assert.InDelta(t, vec[i], decoded[i], 0.1, "Quantization error should be bounded")
	}
}

// TestGenericQuantizer_Float32ToUint64 tests float32 -> uint64 binary quantization (BQ style)
func TestGenericQuantizer_Float32ToUint64(t *testing.T) {
	encoder := types.NewBQEncoder(64)

	// Use 64 dimensions to match encoder
	vec := make([]float32, 64)
	vec[0] = 1.0
	vec[1] = -1.0
	vec[2] = 1.0

	codes := encoder.Encode(vec)
	require.Equal(t, 1, len(codes), "Should produce 1 code word for 64 dims")

	// Verify Hamming distance calculation
	hamming := encoder.HammingDistance(codes, codes)
	assert.Equal(t, 0, hamming, "Distance to self should be 0")

	// Verify score conversion
	score := encoder.ScoreToFloat32(0)
	assert.Equal(t, float32(1.0), score, "Zero Hamming should give max score")
}

// TestGenericQuantizer_Float32ToByte tests float32 -> byte quantization (PQ style)
func TestGenericQuantizer_Float32ToByte(t *testing.T) {
	encoder, err := pq.NewPQEncoder(64, 8, 4) // 64 dims, 8 subspaces, 4 centroids
	require.NoError(t, err)

	// Train with sufficient sample data
	trainingData := make([][]float32, 10)
	for i := range trainingData {
		trainingData[i] = make([]float32, 64)
		for j := range trainingData[i] {
			trainingData[i][j] = float32(i+j) * 0.1
		}
	}
	err = encoder.Train(trainingData)
	require.NoError(t, err)

	vec := make([]float32, 64)
	for i := range vec {
		vec[i] = float32(i) * 0.1
	}
	codes, err := encoder.Encode(vec)
	require.NoError(t, err)
	require.Equal(t, 8, len(codes), "Code length should match M (number of subspaces)")
}

func TestQuantizer_TypeConversion_Float16ToFloat32(t *testing.T) {
	vecs := make([][]float16.Num, 3)
	for i := range vecs {
		vecs[i] = make([]float16.Num, 8)
		for j := range vecs[i] {
			vecs[i][j] = float16.New(float32(i*8 + j))
		}
	}

	encoder, err := core.TrainSQ8EncoderFloat16(vecs)
	require.NoError(t, err)
	require.NotNil(t, encoder)

	minVals, maxVals := encoder.GetBounds()
	require.Equal(t, 8, len(minVals))
	require.Equal(t, 8, len(maxVals))
	require.True(t, minVals[0] < maxVals[0], "min should be less than max")
}

func TestQuantizer_TypeConversion_Int8ToFloat32(t *testing.T) {
	vecs := make([][]int8, 3)
	vecs[0] = []int8{-128, -64, 32, 64, 100, -32, 96, 16}
	vecs[1] = []int8{0, 32, 64, 96, 127, -128, -64, 48}
	vecs[2] = []int8{16, 48, 80, 112, -50, -96, -32, 127}

	encoder, err := core.TrainSQ8EncoderInt8(vecs)
	require.NoError(t, err)
	require.NotNil(t, encoder)

	minVals, maxVals := encoder.GetBounds()
	require.Equal(t, 8, len(minVals))
	require.Equal(t, 8, len(maxVals))
	require.True(t, minVals[0] < maxVals[0], "min should be less than max")

	f32 := make([]float32, len(vecs[0]))
	for i, v := range vecs[0] {
		f32[i] = float32(v)
	}
	codes := encoder.Encode(f32)
	require.Equal(t, 8, len(codes))
	decoded := encoder.Decode(codes)
	require.Equal(t, 8, len(decoded))
}

// TestGenericSQ8Quantizer tests the generic quantizer wrapper
func TestGenericSQ8Quantizer(t *testing.T) {
	vectors := [][]float32{
		{1.0, 2.0, 3.0, 4.0},
		{5.0, 6.0, 7.0, 8.0},
	}
	encoder, err := core.TrainSQ8Encoder(vectors)
	require.NoError(t, err)

	wrapper := NewGenericSQ8Quantizer(encoder)
	require.NotNil(t, wrapper)

	// Test Encode
	vec := []float32{2.0, 3.0, 4.0, 5.0}
	codes, err := wrapper.Encode(vec)
	require.NoError(t, err)
	assert.Equal(t, 4, len(codes))

	// Test Decode
	decoded, err := wrapper.Decode(codes)
	require.NoError(t, err)
	assert.Equal(t, 4, len(decoded))

	// Test Dims
	assert.Equal(t, 4, wrapper.Dims())
}

// TestGenericBQQuantizer tests the generic BQ quantizer wrapper
func TestGenericBQQuantizer(t *testing.T) {
	encoder := types.NewBQEncoder(64)
	wrapper := NewGenericBQQuantizer(encoder)
	require.NotNil(t, wrapper)

	// Test Encode
	vec := make([]float32, 64)
	for i := range vec {
		if i%2 == 0 {
			vec[i] = 1.0
		} else {
			vec[i] = -1.0
		}
	}
	codes, err := wrapper.Encode(vec)
	require.NoError(t, err)
	assert.Equal(t, 1, len(codes))

	// Test Decode
	decoded, err := wrapper.Decode(codes)
	require.NoError(t, err)
	assert.Equal(t, 64, len(decoded))

	// Test Dims
	assert.Equal(t, 64, wrapper.Dims())
}

// TestGenericPQQuantizer tests the generic PQ quantizer wrapper
func TestGenericPQQuantizer(t *testing.T) {
	encoder, err := pq.NewPQEncoder(64, 8, 4)
	require.NoError(t, err)

	// Train with sufficient data
	trainingData := make([][]float32, 10)
	for i := range trainingData {
		trainingData[i] = make([]float32, 64)
		for j := range trainingData[i] {
			trainingData[i][j] = float32(i+j) * 0.1
		}
	}
	err = encoder.Train(trainingData)
	require.NoError(t, err)

	wrapper := NewGenericPQQuantizer(encoder)
	require.NotNil(t, wrapper)

	// Test Encode
	vec := make([]float32, 64)
	for i := range vec {
		vec[i] = float32(i) * 0.1
	}
	codes, err := wrapper.Encode(vec)
	require.NoError(t, err)
	assert.Equal(t, 8, len(codes))

	// Test Decode
	decoded, err := wrapper.Decode(codes)
	require.NoError(t, err)
	assert.Equal(t, 64, len(decoded))

	// Test Dims
	assert.Equal(t, 64, wrapper.Dims())
}

// TestSQ8_PerDimensionBounds tests per-dimension min/max scaling
func TestSQ8_PerDimensionBounds(t *testing.T) {
	vectors := [][]float32{
		{0.0, 0.0},   // Min for both dims
		{100.0, 1.0}, // Max for dim 0, small range for dim 1
	}
	encoder, err := core.TrainSQ8Encoder(vectors)
	require.NoError(t, err)

	minVals, maxVals := encoder.GetBounds()
	assert.Equal(t, float32(0.0), minVals[0])
	assert.Equal(t, float32(100.0), maxVals[0])
	assert.Equal(t, float32(0.0), minVals[1])
	assert.Equal(t, float32(1.0), maxVals[1])
}

// TestSQ8_EncodeInto tests zero-allocation encoding
func TestSQ8_EncodeInto(t *testing.T) {
	vectors := [][]float32{
		{1.0, 2.0, 3.0, 4.0},
		{5.0, 6.0, 7.0, 8.0},
	}
	encoder, err := core.TrainSQ8Encoder(vectors)
	require.NoError(t, err)

	dst := make([]uint8, 4)
	encoder.EncodeInto([]float32{2.0, 3.0, 4.0, 5.0}, dst)
	assert.NotEqual(t, uint8(0), dst[0])
}

// TestBQ_HammingDistanceBatch tests batch distance computation
func TestBQ_HammingDistanceBatch(t *testing.T) {
	encoder := types.NewBQEncoder(64)

	query := make([]float32, 64)
	query[0] = 1.0
	query[1] = -1.0
	query[2] = 1.0
	query[3] = -1.0
	queryCode := encoder.Encode(query)

	cand1 := make([]float32, 64)
	cand1[0] = 1.0
	cand1[1] = -1.0
	cand1[2] = 1.0
	cand1[3] = -1.0

	cand2 := make([]float32, 64)
	cand2[0] = -1.0
	cand2[1] = 1.0
	cand2[2] = -1.0
	cand2[3] = 1.0

	candidates := [][]uint64{
		encoder.Encode(cand1),
		encoder.Encode(cand2),
	}

	results := make([]int, len(candidates))
	encoder.HammingDistanceBatch(queryCode, candidates, results)

	assert.Equal(t, 0, results[0], "Same vector should have 0 distance")
	assert.Greater(t, results[1], 0, "Opposite vector should have positive distance")
}

// TestBQ_CodeSize tests code size calculation for various dimensions
func TestBQ_CodeSize(t *testing.T) {
	testCases := []struct {
		dims     int
		expected int
	}{
		{1, 1},   // 1 dim = 1 uint64
		{64, 1},  // 64 dims = 1 uint64
		{65, 2},  // 65 dims = 2 uint64s
		{128, 2}, // 128 dims = 2 uint64s
		{129, 3}, // 129 dims = 3 uint64s
		{256, 4}, // 256 dims = 4 uint64s
	}

	for _, tc := range testCases {
		encoder := types.NewBQEncoder(tc.dims)
		assert.Equal(t, tc.expected, encoder.CodeSize(), "Dimensions: %d", tc.dims)
	}
}

// TestBQ_Float32ToHamming tests threshold conversion
func TestBQ_Float32ToHamming(t *testing.T) {
	encoder := types.NewBQEncoder(100)

	// Similarity 0.9 should allow 10 bits different
	hamming := encoder.Float32ToHamming(0.9)
	assert.Equal(t, 10, hamming)

	// Similarity 1.0 should allow 0 bits different
	hamming = encoder.Float32ToHamming(1.0)
	assert.Equal(t, 0, hamming)

	// Similarity 0.0 should allow all bits different
	hamming = encoder.Float32ToHamming(0.0)
	assert.Equal(t, 100, hamming)
}

// FuzzQuantizer_RoundTrip tests full encode/decode cycle with random data
func FuzzQuantizer_RoundTrip(f *testing.F) {
	// Seed corpus
	f.Add(float32(1.0), float32(2.0), float32(3.0), float32(4.0))
	f.Add(float32(-5.0), float32(5.0), float32(0.0), float32(2.5))

	f.Fuzz(func(t *testing.T, v1, v2, v3, v4 float32) {
		vectors := [][]float32{
			{v1, v2, v3, v4},
			{v4, v3, v2, v1},
		}

		encoder, err := core.TrainSQ8Encoder(vectors)
		if err != nil {
			t.Skip("Training failed")
		}

		original := []float32{v1, v2, v3, v4}
		codes := encoder.Encode(original)
		decoded := encoder.Decode(codes)

		// Check bounds
		assert.Equal(t, len(original), len(decoded))

		// Quantization error should be bounded by the range of values
		minVal, maxVal := encoder.GetBounds()
		maxRange := float32(0)
		for i := range minVal {
			if r := maxVal[i] - minVal[i]; r > maxRange {
				maxRange = r
			}
		}
		maxError := maxRange / 255.0 // SQ8 quantization step

		for i := range original {
			// The error should be bounded by quantization step
			assert.InDelta(t, original[i], decoded[i], float64(maxError)*2,
				"Quantization error exceeds bounds at index %d", i)
		}
	})
}

// TestConvertFloat32ToUint8 tests the bounds-checked conversion function
func TestConvertFloat32ToUint8(t *testing.T) {
	t.Run("Normal range", func(t *testing.T) {
		vec := []float32{0.0, 63.75, 127.5, 191.25, 255.0}
		result, err := ConvertFloat32ToUint8(vec)
		require.NoError(t, err)
		require.Equal(t, len(vec), len(result))

		assert.Equal(t, uint8(0), result[0])
		assert.InDelta(t, uint8(64), result[1], 1)
		assert.InDelta(t, uint8(128), result[2], 1)
		assert.InDelta(t, uint8(191), result[3], 1)
		assert.Equal(t, uint8(255), result[4])
	})

	t.Run("Negative values", func(t *testing.T) {
		vec := []float32{-10.0, 0.0, 10.0}
		result, err := ConvertFloat32ToUint8(vec)
		require.NoError(t, err)
		require.Equal(t, len(vec), len(result))

		assert.Equal(t, uint8(0), result[0])
		assert.InDelta(t, uint8(128), result[1], 1)
		assert.Equal(t, uint8(255), result[2])
	})

	t.Run("Values above 255", func(t *testing.T) {
		vec := []float32{0.0, 255.0, 510.0}
		result, err := ConvertFloat32ToUint8(vec)
		require.NoError(t, err)
		require.Equal(t, len(vec), len(result))

		assert.Equal(t, uint8(0), result[0])
		assert.InDelta(t, uint8(128), result[1], 1)
		assert.Equal(t, uint8(255), result[2])
	})

	t.Run("Negative values", func(t *testing.T) {
		vec := []float32{-10.0, 0.0, 10.0}
		result, err := ConvertFloat32ToUint8(vec)
		require.NoError(t, err)
		require.Equal(t, len(vec), len(result))

		// Should scale from min to max: [-10, 10] -> [0, 255]
		// -10 -> 0, 0 -> 127.5, 10 -> 255
		assert.Equal(t, uint8(0), result[0])
		assert.InDelta(t, uint8(128), result[1], 1) // 127.5 -> 127 or 128
		assert.Equal(t, uint8(255), result[2])
	})

	t.Run("Values above 255", func(t *testing.T) {
		vec := []float32{0.0, 255.0, 510.0}
		result, err := ConvertFloat32ToUint8(vec)
		require.NoError(t, err)
		require.Equal(t, len(vec), len(result))

		// Should scale from min to max: [0, 510] -> [0, 255]
		assert.Equal(t, uint8(0), result[0])
		assert.InDelta(t, uint8(128), result[1], 1) // 255 -> 127.5 -> 127 or 128
		assert.Equal(t, uint8(255), result[2])
	})

	t.Run("All same values", func(t *testing.T) {
		vec := []float32{100.0, 100.0, 100.0}
		result, err := ConvertFloat32ToUint8(vec)
		require.NoError(t, err)
		require.Equal(t, len(vec), len(result))

		// All values should be clamped to 100
		for _, v := range result {
			assert.Equal(t, uint8(100), v)
		}
	})

	t.Run("All same negative values", func(t *testing.T) {
		vec := []float32{-10.0, -10.0, -10.0}
		result, err := ConvertFloat32ToUint8(vec)
		require.NoError(t, err)
		require.Equal(t, len(vec), len(result))

		// All values should be clamped to 0
		for _, v := range result {
			assert.Equal(t, uint8(0), v)
		}
	})

	t.Run("All same values above 255", func(t *testing.T) {
		vec := []float32{300.0, 300.0, 300.0}
		result, err := ConvertFloat32ToUint8(vec)
		require.NoError(t, err)
		require.Equal(t, len(vec), len(result))

		// All values should be clamped to 255
		for _, v := range result {
			assert.Equal(t, uint8(255), v)
		}
	})

	t.Run("Empty vector", func(t *testing.T) {
		vec := []float32{}
		result, err := ConvertFloat32ToUint8(vec)
		require.NoError(t, err)
		require.Equal(t, 0, len(result))
	})

	t.Run("NaN values", func(t *testing.T) {
		vec := []float32{0.0, float32(math.NaN()), 100.0}
		_, err := ConvertFloat32ToUint8(vec)
		require.Error(t, err)
		require.Contains(t, err.Error(), "NaN")
	})

	t.Run("Inf values", func(t *testing.T) {
		vec := []float32{0.0, float32(math.Inf(1)), 100.0}
		_, err := ConvertFloat32ToUint8(vec)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Inf")
	})
}
