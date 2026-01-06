package store


import (
	"math"
	"testing"
)

// =============================================================================
// Subtask 2: Scalar Quantization (SQ8) Tests - TDD Red Phase
// =============================================================================

// TestDefaultSQ8Config verifies default configuration values
func TestDefaultSQ8Config(t *testing.T) {
	cfg := DefaultSQ8Config()

	if cfg == nil {
		t.Fatal("DefaultSQ8Config returned nil")
	}

	// Default should be trained=false
	if cfg.Trained {
		t.Error("Default config should have Trained=false")
	}
}

// TestSQ8ConfigValidation tests configuration validation rules
func TestSQ8ConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *SQ8Config
		wantErr bool
	}{
		{
			name:    "nil config",
			cfg:     nil,
			wantErr: true,
		},
		{
			name:    "untrained config is valid",
			cfg:     &SQ8Config{Trained: false},
			wantErr: false,
		},
		{
			name: "trained but mismatched min/max lengths",
			cfg: &SQ8Config{
				Trained: true,
				Min:     []float32{0, 0, 0},
				Max:     []float32{1, 1}, // Different length
			},
			wantErr: true,
		},
		{
			name: "trained with valid min/max",
			cfg: &SQ8Config{
				Trained: true,
				Min:     []float32{0, 0, 0},
				Max:     []float32{1, 1, 1},
			},
			wantErr: false,
		},
		{
			name: "trained with min >= max",
			cfg: &SQ8Config{
				Trained: true,
				Min:     []float32{0, 1, 0}, // Min[1] >= Max[1]
				Max:     []float32{1, 1, 1},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestSQ8EncoderCreation tests encoder creation
func TestSQ8EncoderCreation(t *testing.T) {
	cfg := &SQ8Config{
		Trained: true,
		Min:     []float32{-1, -1, -1, -1},
		Max:     []float32{1, 1, 1, 1},
	}

	enc, err := NewSQ8Encoder(cfg)
	if err != nil {
		t.Fatalf("NewSQ8Encoder failed: %v", err)
	}

	if enc == nil {
		t.Fatal("NewSQ8Encoder returned nil encoder")
	}

	if enc.Dims() != 4 {
		t.Errorf("Dims() = %d, want 4", enc.Dims())
	}
}

// TestSQ8EncoderTrainFromData tests training from sample vectors
func TestSQ8EncoderTrainFromData(t *testing.T) {
	// Sample vectors with known min/max
	vectors := [][]float32{
		{-1.0, 0.0, 0.5, 1.0},
		{-0.5, -1.0, 0.0, 0.5},
		{0.0, 1.0, -0.5, -1.0},
		{0.5, 0.5, 1.0, 0.0},
	}

	enc, err := TrainSQ8Encoder(vectors)
	if err != nil {
		t.Fatalf("TrainSQ8Encoder failed: %v", err)
	}

	if enc.Dims() != 4 {
		t.Errorf("Dims() = %d, want 4", enc.Dims())
	}

	// Check that min/max were learned correctly
	minVals, maxVals := enc.GetBounds()

	expectedMin := []float32{-1.0, -1.0, -0.5, -1.0}
	expectedMax := []float32{0.5, 1.0, 1.0, 1.0}

	for i := 0; i < 4; i++ {
		if minVals[i] != expectedMin[i] {
			t.Errorf("Min[%d] = %f, want %f", i, minVals[i], expectedMin[i])
		}
		if maxVals[i] != expectedMax[i] {
			t.Errorf("Max[%d] = %f, want %f", i, maxVals[i], expectedMax[i])
		}
	}
}

// TestSQ8EncodeVector tests encoding float32 to uint8
func TestSQ8EncodeVector(t *testing.T) {
	cfg := &SQ8Config{
		Trained: true,
		Min:     []float32{0, 0, 0, 0},
		Max:     []float32{1, 1, 1, 1},
	}

	enc, _ := NewSQ8Encoder(cfg)

	tests := []struct {
		name   string
		input  []float32
		expect []uint8
	}{
		{
			name:   "all zeros -> 0",
			input:  []float32{0, 0, 0, 0},
			expect: []uint8{0, 0, 0, 0},
		},
		{
			name:   "all ones -> 255",
			input:  []float32{1, 1, 1, 1},
			expect: []uint8{255, 255, 255, 255},
		},
		{
			name:   "half values -> 127 or 128",
			input:  []float32{0.5, 0.5, 0.5, 0.5},
			expect: []uint8{127, 127, 127, 127}, // half value maps to 127
		},
		{
			name:   "values below min clamped to 0",
			input:  []float32{-1, -0.5, 0, 0},
			expect: []uint8{0, 0, 0, 0},
		},
		{
			name:   "values above max clamped to 255",
			input:  []float32{2, 1.5, 1, 1},
			expect: []uint8{255, 255, 255, 255},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := enc.Encode(tt.input)
			for i, v := range result {
				if v != tt.expect[i] {
					t.Errorf("Encode()[%d] = %d, want %d", i, v, tt.expect[i])
				}
			}
		})
	}
}

// TestSQ8DecodeVector tests decoding uint8 back to float32
func TestSQ8DecodeVector(t *testing.T) {
	cfg := &SQ8Config{
		Trained: true,
		Min:     []float32{0, 0, 0, 0},
		Max:     []float32{1, 1, 1, 1},
	}

	enc, _ := NewSQ8Encoder(cfg)

	// Decode should approximately inverse encode
	tests := []struct {
		name      string
		input     []uint8
		expectMin []float32
		expectMax []float32
	}{
		{
			name:      "zeros -> near 0",
			input:     []uint8{0, 0, 0, 0},
			expectMin: []float32{0, 0, 0, 0},
			expectMax: []float32{0.01, 0.01, 0.01, 0.01},
		},
		{
			name:      "255 -> near 1",
			input:     []uint8{255, 255, 255, 255},
			expectMin: []float32{0.99, 0.99, 0.99, 0.99},
			expectMax: []float32{1.01, 1.01, 1.01, 1.01},
		},
		{
			name:      "127 -> near 0.5",
			input:     []uint8{127, 127, 127, 127},
			expectMin: []float32{0.48, 0.48, 0.48, 0.48},
			expectMax: []float32{0.52, 0.52, 0.52, 0.52},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := enc.Decode(tt.input)
			for i, v := range result {
				if v < tt.expectMin[i] || v > tt.expectMax[i] {
					t.Errorf("Decode()[%d] = %f, want between %f and %f",
						i, v, tt.expectMin[i], tt.expectMax[i])
				}
			}
		})
	}
}

// TestSQ8RoundTrip tests encode-decode preserves values within quantization error
func TestSQ8RoundTrip(t *testing.T) {
	cfg := &SQ8Config{
		Trained: true,
		Min:     []float32{-1, -1, -1, -1},
		Max:     []float32{1, 1, 1, 1},
	}

	enc, _ := NewSQ8Encoder(cfg)

	original := []float32{0.123, -0.456, 0.789, -0.234}
	encoded := enc.Encode(original)
	decoded := enc.Decode(encoded)

	// Maximum quantization error: range / 255 = 2.0 / 255 ≈ 0.0078
	maxError := float32(2.0 / 255.0)

	for i := 0; i < len(original); i++ {
		diff := float32(math.Abs(float64(original[i] - decoded[i])))
		if diff > maxError*2 { // Allow 2x for rounding
			t.Errorf("RoundTrip[%d]: original=%f, decoded=%f, diff=%f > maxError=%f",
				i, original[i], decoded[i], diff, maxError)
		}
	}
}

// TestSQ8EncodeInto tests in-place encoding without allocation
func TestSQ8EncodeInto(t *testing.T) {
	cfg := &SQ8Config{
		Trained: true,
		Min:     []float32{0, 0, 0, 0},
		Max:     []float32{1, 1, 1, 1},
	}

	enc, _ := NewSQ8Encoder(cfg)

	input := []float32{0.25, 0.5, 0.75, 1.0}
	dst := make([]uint8, 4)

	enc.EncodeInto(input, dst)

	expected := enc.Encode(input)
	for i, v := range dst {
		if v != expected[i] {
			t.Errorf("EncodeInto[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

// TestSQ8DecodeInto tests in-place decoding without allocation
func TestSQ8DecodeInto(t *testing.T) {
	cfg := &SQ8Config{
		Trained: true,
		Min:     []float32{0, 0, 0, 0},
		Max:     []float32{1, 1, 1, 1},
	}

	enc, _ := NewSQ8Encoder(cfg)

	input := []uint8{64, 128, 192, 255}
	dst := make([]float32, 4)

	enc.DecodeInto(input, dst)

	expected := enc.Decode(input)
	for i, v := range dst {
		if v != expected[i] {
			t.Errorf("DecodeInto[%d] = %f, want %f", i, v, expected[i])
		}
	}
}

// TestSQ8Distance tests distance calculation on quantized vectors
func TestSQ8Distance(t *testing.T) {
	cfg := &SQ8Config{
		Trained: true,
		Min:     []float32{0, 0, 0, 0},
		Max:     []float32{1, 1, 1, 1},
	}

	enc, _ := NewSQ8Encoder(cfg)

	// Test vectors
	v1 := []float32{0, 0, 0, 0}
	v2 := []float32{1, 1, 1, 1}
	v3 := []float32{0.5, 0.5, 0.5, 0.5}

	// Encode
	q1 := enc.Encode(v1)
	q2 := enc.Encode(v2)
	q3 := enc.Encode(v3)

	// Distance between identical vectors should be ~0
	d11 := SQ8EuclideanDistance(q1, q1, enc)
	if d11 > 0.01 {
		t.Errorf("Distance(q1, q1) = %f, want ~0", d11)
	}

	// Distance between opposite corners
	d12 := SQ8EuclideanDistance(q1, q2, enc)
	// Actual Euclidean: sqrt(4) = 2.0
	if d12 < 1.9 || d12 > 2.1 {
		t.Errorf("Distance(q1, q2) = %f, want ~2.0", d12)
	}

	// Distance from center to corner
	d13 := SQ8EuclideanDistance(q1, q3, enc)
	d23 := SQ8EuclideanDistance(q2, q3, enc)
	// Both should be ~1.0 (sqrt(4 * 0.25) = 1.0)
	if d13 < 0.9 || d13 > 1.1 {
		t.Errorf("Distance(q1, q3) = %f, want ~1.0", d13)
	}
	if d23 < 0.9 || d23 > 1.1 {
		t.Errorf("Distance(q2, q3) = %f, want ~1.0", d23)
	}
}

// TestSQ8DistanceFast tests fast integer-only distance approximation
func TestSQ8DistanceFast(t *testing.T) {
	// Fast distance works in quantized space without decoding
	q1 := []uint8{0, 0, 0, 0}
	q2 := []uint8{255, 255, 255, 255}
	q3 := []uint8{127, 127, 127, 127}

	// Distance in quantized space (squared L2)
	d11 := SQ8DistanceFast(q1, q1)
	if d11 != 0 {
		t.Errorf("DistanceFast(q1, q1) = %d, want 0", d11)
	}

	// Maximum distance
	d12 := SQ8DistanceFast(q1, q2)
	expected := uint32(255 * 255 * 4) // 260100
	if d12 != expected {
		t.Errorf("DistanceFast(q1, q2) = %d, want %d", d12, expected)
	}

	// Half distance
	d13 := SQ8DistanceFast(q1, q3)
	expectedHalf := uint32(127 * 127 * 4) // 64516
	if d13 != expectedHalf {
		t.Errorf("DistanceFast(q1, q3) = %d, want %d", d13, expectedHalf)
	}
}

// TestSQ8CompressionRatio tests memory savings calculation
func TestSQ8CompressionRatio(t *testing.T) {
	// 128-dim float32 = 512 bytes
	// 128-dim uint8 = 128 bytes
	// Ratio = 4x

	dims := 128
	originalSize := dims * 4  // float32 = 4 bytes
	quantizedSize := dims * 1 // uint8 = 1 byte

	ratio := float64(originalSize) / float64(quantizedSize)
	if ratio != 4.0 {
		t.Errorf("Compression ratio = %f, want 4.0", ratio)
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkSQ8Encode(b *testing.B) {
	cfg := &SQ8Config{
		Trained: true,
		Min:     make([]float32, 128),
		Max:     make([]float32, 128),
	}
	for i := 0; i < 128; i++ {
		cfg.Min[i] = -1
		cfg.Max[i] = 1
	}
	enc, _ := NewSQ8Encoder(cfg)

	vec := make([]float32, 128)
	for i := range vec {
		vec[i] = float32(i) / 128.0
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = enc.Encode(vec)
	}
}

func BenchmarkSQ8EncodeInto(b *testing.B) {
	cfg := &SQ8Config{
		Trained: true,
		Min:     make([]float32, 128),
		Max:     make([]float32, 128),
	}
	for i := 0; i < 128; i++ {
		cfg.Min[i] = -1
		cfg.Max[i] = 1
	}
	enc, _ := NewSQ8Encoder(cfg)

	vec := make([]float32, 128)
	for i := range vec {
		vec[i] = float32(i) / 128.0
	}
	dst := make([]uint8, 128)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc.EncodeInto(vec, dst)
	}
}

func BenchmarkSQ8DistanceFast(b *testing.B) {
	q1 := make([]uint8, 128)
	q2 := make([]uint8, 128)
	for i := range q2 {
		q2[i] = uint8(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SQ8DistanceFast(q1, q2)
	}
}

func BenchmarkSQ8EuclideanDistance(b *testing.B) {
	cfg := &SQ8Config{
		Trained: true,
		Min:     make([]float32, 128),
		Max:     make([]float32, 128),
	}
	for i := 0; i < 128; i++ {
		cfg.Min[i] = -1
		cfg.Max[i] = 1
	}
	enc, _ := NewSQ8Encoder(cfg)

	q1 := make([]uint8, 128)
	q2 := make([]uint8, 128)
	for i := range q2 {
		q2[i] = uint8(i * 2)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SQ8EuclideanDistance(q1, q2, enc)
	}
}
