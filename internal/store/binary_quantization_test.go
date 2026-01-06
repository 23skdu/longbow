package store


import (
	"math/rand"
	"testing"
)

// =============================================================================
// Binary Quantization Tests - TDD Red Phase
// =============================================================================

func TestBinaryQuantizer_Train(t *testing.T) {
	vectors := [][]float32{
		{0.1, 0.5, 0.9, 0.3},
		{0.2, 0.6, 0.8, 0.4},
		{0.3, 0.4, 0.7, 0.2},
		{0.4, 0.3, 0.6, 0.5},
	}

	bq, err := TrainBinaryQuantizer(vectors)
	if err != nil {
		t.Fatalf("TrainBinaryQuantizer failed: %v", err)
	}

	if bq == nil {
		t.Fatal("BinaryQuantizer is nil")
	}

	if len(bq.Threshold) != 4 {
		t.Errorf("Expected 4 thresholds, got %d", len(bq.Threshold))
	}

	if bq.Threshold[0] < 0.1 || bq.Threshold[0] > 0.4 {
		t.Errorf("Threshold[0] out of expected range: %f", bq.Threshold[0])
	}
}

func TestBinaryQuantizer_Quantize(t *testing.T) {
	bq := &BinaryQuantizer{
		Threshold: []float32{0.5, 0.5, 0.5, 0.5},
	}

	vec := []float32{0.8, 0.2, 0.9, 0.1}
	result := bq.Quantize(vec)
	if len(result) != 1 {
		t.Fatalf("Expected 1 uint64, got %d", len(result))
	}

	expected := uint64(5) // 0b0101
	if result[0] != expected {
		t.Errorf("Expected %b, got %b", expected, result[0])
	}
}

func TestBinaryQuantizer_Quantize64Dims(t *testing.T) {
	thresholds := make([]float32, 64)
	for i := range thresholds {
		thresholds[i] = 0.5
	}
	bq := &BinaryQuantizer{Threshold: thresholds}

	vec := make([]float32, 64)
	for i := range vec {
		if i%2 == 0 {
			vec[i] = 0.8
		} else {
			vec[i] = 0.2
		}
	}

	result := bq.Quantize(vec)
	if len(result) != 1 {
		t.Errorf("64 dims should produce 1 uint64, got %d", len(result))
	}

	expected := uint64(0x5555555555555555)
	if result[0] != expected {
		t.Errorf("Expected %x, got %x", expected, result[0])
	}
}

func TestBinaryQuantizer_Quantize1536Dims(t *testing.T) {
	thresholds := make([]float32, 1536)
	for i := range thresholds {
		thresholds[i] = 0.0
	}
	bq := &BinaryQuantizer{Threshold: thresholds}

	vec := make([]float32, 1536)
	for i := range vec {
		vec[i] = float32(i%2)*2 - 1
	}

	result := bq.Quantize(vec)
	expectedLen := 1536 / 64
	if len(result) != expectedLen {
		t.Errorf("1536 dims should produce %d uint64s, got %d", expectedLen, len(result))
	}

	originalSize := 1536 * 4
	quantizedSize := len(result) * 8
	reduction := float64(originalSize) / float64(quantizedSize)
	if reduction < 31 || reduction > 33 {
		t.Errorf("Expected ~32x reduction, got %.1fx", reduction)
	}
}

func TestHammingDistancePOPCNT(t *testing.T) {
	a := []uint64{0b1010}
	b := []uint64{0b1100}

	dist := HammingDistancePOPCNT(a, b)
	if dist != 2 {
		t.Errorf("Expected Hamming distance 2, got %d", dist)
	}
}

func TestHammingDistancePOPCNT_Identical(t *testing.T) {
	a := []uint64{0xFFFFFFFFFFFFFFFF, 0x123456789ABCDEF0}
	b := []uint64{0xFFFFFFFFFFFFFFFF, 0x123456789ABCDEF0}

	dist := HammingDistancePOPCNT(a, b)
	if dist != 0 {
		t.Errorf("Identical vectors should have distance 0, got %d", dist)
	}
}

func TestHammingDistancePOPCNT_AllDifferent(t *testing.T) {
	a := []uint64{0x0000000000000000}
	b := []uint64{0xFFFFFFFFFFFFFFFF}

	dist := HammingDistancePOPCNT(a, b)
	if dist != 64 {
		t.Errorf("All different bits should give distance 64, got %d", dist)
	}
}

func TestHammingDistancePOPCNT_LargeVectors(t *testing.T) {
	a := make([]uint64, 24)
	b := make([]uint64, 24)

	for i := range a {
		a[i] = 0xFFFFFFFFFFFFFFFF
		b[i] = 0x0000000000000000
	}

	dist := HammingDistancePOPCNT(a, b)
	expected := 24 * 64
	if dist != expected {
		t.Errorf("Expected max distance %d, got %d", expected, dist)
	}
}

func TestBinaryQuantizer_EndToEnd(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	vectors := make([][]float32, 100)
	for i := range vectors {
		vectors[i] = make([]float32, 128)
		for j := range vectors[i] {
			vectors[i][j] = rng.Float32()
		}
	}

	bq, err := TrainBinaryQuantizer(vectors)
	if err != nil {
		t.Fatalf("Training failed: %v", err)
	}

	v1 := vectors[0]
	v2 := make([]float32, 128)
	for i := range v2 {
		v2[i] = v1[i] + 0.01
	}

	q1 := bq.Quantize(v1)
	q2 := bq.Quantize(v2)

	dist := HammingDistancePOPCNT(q1, q2)
	if dist > 20 {
		t.Errorf("Similar vectors should have low distance, got %d", dist)
	}

	v3 := make([]float32, 128)
	for i := range v3 {
		v3[i] = 1.0 - v1[i]
	}
	q3 := bq.Quantize(v3)

	dist3 := HammingDistancePOPCNT(q1, q3)
	if dist3 < dist {
		t.Errorf("Inverted vector should have higher distance than similar vector")
	}
}

func TestBinaryQuantizer_EmptyInput(t *testing.T) {
	_, err := TrainBinaryQuantizer(nil)
	if err == nil {
		t.Error("Expected error for nil input")
	}

	_, err = TrainBinaryQuantizer([][]float32{})
	if err == nil {
		t.Error("Expected error for empty input")
	}
}

func TestBinaryQuantizer_Dims(t *testing.T) {
	bq := &BinaryQuantizer{
		Threshold: make([]float32, 768),
	}

	if bq.Dims() != 768 {
		t.Errorf("Expected 768 dims, got %d", bq.Dims())
	}
}

func BenchmarkHammingDistancePOPCNT(b *testing.B) {
	a := make([]uint64, 24)
	bv := make([]uint64, 24)
	for i := range a {
		a[i] = uint64(i * 12345)
		bv[i] = uint64(i * 67890)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		HammingDistancePOPCNT(a, bv)
	}
}
