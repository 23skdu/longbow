package quantization

import (
	"testing"
)

func TestBitWidthPredictor(t *testing.T) {
	predictor := NewBitWidthPredictor(100)

	// High variance data should use 8 bits
	highVar := make([][]float32, 100)
	for i := range highVar {
		highVar[i] = []float32{float32(i), float32(i * 2)}
	}
	bits := predictor.Predict(highVar)
	if bits != 8 {
		t.Errorf("Expected 8 bits for high variance data, got %d", bits)
	}

	// Low variance data should use fewer bits
	lowVar := make([][]float32, 100)
	for i := range lowVar {
		lowVar[i] = []float32{0.001, 0.002}
	}
	bits = predictor.Predict(lowVar)
	if bits > 4 {
		t.Errorf("Expected <= 4 bits for low variance data, got %d", bits)
	}
}
