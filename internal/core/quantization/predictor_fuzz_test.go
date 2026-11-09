package quantization

import (
	"encoding/binary"
	"math"
	"testing"
)

// FuzzBitWidthPredictor checks the bit-width predictor on rectangular
// matrices derived from fuzz bytes: the result must always be one of the
// supported widths (1/2/4/8), must be deterministic, and a zero-variance
// (constant) matrix must always predict the minimum width of 1 bit.
func FuzzBitWidthPredictor(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8}, uint8(2), uint8(4))
	f.Add(make([]byte, 256), uint8(8), uint8(8))
	f.Add([]byte{}, uint8(1), uint8(0))
	f.Add(make([]byte, 64), uint8(64), uint8(64))
	f.Add([]byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}, uint8(3), uint8(2))
	f.Add(make([]byte, 8), uint8(4), uint8(255)) // out of range: sampleSize > 64

	f.Fuzz(func(t *testing.T, data []byte, dim uint8, sampleSize uint8) {
		if dim < 1 || dim > 64 {
			return
		}
		if sampleSize > 64 {
			return
		}
		if len(data) > 4096 {
			return
		}

		d := int(dim)
		// Build a rectangular matrix: every row has exactly d elements.
		n := 1 + len(data)/(4*d)
		if n > 64 {
			n = 64
		}
		vectors := make([][]float32, n)
		for i := range vectors {
			row := make([]float32, d)
			for j := range row {
				idx := (i*d + j) * 4
				if idx+4 <= len(data) {
					row[j] = math.Float32frombits(binary.LittleEndian.Uint32(data[idx : idx+4]))
				}
			}
			vectors[i] = row
		}

		p := NewBitWidthPredictor(int(sampleSize))
		bits := p.Predict(vectors)
		switch bits {
		case 1, 2, 4, 8:
		default:
			t.Fatalf("Predict() = %d, want one of 1, 2, 4, 8", bits)
		}

		// Determinism: same input, same prediction.
		if again := p.Predict(vectors); again != bits {
			t.Fatalf("Predict() not deterministic: %d vs %d", bits, again)
		}

		// Empty input is explicitly defined to use the maximum width.
		if len(data) == 0 {
			if got := p.Predict(nil); got != 8 {
				t.Fatalf("Predict(nil) = %d, want 8", got)
			}
		}

		// A constant matrix has zero variance and must pick 1 bit when a
		// sane sample size is configured (sampleSize 0 is degenerate and
		// falls back to the maximum width, checked above via the set bound).
		pConst := NewBitWidthPredictor(1)
		var c float32
		if len(data) > 0 {
			c = float32(int8(data[0])) / 16
		}
		constant := make([][]float32, n)
		for i := range constant {
			row := make([]float32, d)
			for j := range row {
				row[j] = c
			}
			constant[i] = row
		}
		if got := pConst.Predict(constant); got != 1 {
			t.Fatalf("Predict(constant matrix) = %d, want 1", got)
		}
	})
}
