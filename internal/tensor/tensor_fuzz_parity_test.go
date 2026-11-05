package tensor

import (
	"math"
	"testing"
)

// TestTensorOracleParity_ContractAndTranspose verifies that tensor Contraction
// (Einstein summation / matrix multiplication) and Transpose match a 64-bit
// float64 scalar reference oracle.
func TestTensorOracleParity_ContractAndTranspose(t *testing.T) {
	m, k, n := 4, 6, 5
	a := New(DtypeFloat32, Shape{m, k})
	b := New(DtypeFloat32, Shape{k, n})

	aSlice := a.Float32s()
	bSlice := b.Float32s()
	for i := range aSlice {
		aSlice[i] = float32(i%7-3) * 0.5
	}
	for i := range bSlice {
		bSlice[i] = float32(i%5-2) * 0.25
	}

	a.SetLabels([]string{"i", "k"})
	b.SetLabels([]string{"k", "j"})
	c, err := TensorContract(a, b, []string{"k"}, []string{"i", "j"})
	if err != nil {
		t.Fatalf("TensorContract failed: %v", err)
	}
	cSlice := c.Float32s()

	for i := 0; i < m; i++ {
		for j := 0; j < n; j++ {
			var gt float64
			for p := 0; p < k; p++ {
				gt += float64(aSlice[i*k+p]) * float64(bSlice[p*n+j])
			}
			got := float64(cSlice[i*n+j])
			if math.Abs(got-gt) > 1e-5 {
				t.Fatalf("Contract mismatch at (%d,%d): got %v, want %v", i, j, got, gt)
			}
		}
	}
}

// FuzzTensorContractOracleParity fuzzes 2D tensor contraction against a float64
// reference matrix multiplication oracle.
func FuzzTensorContractOracleParity(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	f.Add([]byte{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120})

	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) < 12 {
			return
		}
		m, k, n := 2, 3, 2
		a := New(DtypeFloat32, Shape{m, k})
		b := New(DtypeFloat32, Shape{k, n})
		aData := a.Float32s()
		bData := b.Float32s()

		for i := 0; i < m*k; i++ {
			aData[i] = float32(int8(raw[i%len(raw)])) / 16.0
			bData[i] = float32(int8(raw[(i+6)%len(raw)])) / 16.0
		}

		a.SetLabels([]string{"i", "k"})
		b.SetLabels([]string{"k", "j"})
		c, err := MatMul(a, b)
		if err != nil {
			t.Fatalf("MatMul failed: %v", err)
		}
		cData := c.Float32s()

		for i := 0; i < m; i++ {
			for j := 0; j < n; j++ {
				var gt float64
				for p := 0; p < k; p++ {
					gt += float64(aData[i*k+p]) * float64(bData[p*n+j])
				}
				if math.Abs(float64(cData[i*n+j])-gt) > 1e-4 {
					t.Fatalf("FuzzTensorContractOracleParity mismatch at (%d,%d): got %v, want %v", i, j, cData[i*n+j], gt)
				}
			}
		}
	})
}
