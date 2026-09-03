package tensor

import (
	"math"
	"testing"
)

func TestFloat64Contraction(t *testing.T) {
	// A is 2x3, B is 3x2
	a := New(DtypeFloat64, Shape{2, 3})
	b := New(DtypeFloat64, Shape{3, 2})

	aData := a.Float64s()
	bData := b.Float64s()

	// A = [[1, 2, 3], [4, 5, 6]]
	for i := 0; i < 6; i++ {
		aData[i] = float64(i + 1)
	}
	// B = [[1, 2], [3, 4], [5, 6]]
	for i := 0; i < 6; i++ {
		bData[i] = float64(i + 1)
	}

	a.SetLabels([]string{"i", "k"})
	b.SetLabels([]string{"k", "j"})

	// Expected C = A * B
	// C[0,0] = 1*1 + 2*3 + 3*5 = 1 + 6 + 15 = 22
	// C[0,1] = 1*2 + 2*4 + 3*6 = 2 + 8 + 18 = 28
	// C[1,0] = 4*1 + 5*3 + 6*5 = 4 + 15 + 30 = 49
	// C[1,1] = 4*2 + 5*4 + 6*6 = 8 + 20 + 36 = 64
	c, err := TensorContract(a, b, []string{"k"}, []string{"i", "j"})
	if err != nil {
		t.Fatalf("Float64 contraction failed: %v", err)
	}

	cData := c.Float64s()
	expected := []float64{22, 28, 49, 64}
	for i, exp := range expected {
		if math.Abs(cData[i]-exp) > 1e-6 {
			t.Errorf("cData[%d]: expected %f, got %f", i, exp, cData[i])
		}
	}
}

func TestComplexContraction(t *testing.T) {
	// 2x2 complex64 and complex128
	a := New(DtypeComplex128, Shape{2, 2})
	b := New(DtypeComplex128, Shape{2, 2})

	aData := a.Complex128s()
	bData := b.Complex128s()

	// A = [[1+2i, 0], [0, 1+2i]]
	aData[0] = complex(1, 2)
	aData[1] = complex(0, 0)
	aData[2] = complex(0, 0)
	aData[3] = complex(1, 2)

	// B = [[3+4i, 0], [0, 3+4i]]
	bData[0] = complex(3, 4)
	bData[1] = complex(0, 0)
	bData[2] = complex(0, 0)
	bData[3] = complex(3, 4)

	a.SetLabels([]string{"i", "k"})
	b.SetLabels([]string{"k", "j"})

	c, err := TensorContract(a, b, []string{"k"}, []string{"i", "j"})
	if err != nil {
		t.Fatalf("Complex128 contraction failed: %v", err)
	}

	cData := c.Complex128s()
	// (1+2i)(3+4i) = (3 - 8) + (4 + 6)i = -5 + 10i
	expectedDiag := complex(-5, 10)
	if cData[0] != expectedDiag || cData[3] != expectedDiag {
		t.Errorf("expected diag %v, got %v and %v", expectedDiag, cData[0], cData[3])
	}
}

func TestIntContraction(t *testing.T) {
	a := New(DtypeInt64, Shape{2, 2})
	b := New(DtypeInt64, Shape{2, 2})

	a.Int64s()[0] = 2
	a.Int64s()[3] = 3
	b.Int64s()[0] = 5
	b.Int64s()[3] = 7

	a.SetLabels([]string{"i", "k"})
	b.SetLabels([]string{"k", "j"})

	c, err := TensorContract(a, b, []string{"k"}, []string{"i", "j"})
	if err != nil {
		t.Fatalf("Int64 contraction failed: %v", err)
	}

	if c.Int64s()[0] != 10 || c.Int64s()[3] != 21 {
		t.Errorf("unexpected Int64 result: %v", c.Int64s())
	}
}

func TestEinsumDiagonalAndTrace(t *testing.T) {
	// 3x3 matrix
	m := New(DtypeFloat64, Shape{3, 3})
	data := m.Float64s()
	// m = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
	for i := 0; i < 9; i++ {
		data[i] = float64(i + 1)
	}

	// 1. Diagonal: "ii->i"
	diag, err := Einsum("ii->i", m)
	if err != nil {
		t.Fatalf("Einsum('ii->i') failed: %v", err)
	}
	if diag.Rank() != 1 || diag.Shape()[0] != 3 {
		t.Fatalf("expected shape [3], got %v", diag.Shape())
	}
	diagData := diag.Float64s()
	if diagData[0] != 1 || diagData[1] != 5 || diagData[2] != 9 {
		t.Errorf("expected diag [1, 5, 9], got %v", diagData)
	}

	// 2. Trace: "ii->"
	tr, err := Einsum("ii->", m)
	if err != nil {
		t.Fatalf("Einsum('ii->') failed: %v", err)
	}
	trVal := getScalarFloat(tr, []int{0})
	if trVal != 15.0 {
		t.Errorf("expected trace 15.0 (1+5+9), got %f", trVal)
	}
}

func TestEinsumMultiTensorChain(t *testing.T) {
	// 3 tensors: A (2x3), B (3x4), C (4x2)
	// Compute A * B * C via "ij,jk,kl->il"
	a := New(DtypeFloat32, Shape{2, 3})
	b := New(DtypeFloat32, Shape{3, 4})
	c := New(DtypeFloat32, Shape{4, 2})

	for i := range a.Float32s() {
		a.Float32s()[i] = 1.0
	}
	for i := range b.Float32s() {
		b.Float32s()[i] = 1.0
	}
	for i := range c.Float32s() {
		c.Float32s()[i] = 1.0
	}

	// A (2x3) * B (3x4) gives (2x4) matrix of all 3.0s
	// (2x4) * C (4x2) gives (2x2) matrix of all 12.0s
	res, err := Einsum("ij,jk,kl->il", a, b, c)
	if err != nil {
		t.Fatalf("Einsum multi-tensor chain failed: %v", err)
	}
	if res.Shape()[0] != 2 || res.Shape()[1] != 2 {
		t.Fatalf("expected shape [2, 2], got %v", res.Shape())
	}
	for i, v := range res.Float32s() {
		if v != 12.0 {
			t.Errorf("res[%d]: expected 12.0, got %f", i, v)
		}
	}
}
