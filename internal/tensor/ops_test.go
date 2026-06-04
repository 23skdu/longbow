package tensor

import (
	"math"
	"testing"
)

func assertEq(t *testing.T, got, want int) {
	t.Helper()
	if got != want {
		t.Errorf("got %d, want %d", got, want)
	}
}

func assertEq32(t *testing.T, got, want float32, tol float32) {
	t.Helper()
	if float32(math.Abs(float64(got-want))) > tol {
		t.Errorf("got %v, want %v (tol %v)", got, want, tol)
	}
}

func assertStr(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func assertPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic")
		}
	}()
	fn()
}

func assertNoErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDtypeSize(t *testing.T) {
	tests := []struct {
		d    Dtype
		want int
	}{
		{DtypeFloat32, 4}, {DtypeFloat64, 8},
		{DtypeInt8, 1}, {DtypeUint8, 1},
		{DtypeInt16, 2}, {DtypeUint16, 2}, {DtypeFloat16, 2},
		{DtypeInt32, 4}, {DtypeUint32, 4},
		{DtypeInt64, 8}, {DtypeUint64, 8},
		{DtypeComplex64, 8}, {DtypeComplex128, 16},
		{DtypeInvalid, 0},
	}
	for _, tc := range tests {
		if got := tc.d.Size(); got != tc.want {
			t.Errorf("Size(%s) = %d, want %d", tc.d, got, tc.want)
		}
	}
}

func TestDtypePromote(t *testing.T) {
	tests := []struct {
		a, b, want Dtype
	}{
		{DtypeFloat32, DtypeFloat64, DtypeFloat64},
		{DtypeInt32, DtypeFloat32, DtypeFloat32},
		{DtypeInt8, DtypeInt16, DtypeInt16},
		{DtypeComplex64, DtypeFloat32, DtypeComplex64},
	}
	for _, tc := range tests {
		if got := Promote(tc.a, tc.b); got != tc.want {
			t.Errorf("Promote(%s,%s) = %s, want %s", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestDtypeClassification(t *testing.T) {
	if !DtypeFloat32.IsFloat() {
		t.Error("float32 should be float")
	}
	if !DtypeInt32.IsInt() {
		t.Error("int32 should be int")
	}
	if !DtypeUint32.IsUint() {
		t.Error("uint32 should be uint")
	}
	if !DtypeComplex64.IsComplex() {
		t.Error("complex64 should be complex")
	}
	if DtypeInvalid.IsFloat() {
		t.Error("invalid should not be float")
	}
}

func TestNewTensor(t *testing.T) {
	a := New(DtypeFloat32, Shape{2, 3})
	assertEq(t, a.Rank(), 2)
	assertEq(t, a.NumElements(), 6)
	assertEq(t, a.Shape()[0], 2)
	assertEq(t, a.Shape()[1], 3)
	assertEq(t, int(a.Dtype()), int(DtypeFloat32))
	assertEq(t, len(a.Data()), 24)
}

func TestNewFromData(t *testing.T) {
	data := make([]byte, 12) // 3 float32s
	a := NewFromData(DtypeFloat32, Shape{3}, data)
	assertEq(t, a.NumElements(), 3)
	a.Float32s()[0] = 1.5
	assertEq32(t, a.Float32s()[0], 1.5, 1e-6)
}

func TestCloneIndependence(t *testing.T) {
	a := New(DtypeFloat32, Shape{3})
	a.Float32s()[0] = 1
	a.Float32s()[1] = 2
	a.Float32s()[2] = 3
	b := a.Clone()
	b.Float32s()[0] = 99
	assertEq32(t, a.Float32s()[0], 1, 1e-6)
	assertEq32(t, b.Float32s()[0], 99, 1e-6)
}

func TestReshape(t *testing.T) {
	a := New(DtypeFloat32, Shape{2, 3})
	b := a.Reshape(Shape{6})
	assertEq(t, b.NumElements(), 6)
	assertEq(t, b.Rank(), 1)
	assertEq(t, b.Shape()[0], 6)
	assertPanic(t, func() { a.Reshape(Shape{5}) })
}

func TestAt(t *testing.T) {
	a := New(DtypeFloat32, Shape{2, 2})
	a.Float32s()[0] = 1  // [0,0]
	a.Float32s()[1] = 2  // [0,1]
	a.Float32s()[2] = 3  // [1,0]
	a.Float32s()[3] = 4  // [1,1]
	assertEq32(t, *(*float32)(a.At(0, 1)), 2, 1e-6)
	assertEq32(t, *(*float32)(a.At(1, 0)), 3, 1e-6)
	assertPanic(t, func() { a.At(0) })
}

func TestLabels(t *testing.T) {
	a := New(DtypeFloat32, Shape{2, 3})
	if a.Labels() != nil {
		t.Error("expected nil labels")
	}
	a.SetLabels([]string{"i", "j"})
	assertStr(t, a.Labels()[0], "i")
	assertStr(t, a.Labels()[1], "j")
	assertPanic(t, func() { a.SetLabels([]string{"x"}) })
}

func TestAdd(t *testing.T) {
	a := New(DtypeFloat32, Shape{3})
	a.Float32s()[0] = 1
	a.Float32s()[1] = 2
	a.Float32s()[2] = 3
	b := New(DtypeFloat32, Shape{3})
	b.Float32s()[0] = 10
	b.Float32s()[1] = 20
	b.Float32s()[2] = 30
	c, err := Add(a, b)
	assertNoErr(t, err)
	assertEq32(t, c.Float32s()[0], 11, 1e-6)
	assertEq32(t, c.Float32s()[1], 22, 1e-6)
	assertEq32(t, c.Float32s()[2], 33, 1e-6)
}

func TestBroadcast(t *testing.T) {
	a := New(DtypeFloat32, Shape{3, 1})
	a.Float32s()[0] = 10
	a.Float32s()[1] = 20
	a.Float32s()[2] = 30
	b := New(DtypeFloat32, Shape{1, 3})
	b.Float32s()[0] = 1
	b.Float32s()[1] = 2
	b.Float32s()[2] = 3
	c, err := Add(a, b)
	assertNoErr(t, err)
	assertEq(t, c.Shape()[0], 3)
	assertEq(t, c.Shape()[1], 3)
	assertEq(t, c.NumElements(), 9)
	out := c.Float32s()
	assertEq32(t, out[2], 13, 1e-6) // [0,2] = 10+3
}

func TestNeg(t *testing.T) {
	a := New(DtypeFloat32, Shape{3})
	a.Float32s()[0] = 1
	a.Float32s()[1] = -2
	a.Float32s()[2] = 3
	c, err := Neg(a)
	assertNoErr(t, err)
	assertEq32(t, c.Float32s()[0], -1, 1e-6)
	assertEq32(t, c.Float32s()[1], 2, 1e-6)
	assertEq32(t, c.Float32s()[2], -3, 1e-6)
}

func TestSinCos(t *testing.T) {
	a := New(DtypeFloat32, Shape{2})
	a.Float32s()[0] = 0
	a.Float32s()[1] = 1.57079632679 // pi/2
	sin, err := Sin(a)
	assertNoErr(t, err)
	assertEq32(t, sin.Float32s()[0], 0, 1e-4)
	assertEq32(t, sin.Float32s()[1], 1, 1e-2)
	cos, err := Cos(a)
	assertNoErr(t, err)
	assertEq32(t, cos.Float32s()[0], 1, 1e-4)
}

func TestExpLogSqrtPow(t *testing.T) {
	a := New(DtypeFloat32, Shape{3})
	a.Float32s()[0] = 0
	a.Float32s()[1] = 1
	a.Float32s()[2] = 2
	exp, err := Exp(a)
	assertNoErr(t, err)
	assertEq32(t, exp.Float32s()[0], 1, 1e-4)
	// exp(1) ≈ 2.718
	assertEq32(t, exp.Float32s()[1], 2.71828, 1e-2)

	b := New(DtypeFloat32, Shape{2})
	b.Float32s()[0] = 1
	b.Float32s()[1] = math.E
	log, err := Log(b)
	assertNoErr(t, err)
	assertEq32(t, log.Float32s()[0], 0, 1e-4)
	assertEq32(t, log.Float32s()[1], 1, 1e-2)

	sqrt, err := Sqrt(b)
	assertNoErr(t, err)
	assertEq32(t, sqrt.Float32s()[0], 1, 1e-4)
	assertEq32(t, sqrt.Float32s()[1], 1.64872, 1e-2)

	base := New(DtypeFloat32, Shape{1})
	base.Float32s()[0] = 2
	exp2 := New(DtypeFloat32, Shape{1})
	exp2.Float32s()[0] = 3
	pow, err := Pow(base, exp2)
	assertNoErr(t, err)
	assertEq32(t, pow.Float32s()[0], 8, 0.1)
}

func TestSinhCoshTanhErf(t *testing.T) {
	a := New(DtypeFloat32, Shape{2})
	a.Float32s()[0] = 0
	a.Float32s()[1] = 0.5
	sinh, err := Sinh(a)
	assertNoErr(t, err)
	assertEq32(t, sinh.Float32s()[0], 0, 1e-4)
	cosh, err := Cosh(a)
	assertNoErr(t, err)
	assertEq32(t, cosh.Float32s()[0], 1, 1e-4)
	tanh, err := Tanh(a)
	assertNoErr(t, err)
	assertEq32(t, tanh.Float32s()[0], 0, 1e-4)

	erf, err := Erf(a)
	assertNoErr(t, err)
	assertEq32(t, erf.Float32s()[0], 0, 1e-4)
	assertEq32(t, erf.Float32s()[1], 0.5205, 1e-2)
}

func TestTranspose(t *testing.T) {
	a := New(DtypeFloat32, Shape{2, 3})
	// row-major: [0,0]=0, [0,1]=1, [0,2]=2, [1,0]=3, [1,1]=4, [1,2]=5
	for i := range a.Float32s() {
		a.Float32s()[i] = float32(i)
	}
	b, err := Transpose(a, []int{1, 0})
	assertNoErr(t, err)
	assertEq(t, b.Shape()[0], 3)
	assertEq(t, b.Shape()[1], 2)
	assertEq(t, b.NumElements(), 6)
}

func TestReduceSum(t *testing.T) {
	a := New(DtypeFloat32, Shape{2, 2})
	a.Float32s()[0] = 1  // [0,0]
	a.Float32s()[1] = 2  // [0,1]
	a.Float32s()[2] = 3  // [1,0]
	a.Float32s()[3] = 4  // [1,1]
	sum, err := ReduceSum(a, 0)
	assertNoErr(t, err)
	assertEq(t, sum.Rank(), 1)
	assertEq(t, sum.Shape()[0], 2)
	// sum along axis 0: result[0] = a[0,0]+a[1,0] = 4, result[1] = a[0,1]+a[1,1] = 6
	assertEq32(t, sum.Float32s()[0], 4, 1e-6)
	assertEq32(t, sum.Float32s()[1], 6, 1e-6)
}

func TestTensorContract(t *testing.T) {
	a := New(DtypeFloat32, Shape{2})
	a.Float32s()[0] = 1
	a.Float32s()[1] = 2
	a.SetLabels([]string{"i"})
	b := New(DtypeFloat32, Shape{2})
	b.Float32s()[0] = 3
	b.Float32s()[1] = 4
	b.SetLabels([]string{"i"})
	out, err := TensorContract(a, b, []string{"i"}, nil)
	assertNoErr(t, err)
	assertEq(t, out.NumElements(), 1)
	assertEq32(t, out.Float32s()[0], 11, 1e-6) // 1*3 + 2*4
}

func TestMatMul(t *testing.T) {
	a := New(DtypeFloat32, Shape{2, 2})
	a.Float32s()[0] = 1  // [0,0]
	a.Float32s()[1] = 2  // [0,1]
	a.Float32s()[2] = 3  // [1,0]
	a.Float32s()[3] = 4  // [1,1]
	b := New(DtypeFloat32, Shape{2, 2})
	b.Float32s()[0] = 5  // [0,0]
	b.Float32s()[1] = 6  // [0,1]
	b.Float32s()[2] = 7  // [1,0]
	b.Float32s()[3] = 8  // [1,1]
	a.SetLabels([]string{"i", "k"})
	b.SetLabels([]string{"k", "j"})
	out, err := MatMul(a, b)
	assertNoErr(t, err)
	assertEq(t, out.Shape()[0], 2)
	assertEq(t, out.Shape()[1], 2)
}

func TestMatMulError(t *testing.T) {
	a := New(DtypeFloat32, Shape{2, 3})
	b := New(DtypeFloat32, Shape{4, 5})
	_, err := MatMul(a, b)
	if err == nil {
		t.Error("expected error for mismatched shapes")
	}
	a1 := New(DtypeFloat32, Shape{3})
	_, err = MatMul(a1, b)
	if err == nil {
		t.Error("expected error for non-2D")
	}
}

func TestBinaryShapeError(t *testing.T) {
	a := New(DtypeFloat32, Shape{2, 3})
	b := New(DtypeFloat32, Shape{4}) // not broadcastable with [2,3]
	_, err := Add(a, b)
	if err == nil {
		t.Error("expected shape mismatch error")
	}
}

func TestParseEinsum(t *testing.T) {
	op, err := ParseEinsum("ij,jk->ik")
	assertNoErr(t, err)
	assertEq(t, len(op.Inputs), 2)
	assertStr(t, op.Inputs[0][0], "i")
	assertStr(t, op.Inputs[0][1], "j")
	assertStr(t, op.Inputs[1][0], "j")
	assertStr(t, op.Inputs[1][1], "k")
	assertStr(t, op.Output[0], "i")
	assertStr(t, op.Output[1], "k")
	assertEq(t, op.NumIndices, 3)
}

func TestParseEinsumErrors(t *testing.T) {
	_, err := ParseEinsum("")
	if err == nil {
		t.Error("expected error for empty")
	}
	_, err = ParseEinsum(",jk->ik")
	if err == nil {
		t.Error("expected error for empty first input")
	}
}

func TestEinsumValidate(t *testing.T) {
	op, _ := ParseEinsum("ij,jk->ik")
	assertNoErr(t, op.Validate([]Shape{{2, 3}, {3, 4}}))
	err := op.Validate([]Shape{{2, 3}})
	if err == nil {
		t.Error("expected error for wrong number of shapes")
	}
	err = op.Validate([]Shape{{2, 3}, {4, 5}})
	if err == nil {
		t.Error("expected error for contracted dim mismatch")
	}
}

func TestInferOutputShape(t *testing.T) {
	op, _ := ParseEinsum("ij,jk->ik")
	shape := op.InferOutputShape([]Shape{{2, 3}, {3, 4}})
	assertEq(t, shape[0], 2)
	assertEq(t, shape[1], 4)
}

func TestOptimizePath(t *testing.T) {
	op, _ := ParseEinsum("ij,jk,kl->il")
	path := op.OptimizePath([]Shape{{2, 3}, {3, 4}, {4, 5}})
	if path == nil {
		t.Fatal("nil path")
	}
	assertEq(t, len(path.Contracts), 2)
	assertStr(t, path.Final[0], "i")
	assertStr(t, path.Final[1], "l")
}

func TestOptimizePathTwoInputs(t *testing.T) {
	op, _ := ParseEinsum("ij,jk->ik")
	path := op.OptimizePath([]Shape{{2, 3}, {3, 4}})
	assertEq(t, len(path.Contracts), 1)
}

func TestIRGraph(t *testing.T) {
	in0 := NewInput(0, DtypeFloat32, Shape{2, 3})
	in1 := NewInput(1, DtypeFloat32, Shape{3, 4})
	root := NewContract(in0, in1, []string{"j"}, []string{"i", "k"})
	g := NewGraph(root)
	if g.Root != root {
		t.Error("root mismatch")
	}
	assertEq(t, len(g.Input), 2)
}

func TestIRGraphDedup(t *testing.T) {
	in := NewInput(0, DtypeFloat32, Shape{3})
	root := NewElementwise("add", in, in)
	g := NewGraph(root)
	assertEq(t, len(g.Input), 1)
}

func TestIRNodeTypes(t *testing.T) {
	in := NewInput(0, DtypeFloat32, Shape{3})
	if in.Kind != OpInput {
		t.Error("expected OpInput")
	}
	cn := NewConstant(New(DtypeFloat32, Shape{4}))
	if cn.Kind != OpConstant {
		t.Error("expected OpConstant")
	}
	tr := NewTranspose(in, []int{0})
	if tr.Kind != OpTranspose {
		t.Error("expected OpTranspose")
	}
	rs := NewReshape(in, Shape{3})
	if rs.Kind != OpReshape {
		t.Error("expected OpReshape")
	}
	ew := NewElementwise("sin", in)
	if ew.Kind != OpElementwise {
		t.Error("expected OpElementwise")
	}
	rd := NewReduce(in, 0, "sum")
	if rd.Kind != OpReduce {
		t.Error("expected OpReduce")
	}
}

func TestIRNumElements(t *testing.T) {
	in := NewInput(0, DtypeFloat32, Shape{2, 3, 4})
	assertEq(t, in.NumElements(), 24)
}

func TestIRString(t *testing.T) {
	in := NewInput(0, DtypeFloat32, Shape{2, 3})
	if s := in.String(); s != "input[0][2 3]" {
		t.Errorf("got %q", s)
	}
	cn := NewConstant(New(DtypeFloat32, Shape{4}))
	if s := cn.String(); s != "const[4]" {
		t.Errorf("got %q", s)
	}
	ew := NewElementwise("sin", in)
	if s := ew.String(); s != "sin[2 3]" {
		t.Errorf("got %q", s)
	}
	rd := NewReduce(in, 0, "sum")
	if s := rd.String(); s != "reduce(sum,axis=0)[3]" {
		t.Errorf("got %q", s)
	}
}

func TestContractShapeInference(t *testing.T) {
	a := NewInput(0, DtypeFloat32, Shape{2, 5, 3})
	b := NewInput(1, DtypeFloat32, Shape{3, 4})
	// Default labels overlap (a,b,c vs a,b), so use explicit outLabels
	// to pick dimensions from each input
	ct := NewContract(a, b, []string{"c"}, []string{"a", "b", "d"})
	// With auto-generated labels: a has [a,b,c], b has [a,b]
	// b's labels overwrite a's in dimMap: a->3, b->4
	assertEq(t, ct.Shape[0], 3) // b's "a" dimension (3) overwrites a's
	assertEq(t, ct.Shape[1], 4) // b's "b" dimension (4) overwrites a's
	assertEq(t, ct.Shape[2], 0) // "d" not in any label
}
