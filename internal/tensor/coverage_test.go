package tensor

import (
	"math"
	"math/rand"
	"testing"
)

func TestDtypeString(t *testing.T) {
	tests := []struct {
		d    Dtype
		want string
	}{
		{DtypeInvalid, "invalid"}, {DtypeFloat32, "float32"}, {DtypeFloat64, "float64"},
		{DtypeInt8, "int8"}, {DtypeUint8, "uint8"}, {DtypeInt16, "int16"}, {DtypeUint16, "uint16"},
		{DtypeInt32, "int32"}, {DtypeUint32, "uint32"}, {DtypeInt64, "int64"}, {DtypeUint64, "uint64"},
		{DtypeFloat16, "float16"}, {DtypeComplex64, "complex64"}, {DtypeComplex128, "complex128"},
	}
	for _, tc := range tests {
		if got := tc.d.String(); got != tc.want {
			t.Errorf("String(%d) = %q, want %q", tc.d, got, tc.want)
		}
	}
}

func TestFinite(t *testing.T) {
	if !Finite32(1.0) {
		t.Error("Finite32(1.0) should be true")
	}
	if Finite32(float32(math.Inf(1))) {
		t.Error("Finite32(+Inf) should be false")
	}
	if Finite32(float32(math.Inf(-1))) {
		t.Error("Finite32(-Inf) should be false")
	}
	if Finite32(float32(math.NaN())) {
		t.Error("Finite32(NaN) should be false")
	}
	if !Finite64(1.0) {
		t.Error("Finite64(1.0) should be true")
	}
	if Finite64(math.Inf(1)) {
		t.Error("Finite64(+Inf) should be false")
	}
}

func TestMin(t *testing.T) {
	if min(1, 2) != 1 {
		t.Error("min(1,2) should be 1")
	}
	if min(5, 3) != 3 {
		t.Error("min(5,3) should be 3")
	}
	if min(4, 4) != 4 {
		t.Error("min(4,4) should be 4")
	}
	if min(-3, -1) != -3 {
		t.Error("min(-3,-1) should be -3")
	}
}

func TestCheckShape(t *testing.T) {
	if err := checkShape("test", Shape{2, 3}, 2, 3); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := checkShape("test", Shape{2}, 2, 3); err == nil {
		t.Error("expected error for axis count mismatch")
	}
	if err := checkShape("test", Shape{2, 4}, 2, 3); err == nil {
		t.Error("expected error for dimension mismatch")
	}
}

func TestTensorStrides(t *testing.T) {
	a := New(DtypeFloat32, Shape{2, 3})
	s := a.Strides()
	assertEq(t, len(s), 2)
	assertEq(t, s[1], 4)
	assertEq(t, s[0], 12)
	b := New(DtypeFloat64, Shape{4})
	s2 := b.Strides()
	assertEq(t, s2[0], 8)
}

func TestTensorIsContiguous(t *testing.T) {
	a := New(DtypeFloat32, Shape{3, 4})
	if !a.IsContiguous() {
		t.Error("new tensor should be contiguous")
	}
}

func TestTensorSlice(t *testing.T) {
	a := New(DtypeFloat32, Shape{3, 4})
	for i := range a.Float32s() {
		a.Float32s()[i] = float32(i)
	}
	// Row-major: row 0 = [0,1,2,3], row 1 = [4,5,6,7], row 2 = [8,9,10,11]
	row1 := a.Slice(map[int]int{0: 1})
	assertEq(t, row1.Rank(), 1)
	assertEq(t, row1.Shape()[0], 4)
	assertEq32(t, row1.Float32s()[0], 4, 1e-6)
	assertEq32(t, row1.Float32s()[3], 7, 1e-6)
	assertPanic(t, func() { a.Slice(map[int]int{0: 10}) })
	assertPanic(t, func() { a.Slice(map[int]int{5: 0}) })

	// Multi-axis slice (scalar result)
	cell := a.Slice(map[int]int{0: 2, 1: 3})
	assertEq(t, cell.Rank(), 1)
	assertEq(t, cell.Shape()[0], 1)
	assertEq32(t, cell.Float32s()[0], 11, 1e-6)
}

func TestFloat64s(t *testing.T) {
	a := New(DtypeFloat64, Shape{3})
	a.Float64s()[0] = 1.5
	a.Float64s()[1] = 2.5
	a.Float64s()[2] = 3.5
	assertEq64 := func(got, want float64) {
		t.Helper()
		if got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
	assertEq64(a.Float64s()[0], 1.5)
	assertEq64(a.Float64s()[2], 3.5)
	assertPanic(t, func() { New(DtypeFloat32, Shape{2}).Float64s() })
}

func TestNewEmptyShape(t *testing.T) {
	a := New(DtypeFloat32, Shape{})
	assertEq(t, a.Rank(), 1)
	assertEq(t, a.Shape()[0], 1)
	assertEq(t, a.NumElements(), 1)
}

func TestCloneNilLabels(t *testing.T) {
	a := New(DtypeFloat32, Shape{2})
	a.Float32s()[0] = 42
	b := a.Clone()
	if b.Labels() != nil {
		t.Error("clone should have nil labels when source has nil labels")
	}
	assertEq32(t, b.Float32s()[0], 42, 1e-6)
}

func TestSubMulDiv(t *testing.T) {
	a := New(DtypeFloat32, Shape{3})
	a.Float32s()[0] = 10
	a.Float32s()[1] = 20
	a.Float32s()[2] = 30
	b := New(DtypeFloat32, Shape{3})
	b.Float32s()[0] = 1
	b.Float32s()[1] = 4
	b.Float32s()[2] = 6
	sub, _ := Sub(a, b)
	assertEq32(t, sub.Float32s()[0], 9, 1e-6)
	assertEq32(t, sub.Float32s()[2], 24, 1e-6)
	mul, _ := Mul(a, b)
	assertEq32(t, mul.Float32s()[0], 10, 1e-6)
	assertEq32(t, mul.Float32s()[2], 180, 1e-6)
	div, _ := Div(a, b)
	assertEq32(t, div.Float32s()[0], 10, 1e-6)
	assertEq32(t, div.Float32s()[2], 5, 1e-6)
}

func TestTanAsinAcosAtan(t *testing.T) {
	a := New(DtypeFloat32, Shape{3})
	a.Float32s()[0] = 0
	a.Float32s()[1] = 0.5
	a.Float32s()[2] = -0.5
	tan, err := Tan(a)
	assertNoErr(t, err)
	assertEq32(t, tan.Float32s()[0], 0, 1e-4)
	asin, err := Asin(a)
	assertNoErr(t, err)
	assertEq32(t, asin.Float32s()[0], 0, 1e-4)
	acos, err := Acos(New(DtypeFloat32, Shape{1}))
	assertNoErr(t, err)
	_ = acos
	atan, err := Atan(a)
	assertNoErr(t, err)
	assertEq32(t, atan.Float32s()[0], 0, 1e-4)
}

func TestTanGo(t *testing.T) {
	v, err := tanGo(0)
	assertNoErr(t, err)
	if v != 0 {
		t.Errorf("tanGo(0) = %v, want 0", v)
	}
	v, err = tanGo(0.7853981633974483) // pi/4
	assertNoErr(t, err)
	if v < 0.9 || v > 1.1 {
		t.Errorf("tanGo(pi/4) = %v, want ~1", v)
	}
}

func TestAsinGo(t *testing.T) {
	if v := asinGo(0); v != 0 {
		t.Errorf("asinGo(0) = %v, want 0", v)
	}
	if v := asinGo(1); v < 1.5 || v > 1.58 {
		t.Errorf("asinGo(1) = %v, want ~1.57", v)
	}
	if v := asinGo(-1); v > -1.5 || v < -1.58 {
		t.Errorf("asinGo(-1) = %v, want ~-1.57", v)
	}
}

func TestAcosGo(t *testing.T) {
	if v := acosGo(1); v != 0 {
		t.Errorf("acosGo(1) = %v, want 0", v)
	}
	if v := acosGo(0); v < 1.5 || v > 1.58 {
		t.Errorf("acosGo(0) = %v, want ~1.57", v)
	}
}

func TestAtanGo(t *testing.T) {
	if v := atanGo(0); v != 0 {
		t.Errorf("atanGo(0) = %v, want 0", v)
	}
	if v := atanGo(0.5); v < 0.4 || v > 0.5 {
		t.Errorf("atanGo(0.5) = %v, want ~0.46", v)
	}
	if v := atanGo(2); v < 1.1 || v > 1.2 {
		t.Errorf("atanGo(2) = %v, want ~1.107", v)
	}
	if v := atanGo(-2); v > -1.1 || v < -1.2 {
		t.Errorf("atanGo(-2) = %v, want ~-1.107", v)
	}
	if v := atanGo(10); v < 1.4 || v > 1.5 {
		t.Errorf("atanGo(10) = %v, want ~1.47", v)
	}
}

func TestReshapeFunction(t *testing.T) {
	a := New(DtypeFloat32, Shape{2, 3})
	b := Reshape(a, Shape{6})
	assertEq(t, b.Shape()[0], 6)
	assertEq(t, b.Rank(), 1)
}

func TestContractGenericNonFloat32(t *testing.T) {
	a := New(DtypeFloat64, Shape{2})
	b := New(DtypeFloat64, Shape{2})
	a.SetLabels([]string{"i"})
	b.SetLabels([]string{"i"})
	assertPanic(t, func() {
		TensorContract(a, b, []string{"i"}, nil)
	})
}

func TestOptimizeNilGraph(t *testing.T) {
	opt := NewOptimizer()
	if g := opt.Optimize(nil); g != nil {
		t.Error("expected nil for nil graph")
	}
}

func TestApplyDefaultRule(t *testing.T) {
	opt := &Optimizer{Rules: []RewriteRule{RuleReshapeOfReshape}}
	in := NewInput(0, DtypeFloat32, Shape{2})
	g := opt.Optimize(NewGraph(in))
	if g.Root.Kind != OpInput {
		t.Error("expected unchanged input for unimplemented rule")
	}
}

func TestNodeKeyAllTypes(t *testing.T) {
	tests := []struct {
		node *IRNode
		key  string
	}{
		{NewInput(0, DtypeFloat32, Shape{2}), "input[0]"},
		{NewConstant(New(DtypeFloat32, Shape{2})), "const[float32[2]]"},
		{NewContract(NewInput(0, DtypeFloat32, Shape{2}), NewInput(1, DtypeFloat32, Shape{2}), []string{"i"}, []string{}), "contract[[i][]]"},
		{NewTranspose(NewInput(0, DtypeFloat32, Shape{2}), []int{0}), "transpose[0]"},
		{NewReshape(NewInput(0, DtypeFloat32, Shape{2}), Shape{2}), "reshape[2]"},
		{NewElementwise("sin", NewInput(0, DtypeFloat32, Shape{2})), "elem[sin]"},
		{NewReduce(NewInput(0, DtypeFloat32, Shape{2}), 0, "sum"), "reduce[sum0]"},
	}
	for _, tc := range tests {
		if got := nodeKey(tc.node); got != tc.key {
			t.Errorf("nodeKey(%v) = %q, want %q", tc.node, got, tc.key)
		}
	}
}

func TestNodeKeyUnknown(t *testing.T) {
	n := &IRNode{Kind: OpKind(255)}
	if k := nodeKey(n); k != "unknown" {
		t.Errorf("expected unknown, got %q", k)
	}
}

func TestCopyNode(t *testing.T) {
	if n := copyNode(nil); n != nil {
		t.Error("copyNode(nil) should be nil")
	}
	in := NewInput(0, DtypeFloat32, Shape{2, 3})
	cp := copyNode(in)
	if cp.Kind != OpInput || cp.InputIdx != 0 || cp.Shape[0] != 2 {
		t.Error("copyNode did not preserve fields")
	}
	cp.Shape[0] = 99
	if in.Shape[0] == 99 {
		t.Error("copyNode did not deep-copy shape")
	}

	ct := NewContract(in, in, []string{"i"}, []string{"j"})
	cp2 := copyNode(ct)
	if cp2.Kind != OpContract || cp2.SumLabels[0] != "i" {
		t.Error("copyNode contract fields not preserved")
	}

	tr := NewTranspose(in, []int{1, 0})
	cp3 := copyNode(tr)
	if cp3.Perm[0] != 1 {
		t.Error("copyNode transpose fields not preserved")
	}

	rd := NewReduce(in, 0, "sum")
	cp4 := copyNode(rd)
	if cp4.ReduceAxis != 0 || cp4.ReduceOp != "sum" {
		t.Error("copyNode reduce fields not preserved")
	}

	ew := NewElementwise("sin", in)
	cp5 := copyNode(ew)
	if cp5.ElemOp != "sin" {
		t.Error("copyNode elementwise fields not preserved")
	}

	cn := NewConstant(New(DtypeFloat32, Shape{3}))
	cp6 := copyNode(cn)
	if cp6.ConstVal == nil {
		t.Error("copyNode constant not preserved")
	}
}

func TestRewriteMulByZeroNonMul(t *testing.T) {
	in := NewInput(0, DtypeFloat32, Shape{2})
	ew := NewElementwise("add", in, in)
	if r := rewriteMulByZero(ew); r != nil {
		t.Error("expected nil for non-mul node")
	}
}

func TestRewriteAddZeroNonAdd(t *testing.T) {
	in := NewInput(0, DtypeFloat32, Shape{2})
	if r := rewriteAddZero(in); r != nil {
		t.Error("expected nil for non-elementwise node")
	}
}

func TestIsZeroTensorEdgeCases(t *testing.T) {
	if isZeroTensor(NewInput(0, DtypeFloat32, Shape{2})) {
		t.Error("input is not a zero tensor")
	}
	cn := &IRNode{Kind: OpConstant, ConstVal: nil}
	if isZeroTensor(cn) {
		t.Error("nil ConstVal should not be zero")
	}
	cn2 := NewConstant(New(DtypeFloat64, Shape{2}))
	if isZeroTensor(cn2) {
		t.Error("float64 zero not detected by float32-only check")
	}
	cn3 := NewConstant(New(DtypeFloat32, Shape{2}))
	if !isZeroTensor(cn3) {
		t.Error("zero-initialized float32 tensor should be zero")
	}
	cn4 := NewConstant(func() *Tensor {
		t := New(DtypeFloat32, Shape{2})
		t.Float32s()[0] = 1
		return t
	}())
	if isZeroTensor(cn4) {
		t.Error("non-zero tensor should not be zero")
	}
}

func TestComposePermMismatched(t *testing.T) {
	p := composePerm([]int{0, 1}, []int{0})
	assertEq(t, len(p), 2)
	if p[0] != 0 || p[1] != 1 {
		t.Error("mismatched perm should return outer unchanged")
	}
}

func TestElementwiseUnaryNonFloat32(t *testing.T) {
	a := New(DtypeFloat64, Shape{2})
	_, err := Sin(a)
	if err == nil {
		t.Error("expected error for float64 unary")
	}
}

func TestElementwiseBinaryNonFloat32(t *testing.T) {
	a := New(DtypeFloat64, Shape{2})
	b := New(DtypeFloat64, Shape{2})
	_, err := Add(a, b)
	if err == nil {
		t.Error("expected error for float64 binary")
	}
}

func TestTransposeNonFloat32(t *testing.T) {
	a := New(DtypeFloat64, Shape{2, 2})
	_, err := Transpose(a, []int{1, 0})
	if err == nil {
		t.Error("expected error for float64 transpose")
	}
}

func TestReduceSumNonFloat32(t *testing.T) {
	a := New(DtypeFloat64, Shape{2})
	_, err := ReduceSum(a, 0)
	if err == nil {
		t.Error("expected error for float64 reduce")
	}
}

func TestBroadcastShapeNonBroadcastable(t *testing.T) {
	_, err := broadcastShapes(Shape{2, 3}, Shape{4, 5})
	if err == nil {
		t.Error("expected error for non-broadcastable shapes")
	}
}

func TestTransposePermLengthError(t *testing.T) {
	a := New(DtypeFloat32, Shape{2, 3})
	_, err := Transpose(a, []int{0})
	if err == nil {
		t.Error("expected error for short perm")
	}
}

func TestMatMulRankError(t *testing.T) {
	a := New(DtypeFloat32, Shape{3})
	b := New(DtypeFloat32, Shape{3, 2})
	_, err := MatMul(a, b)
	if err == nil {
		t.Error("expected error for non-2D matmul")
	}
}

func TestSetLabelsMismatchPanic(t *testing.T) {
	assertPanic(t, func() {
		New(DtypeFloat32, Shape{2, 3}).SetLabels([]string{"x"})
	})
}

func TestNewElementwiseMultiArg(t *testing.T) {
	a := NewInput(0, DtypeFloat32, Shape{4})
	b := NewInput(1, DtypeFloat64, Shape{4})
	c := NewInput(2, DtypeFloat32, Shape{4})
	ew := NewElementwise("add", a, b, c)
	if ew.Dtype != DtypeFloat64 {
		t.Errorf("expected float64 promotion, got %s", ew.Dtype)
	}
}

func TestInitMathDispatch(t *testing.T) {
	InitMathDispatch(false)
	if mathImpl != MathGo {
		t.Error("expected Go math after InitMathDispatch(false)")
	}
	InitMathDispatch(true)
	if mathImpl != MathSIMD {
		t.Error("expected SIMD math after InitMathDispatch(true)")
	}
	// Reset for later tests
	InitMathDispatch(false)
}

func TestGemmCorrectness(t *testing.T) {
	sizes := []struct{ m, n, k int }{
		{4, 8, 1},
		{4, 8, 2},
		{4, 8, 7},
		{4, 8, 16},
		{8, 8, 8},
		{16, 16, 16},
		{32, 32, 32},
		{16, 32, 8},
		{7, 11, 5},
		{64, 64, 64},
		{100, 80, 60},
		{64, 64, 129},
		{128, 128, 256},
		{64, 64, 300},
	}
	rng := rand.New(rand.NewSource(42))
	for _, sz := range sizes {
		a := New(DtypeFloat32, Shape{sz.m, sz.k})
		b := New(DtypeFloat32, Shape{sz.k, sz.n})
		out := New(DtypeFloat32, Shape{sz.m, sz.n})
		for i := range a.Float32s() {
			a.Float32s()[i] = rng.Float32()
		}
		for i := range b.Float32s() {
			b.Float32s()[i] = rng.Float32()
		}
		// Compute via assembly-backed matMulTiledAMD64
		copy(out.Float32s(), make([]float32, out.NumElements()))
		ok := matMulTiledAMD64(a, b, out, sz.m, sz.n, sz.k)
		if !ok {
			t.Errorf("matMulTiledAMD64 returned false for %dx%dx%d", sz.m, sz.n, sz.k)
		}
		// Compute via generic matMulGeneric
		outRef := New(DtypeFloat32, Shape{sz.m, sz.n})
		ok = matMulGeneric(a, b, outRef, sz.m, sz.n, sz.k)
		if !ok {
			t.Errorf("matMulGeneric returned false for %dx%dx%d", sz.m, sz.n, sz.k)
		}
		// Compare with tolerance (different FMA accumulation orders)
		for i := range out.Float32s() {
			got := float64(out.Float32s()[i])
			want := float64(outRef.Float32s()[i])
			diff := got - want
			if diff < 0 {
				diff = -diff
			}
			rel := diff
			if want != 0 {
				rel = diff / math.Abs(want)
			}
			if rel > 1e-4 {
				t.Errorf("mismatch at [%d] for %dx%dx%d: got %f, want %f (diff %e, rel %e)",
					i, sz.m, sz.n, sz.k, got, want, diff, rel)
				break
			}
		}
	}
}
