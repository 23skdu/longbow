package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/cmplx"
	"os"
	"time"

	"github.com/23skdu/longbow/internal/tensor"
)

type VerificationResult struct {
	Name     string        `json:"name"`
	Category string        `json:"category"`
	Passed   bool          `json:"passed"`
	Duration time.Duration `json:"duration_ns"`
	Details  string        `json:"details,omitempty"`
	Error    string        `json:"error,omitempty"`
}

type SummaryReport struct {
	Total    int                  `json:"total"`
	Passed   int                  `json:"passed"`
	Failed   int                  `json:"failed"`
	Duration time.Duration        `json:"duration_ns"`
	Results  []VerificationResult `json:"results"`
}

func shapeEqual(a, b tensor.Shape) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func atFloat64(t *tensor.Tensor, indices ...int) float64 {
	switch t.Dtype() {
	case tensor.DtypeFloat64:
		return *(*float64)(t.At(indices...))
	case tensor.DtypeFloat32:
		return float64(*(*float32)(t.At(indices...)))
	default:
		return 0
	}
}

func main() {
	jsonOutput := flag.Bool("json", false, "Output results in JSON format")
	categoryFilter := flag.String("category", "all", "Filter by category: all, core, ops, linalg, einsum, optimizer, calculus, multidtype, hardware")
	flag.Parse()

	// Initialize fast hardware math dispatch for math functions
	tensor.InitMathDispatch(true)

	start := time.Now()
	var results []VerificationResult

	tests := []struct {
		name     string
		category string
		fn       func() error
	}{
		// 1. Core Tensors & Type System
		{"Tensor Creation (Ranks 1D to 4D)", "core", testCoreCreation},
		{"All Supported Dtypes (Float32/64, Complex64/128, Int64/32/8, Uint8)", "core", testAllDtypes},
		{"Memory Layout, Strides & Zero-Copy Slicing", "core", testMemoryLayoutAndSlicing},
		{"Scalar Accessors (At, Direct Typed Slices)", "core", testScalarAccessors},
		{"Reshape, Transpose & Axis Permutation", "core", testReshapeTransposePermute},
		{"Index Labels & Label Integrity", "core", testIndexLabels},

		// 2. Elementwise Operations & Special Functions
		{"Elementwise Binary Arithmetic (Add, Sub, Mul, Div, Pow)", "ops", testElementwiseBinary},
		{"Broadcasting Semantics ([2, 3] with [3] and [1, 3])", "ops", testBroadcasting},
		{"Elementwise Unary Transcendental (Sin, Cos, Tan, Exp, Log, Sqrt)", "ops", testElementwiseUnary},
		{"Hyperbolic & Error Functions (Sinh, Cosh, Tanh, Erf)", "ops", testHyperbolicAndErf},
		{"Tensor Reductions (ReduceSum along axes)", "ops", testTensorReductions},

		// 3. Linear Algebra & Contractions
		{"Matrix Multiplication (MatMul 2D)", "linalg", testMatMul},
		{"Vector Dot Product & Outer Product via Einsum", "linalg", testDotAndOuter},
		{"Contraction by Axis Labels (TensorContract)", "linalg", testTensorContract},

		// 4. Einstein Summation Engine (Einsum)
		{"Einsum: Matrix Multiplication ('ij,jk->ik')", "einsum", testEinsumMatMul},
		{"Einsum: Vector Dot Product ('i,i->')", "einsum", testEinsumDot},
		{"Einsum: Vector Outer Product ('i,j->ij')", "einsum", testEinsumOuter},
		{"Einsum: Matrix Transposition ('ij->ji')", "einsum", testEinsumTranspose},
		{"Einsum: Diagonal Extraction ('ii->i')", "einsum", testEinsumDiagonal},
		{"Einsum: Matrix Trace ('ii->')", "einsum", testEinsumTrace},
		{"Einsum: Multi-Tensor Contraction Chain ('ij,jk,kl->il')", "einsum", testEinsumMultiChain},
		{"Einsum: Path Optimization (OptimizePath FLOP Reduction)", "einsum", testEinsumOptimizePath},

		// 5. Computational DAG & Optimizer
		{"DAG Graph Construction & Node Hierarchy", "optimizer", testDAGConstruction},
		{"Common Subexpression Elimination (CSE)", "optimizer", testOptimizerCSE},
		{"Constant Subgraph Folding", "optimizer", testConstantFolding},
		{"Algebraic Rewriting: Mul by Zero (A * 0 -> 0)", "optimizer", testRewriteMulZero},
		{"Algebraic Rewriting: Add Zero (A + 0 -> A)", "optimizer", testRewriteAddZero},
		{"Algebraic Rewriting: Double Negation (-(-A) -> A)", "optimizer", testRewriteDoubleNeg},
		{"Algebraic Rewriting: Double Transpose Identity (T(T(A)) -> A)", "optimizer", testRewriteDoubleTranspose},

		// 6. Relativistic & Differential Geometry Calculus
		{"Levi-Civita 3D Permutation Symbol (Parity & 6 non-zeros)", "calculus", testLeviCivita3D},
		{"Levi-Civita 4D Permutation Symbol (Parity & 24 non-zeros)", "calculus", testLeviCivita4D},
		{"4D Minkowski Metric Inversion (eta^mu_rho eta_rho_nu = delta)", "calculus", testMetricInversion},
		{"Covariant & Contravariant Index Raising / Lowering (v^mu = eta^mu_nu v_nu)", "calculus", testIndexRaisingLowering},
		{"Relativistic 4-Momentum Invariant (p^mu p_mu = -m^2)", "calculus", testRelativisticInvariant},
		{"Christoffel Connection Symbols in Flat Spacetime (Gamma = 0)", "calculus", testChristoffelFlat},
		{"Riemann Curvature, Ricci Tensor & Ricci Scalar in Flat Spacetime (R = 0)", "calculus", testRiemannFlat},
		{"Exterior Calculus: Differential Forms Wedge Product (Antisymmetry)", "calculus", testWedgeProduct},

		// 7. Multi-Dtype Execution
		{"Multi-Dtype Contraction (Float64)", "multidtype", testMultiDtypeFloat64},
		{"Multi-Dtype Contraction (Complex128)", "multidtype", testMultiDtypeComplex},
		{"Multi-Dtype Contraction (Int64)", "multidtype", testMultiDtypeInt},

		// 8. Hardware Acceleration & Math Dispatch
		{"AVX2 SIMD GEMM Kernel vs Generic Verification", "hardware", testGemmAVX2},
		{"Hardware Math Dispatch (Fast Math vs Go Standard Library)", "hardware", testMathDispatch},
	}

	passed := 0
	failed := 0

	for _, t := range tests {
		if *categoryFilter != "all" && *categoryFilter != t.category {
			continue
		}

		t0 := time.Now()
		err := t.fn()
		elapsed := time.Since(t0)

		res := VerificationResult{
			Name:     t.name,
			Category: t.category,
			Duration: elapsed,
			Passed:   err == nil,
		}

		if err != nil {
			res.Error = err.Error()
			failed++
			if !*jsonOutput {
				fmt.Printf("  ❌ [%-10s] %-60s (FAIL: %v)\n", t.category, t.name, err)
			}
		} else {
			passed++
			if !*jsonOutput {
				fmt.Printf("  ✅ [%-10s] %-60s (%s)\n", t.category, t.name, elapsed.Round(time.Microsecond))
			}
		}

		results = append(results, res)
	}

	totalDuration := time.Since(start)

	report := SummaryReport{
		Total:    passed + failed,
		Passed:   passed,
		Failed:   failed,
		Duration: totalDuration,
		Results:  results,
	}

	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(report)
	} else {
		fmt.Printf("\n------------------------------------------------------------\n")
		fmt.Printf("Tensor Engine Verification: %d/%d passed (%d failed) in %s\n",
			passed, passed+failed, failed, totalDuration.Round(time.Millisecond))
		fmt.Printf("------------------------------------------------------------\n")
	}

	if failed > 0 {
		os.Exit(1)
	}
}

// ----------------------------------------------------------------------------
// Test Implementations
// ----------------------------------------------------------------------------

func testCoreCreation() error {
	s1 := tensor.New(tensor.DtypeFloat32, tensor.Shape{5})
	if s1.Rank() != 1 || s1.NumElements() != 5 {
		return fmt.Errorf("1D vector failed: rank=%d len=%d", s1.Rank(), s1.NumElements())
	}
	s2 := tensor.New(tensor.DtypeFloat32, tensor.Shape{3, 4})
	if s2.Rank() != 2 || s2.NumElements() != 12 {
		return fmt.Errorf("2D matrix failed: rank=%d len=%d", s2.Rank(), s2.NumElements())
	}
	s4 := tensor.New(tensor.DtypeFloat32, tensor.Shape{2, 3, 4, 5})
	if s4.Rank() != 4 || s4.NumElements() != 120 {
		return fmt.Errorf("4D tensor failed: rank=%d len=%d", s4.Rank(), s4.NumElements())
	}
	return nil
}

func testAllDtypes() error {
	dtypes := []tensor.Dtype{
		tensor.DtypeFloat32, tensor.DtypeFloat64,
		tensor.DtypeComplex64, tensor.DtypeComplex128,
		tensor.DtypeInt64, tensor.DtypeInt32,
		tensor.DtypeInt8, tensor.DtypeUint8,
	}
	for _, dt := range dtypes {
		t := tensor.New(dt, tensor.Shape{2, 3})
		if t.Dtype() != dt {
			return fmt.Errorf("expected dtype %v, got %v", dt, t.Dtype())
		}
		if t.NumElements() != 6 {
			return fmt.Errorf("expected 6 elements, got %d", t.NumElements())
		}
		expectedBytes := 6 * dt.Size()
		if len(t.Data()) != expectedBytes {
			return fmt.Errorf("byte size mismatch for %v: %d vs %d", dt, len(t.Data()), expectedBytes)
		}
	}
	return nil
}

func testMemoryLayoutAndSlicing() error {
	t := tensor.New(tensor.DtypeFloat32, tensor.Shape{3, 4})
	floats := t.Float32s()
	for i := range floats {
		floats[i] = float32(i)
	}

	clone := t.Clone()
	clone.Float32s()[0] = 999.0
	if t.Float32s()[0] == 999.0 {
		return fmt.Errorf("clone is not independent from original tensor")
	}

	// Slice row 1 -> shape [4]
	row1 := t.Slice(map[int]int{0: 1})
	if row1.Rank() != 1 || row1.NumElements() != 4 {
		return fmt.Errorf("slice failed: rank=%d shape=%v", row1.Rank(), row1.Shape())
	}
	if row1.Float32s()[0] != 4.0 || row1.Float32s()[3] != 7.0 {
		return fmt.Errorf("slice data mismatch: %v", row1.Float32s())
	}
	return nil
}

func testScalarAccessors() error {
	t := tensor.New(tensor.DtypeFloat64, tensor.Shape{2, 2})
	f64s := t.Float64s()
	f64s[0], f64s[1], f64s[2], f64s[3] = 10.0, 20.0, 30.0, 40.0

	v10 := atFloat64(t, 0, 0)
	if v10 != 10.0 {
		return fmt.Errorf("At(0,0) failed: got %v", v10)
	}
	v40 := atFloat64(t, 1, 1)
	if v40 != 40.0 {
		return fmt.Errorf("At(1,1) failed: got %v", v40)
	}
	return nil
}

func testReshapeTransposePermute() error {
	t := tensor.New(tensor.DtypeFloat32, tensor.Shape{2, 6})
	for i := range t.Float32s() {
		t.Float32s()[i] = float32(i)
	}

	r := t.Reshape(tensor.Shape{3, 4})
	if r.Shape()[0] != 3 || r.Shape()[1] != 4 {
		return fmt.Errorf("reshape failed: shape=%v", r.Shape())
	}

	tr, err := tensor.Transpose(t, []int{1, 0})
	if err != nil || tr.Shape()[0] != 6 || tr.Shape()[1] != 2 {
		return fmt.Errorf("transpose failed: shape=%v err=%v", tr.Shape(), err)
	}
	// Verify transposition: t[1, 2] = 1*6 + 2 = 8 => tr[2, 1] should be 8
	trVal := atFloat64(tr, 2, 1)
	if trVal != 8.0 {
		return fmt.Errorf("tr.At(2, 1) expected 8.0, got %f", trVal)
	}
	return nil
}

func testIndexLabels() error {
	t := tensor.New(tensor.DtypeFloat32, tensor.Shape{2, 3})
	t.SetLabels([]string{"i", "j"})
	if len(t.Labels()) != 2 || t.Labels()[0] != "i" || t.Labels()[1] != "j" {
		return fmt.Errorf("Labels mismatch: %v", t.Labels())
	}
	return nil
}

func testElementwiseBinary() error {
	a := tensor.New(tensor.DtypeFloat32, tensor.Shape{2, 2})
	b := tensor.New(tensor.DtypeFloat32, tensor.Shape{2, 2})
	for i := range a.Float32s() {
		a.Float32s()[i] = float32(i + 1)
		b.Float32s()[i] = float32(10)
	}

	sum, err := tensor.Add(a, b)
	if err != nil || sum.Float32s()[0] != 11.0 {
		return fmt.Errorf("Add failed: %v", err)
	}

	prod, err := tensor.Mul(a, b)
	if err != nil || prod.Float32s()[0] != 10.0 || prod.Float32s()[3] != 40.0 {
		return fmt.Errorf("Mul failed: %v", err)
	}

	diff, err := tensor.Sub(b, a)
	if err != nil || diff.Float32s()[0] != 9.0 {
		return fmt.Errorf("Sub failed: %v", err)
	}

	div, err := tensor.Div(b, a)
	if err != nil || div.Float32s()[0] != 10.0 {
		return fmt.Errorf("Div failed: %v", err)
	}
	return nil
}

func testBroadcasting() error {
	a := tensor.New(tensor.DtypeFloat32, tensor.Shape{2, 3})
	b := tensor.New(tensor.DtypeFloat32, tensor.Shape{3})
	for i := range a.Float32s() {
		a.Float32s()[i] = 1.0
	}
	for i := range b.Float32s() {
		b.Float32s()[i] = float32(i + 1)
	}

	res, err := tensor.Add(a, b)
	if err != nil {
		return fmt.Errorf("broadcasting add failed: %v", err)
	}
	if !shapeEqual(res.Shape(), tensor.Shape{2, 3}) {
		return fmt.Errorf("expected shape [2, 3], got %v", res.Shape())
	}
	// Row 0: [1+1, 1+2, 1+3] = [2, 3, 4]
	if res.Float32s()[0] != 2.0 || res.Float32s()[1] != 3.0 || res.Float32s()[2] != 4.0 {
		return fmt.Errorf("broadcasting result mismatch: %v", res.Float32s()[:3])
	}
	return nil
}

func testElementwiseUnary() error {
	t := tensor.New(tensor.DtypeFloat32, tensor.Shape{3})
	t.Float32s()[0] = 0.0
	t.Float32s()[1] = float32(math.Pi / 2)
	t.Float32s()[2] = float32(math.Pi)

	sinT, err := tensor.Sin(t)
	if err != nil {
		return fmt.Errorf("Sin failed: %v", err)
	}
	if math.Abs(float64(sinT.Float32s()[0])) > 1e-4 || math.Abs(float64(sinT.Float32s()[1]-1.0)) > 1e-4 || math.Abs(float64(sinT.Float32s()[2])) > 1e-4 {
		return fmt.Errorf("Sin output mismatch: %v", sinT.Float32s())
	}

	expT, err := tensor.Exp(tensor.New(tensor.DtypeFloat32, tensor.Shape{1}))
	if err != nil || math.Abs(float64(expT.Float32s()[0]-1.0)) > 1e-4 {
		return fmt.Errorf("Exp(0) != 1.0: %v", err)
	}
	return nil
}

func testHyperbolicAndErf() error {
	t := tensor.New(tensor.DtypeFloat32, tensor.Shape{1})
	t.Float32s()[0] = 0.0

	sinhT, err := tensor.Sinh(t)
	if err != nil || math.Abs(float64(sinhT.Float32s()[0])) > 1e-4 {
		return fmt.Errorf("Sinh(0) != 0: %v", err)
	}

	coshT, err := tensor.Cosh(t)
	if err != nil || math.Abs(float64(coshT.Float32s()[0]-1.0)) > 1e-4 {
		return fmt.Errorf("Cosh(0) != 1: %v", err)
	}

	erfT, err := tensor.Erf(t)
	if err != nil || math.Abs(float64(erfT.Float32s()[0])) > 1e-4 {
		return fmt.Errorf("Erf(0) != 0: %v", err)
	}
	return nil
}

func testTensorReductions() error {
	t := tensor.New(tensor.DtypeFloat32, tensor.Shape{2, 3})
	for i := range t.Float32s() {
		t.Float32s()[i] = float32(i + 1) // 1..6
	}

	sumAxis0, err := tensor.ReduceSum(t, 0)
	if err != nil || len(sumAxis0.Float32s()) != 3 {
		return fmt.Errorf("ReduceSum axis 0 failed: %v", err)
	}
	// [1+4, 2+5, 3+6] = [5, 7, 9]
	if sumAxis0.Float32s()[0] != 5.0 || sumAxis0.Float32s()[1] != 7.0 || sumAxis0.Float32s()[2] != 9.0 {
		return fmt.Errorf("ReduceSum axis 0 output mismatch: %v", sumAxis0.Float32s())
	}

	totalSum, err := tensor.ReduceSum(sumAxis0, 0)
	if err != nil || totalSum.Float32s()[0] != 21.0 {
		return fmt.Errorf("total ReduceSum mismatch: %v", totalSum.Float32s())
	}
	return nil
}

func testMatMul() error {
	// A: 2x3, B: 3x2
	a := tensor.New(tensor.DtypeFloat32, tensor.Shape{2, 3})
	b := tensor.New(tensor.DtypeFloat32, tensor.Shape{3, 2})
	a.SetLabels([]string{"i", "k"})
	b.SetLabels([]string{"k", "j"})
	for i := range a.Float32s() {
		a.Float32s()[i] = 1.0
	}
	for i := range b.Float32s() {
		b.Float32s()[i] = 2.0
	}

	c, err := tensor.MatMul(a, b)
	if err != nil {
		return fmt.Errorf("MatMul failed: %v", err)
	}
	if !shapeEqual(c.Shape(), tensor.Shape{2, 2}) {
		return fmt.Errorf("expected shape [2, 2], got %v", c.Shape())
	}
	// Each element is 1*2 + 1*2 + 1*2 = 6
	for _, val := range c.Float32s() {
		if val != 6.0 {
			return fmt.Errorf("MatMul expected 6.0, got %f", val)
		}
	}
	return nil
}

func testDotAndOuter() error {
	u := tensor.New(tensor.DtypeFloat32, tensor.Shape{3})
	v := tensor.New(tensor.DtypeFloat32, tensor.Shape{3})
	u.Float32s()[0], u.Float32s()[1], u.Float32s()[2] = 1, 2, 3
	v.Float32s()[0], v.Float32s()[1], v.Float32s()[2] = 4, 5, 6

	dot, err := tensor.Einsum("i,i->", u, v)
	if err != nil || dot.Float32s()[0] != 32.0 {
		return fmt.Errorf("Dot product expected 32.0, got %v (err: %v)", dot.Float32s(), err)
	}

	outer, err := tensor.Einsum("i,j->ij", u, v)
	if err != nil || !shapeEqual(outer.Shape(), tensor.Shape{3, 3}) {
		return fmt.Errorf("Outer product failed: %v", err)
	}
	if outer.Float32s()[0] != 4.0 || outer.Float32s()[8] != 18.0 {
		return fmt.Errorf("Outer product output mismatch: %v", outer.Float32s())
	}
	return nil
}

func testTensorContract() error {
	a := tensor.New(tensor.DtypeFloat32, tensor.Shape{2, 3})
	a.SetLabels([]string{"i", "j"})
	b := tensor.New(tensor.DtypeFloat32, tensor.Shape{3, 4})
	b.SetLabels([]string{"j", "k"})
	for i := range a.Float32s() {
		a.Float32s()[i] = 1.0
	}
	for i := range b.Float32s() {
		b.Float32s()[i] = 1.0
	}

	c, err := tensor.TensorContract(a, b, []string{"j"}, []string{"i", "k"})
	if err != nil {
		return fmt.Errorf("TensorContract failed: %v", err)
	}
	if !shapeEqual(c.Shape(), tensor.Shape{2, 4}) {
		return fmt.Errorf("expected shape [2, 4], got %v", c.Shape())
	}
	if c.Float32s()[0] != 3.0 {
		return fmt.Errorf("expected 3.0, got %f", c.Float32s()[0])
	}
	return nil
}

func testEinsumMatMul() error {
	a := tensor.New(tensor.DtypeFloat32, tensor.Shape{2, 3})
	b := tensor.New(tensor.DtypeFloat32, tensor.Shape{3, 2})
	for i := range a.Float32s() {
		a.Float32s()[i] = 1.0
	}
	for i := range b.Float32s() {
		b.Float32s()[i] = 2.0
	}

	c, err := tensor.Einsum("ij,jk->ik", a, b)
	if err != nil || !shapeEqual(c.Shape(), tensor.Shape{2, 2}) {
		return fmt.Errorf("Einsum matmul failed: %v", err)
	}
	if c.Float32s()[0] != 6.0 {
		return fmt.Errorf("Einsum matmul expected 6.0, got %f", c.Float32s()[0])
	}
	return nil
}

func testEinsumDot() error {
	a := tensor.New(tensor.DtypeFloat32, tensor.Shape{4})
	b := tensor.New(tensor.DtypeFloat32, tensor.Shape{4})
	for i := 0; i < 4; i++ {
		a.Float32s()[i] = float32(i + 1)
		b.Float32s()[i] = 1.0
	}
	dot, err := tensor.Einsum("i,i->", a, b)
	if err != nil || dot.Float32s()[0] != 10.0 {
		return fmt.Errorf("Einsum dot expected 10.0, got %v", dot.Float32s())
	}
	return nil
}

func testEinsumOuter() error {
	a := tensor.New(tensor.DtypeFloat32, tensor.Shape{2})
	b := tensor.New(tensor.DtypeFloat32, tensor.Shape{3})
	a.Float32s()[0], a.Float32s()[1] = 1, 2
	b.Float32s()[0], b.Float32s()[1], b.Float32s()[2] = 3, 4, 5

	res, err := tensor.Einsum("i,j->ij", a, b)
	if err != nil || !shapeEqual(res.Shape(), tensor.Shape{2, 3}) {
		return fmt.Errorf("Einsum outer failed: %v", err)
	}
	return nil
}

func testEinsumTranspose() error {
	a := tensor.New(tensor.DtypeFloat32, tensor.Shape{2, 3})
	for i := range a.Float32s() {
		a.Float32s()[i] = float32(i)
	}
	at, err := tensor.Einsum("ij->ji", a)
	if err != nil || !shapeEqual(at.Shape(), tensor.Shape{3, 2}) {
		return fmt.Errorf("Einsum transpose failed: %v", err)
	}
	return nil
}

func testEinsumDiagonal() error {
	a := tensor.New(tensor.DtypeFloat32, tensor.Shape{3, 3})
	f := a.Float32s()
	f[0], f[4], f[8] = 11.0, 22.0, 33.0

	diag, err := tensor.Einsum("ii->i", a)
	if err != nil {
		return fmt.Errorf("Einsum diagonal failed: %v", err)
	}
	if !shapeEqual(diag.Shape(), tensor.Shape{3}) {
		return fmt.Errorf("expected shape [3], got %v", diag.Shape())
	}
	if diag.Float32s()[0] != 11.0 || diag.Float32s()[1] != 22.0 || diag.Float32s()[2] != 33.0 {
		return fmt.Errorf("diagonal mismatch: %v", diag.Float32s())
	}
	return nil
}

func testEinsumTrace() error {
	a := tensor.New(tensor.DtypeFloat32, tensor.Shape{3, 3})
	f := a.Float32s()
	f[0], f[4], f[8] = 5.0, 15.0, 25.0

	tr, err := tensor.Einsum("ii->", a)
	if err != nil {
		return fmt.Errorf("Einsum trace failed: %v", err)
	}
	if tr.Float32s()[0] != 45.0 {
		return fmt.Errorf("trace expected 45.0, got %f", tr.Float32s()[0])
	}
	return nil
}

func testEinsumMultiChain() error {
	a := tensor.New(tensor.DtypeFloat32, tensor.Shape{2, 3})
	b := tensor.New(tensor.DtypeFloat32, tensor.Shape{3, 4})
	c := tensor.New(tensor.DtypeFloat32, tensor.Shape{4, 2})
	for i := range a.Float32s() {
		a.Float32s()[i] = 1.0
	}
	for i := range b.Float32s() {
		b.Float32s()[i] = 1.0
	}
	for i := range c.Float32s() {
		c.Float32s()[i] = 1.0
	}

	res, err := tensor.Einsum("ij,jk,kl->il", a, b, c)
	if err != nil {
		return fmt.Errorf("Einsum contraction chain failed: %v", err)
	}
	if !shapeEqual(res.Shape(), tensor.Shape{2, 2}) {
		return fmt.Errorf("expected shape [2, 2], got %v", res.Shape())
	}
	if res.Float32s()[0] != 12.0 {
		return fmt.Errorf("expected 12.0, got %f", res.Float32s()[0])
	}
	return nil
}

func testEinsumOptimizePath() error {
	op, err := tensor.ParseEinsum("ij,jk,kl->il")
	if err != nil {
		return fmt.Errorf("ParseEinsum failed: %v", err)
	}
	shapes := []tensor.Shape{{10, 100}, {100, 5}, {5, 50}}
	path := op.OptimizePath(shapes)
	if len(path.Contracts) != 2 {
		return fmt.Errorf("expected 2 contraction steps, got %d", len(path.Contracts))
	}
	return nil
}

func testDAGConstruction() error {
	inA := tensor.NewInput(0, tensor.DtypeFloat32, tensor.Shape{2, 3})
	inA.OutLabels = []string{"i", "j"}
	inB := tensor.NewInput(1, tensor.DtypeFloat32, tensor.Shape{3, 4})
	inB.OutLabels = []string{"j", "k"}
	n3 := tensor.NewContract(inA, inB, []string{"j"}, []string{"i", "k"})
	g := tensor.NewGraph(n3)
	if g.Root == nil || !shapeEqual(g.Root.Shape, tensor.Shape{2, 4}) {
		return fmt.Errorf("DAG contract construction failed: got shape %v", g.Root.Shape)
	}
	return nil
}

func testOptimizerCSE() error {
	inA := tensor.NewInput(0, tensor.DtypeFloat32, tensor.Shape{4, 4})
	inB := tensor.NewInput(1, tensor.DtypeFloat32, tensor.Shape{4, 4})

	add1 := tensor.NewElementwise("add", inA, inB)
	add2 := tensor.NewElementwise("add", inA, inB)
	mul := tensor.NewElementwise("mul", add1, add2)

	g := tensor.NewGraph(mul)
	opt := tensor.NewOptimizer()
	optG := opt.Optimize(g)

	if optG.Root.Children[0] != optG.Root.Children[1] {
		return fmt.Errorf("CSE failed to identify duplicate subexpression")
	}
	return nil
}

func testConstantFolding() error {
	t1 := tensor.New(tensor.DtypeFloat32, tensor.Shape{2})
	t1.Float32s()[0] = 3.0
	t1.Float32s()[1] = 5.0
	t2 := tensor.New(tensor.DtypeFloat32, tensor.Shape{2})
	t2.Float32s()[0] = 4.0
	t2.Float32s()[1] = 2.0

	c1 := tensor.NewConstant(t1)
	c2 := tensor.NewConstant(t2)
	add := tensor.NewElementwise("add", c1, c2)

	g := tensor.NewGraph(add)
	opt := tensor.NewOptimizer()
	optG := opt.Optimize(g)

	if optG.Root.Kind != tensor.OpConstant || optG.Root.ConstVal == nil {
		return fmt.Errorf("constant folding failed")
	}
	if optG.Root.ConstVal.Float32s()[0] != 7.0 || optG.Root.ConstVal.Float32s()[1] != 7.0 {
		return fmt.Errorf("constant folding values mismatch")
	}
	return nil
}

func testRewriteMulZero() error {
	zero := tensor.NewConstant(tensor.New(tensor.DtypeFloat32, tensor.Shape{2}))
	in := tensor.NewInput(0, tensor.DtypeFloat32, tensor.Shape{2})
	mul := tensor.NewElementwise("mul", in, zero)

	opt := tensor.NewOptimizer()
	g := opt.Optimize(tensor.NewGraph(mul))
	if g.Root.Kind != tensor.OpConstant {
		return fmt.Errorf("A * 0 was not rewritten to OpConstant")
	}
	return nil
}

func testRewriteAddZero() error {
	zero := tensor.NewConstant(tensor.New(tensor.DtypeFloat32, tensor.Shape{2}))
	in := tensor.NewInput(0, tensor.DtypeFloat32, tensor.Shape{2})
	add := tensor.NewElementwise("add", in, zero)

	opt := tensor.NewOptimizer()
	g := opt.Optimize(tensor.NewGraph(add))
	if g.Root.Kind != tensor.OpInput {
		return fmt.Errorf("A + 0 was not rewritten to OpInput")
	}
	return nil
}

func testRewriteDoubleNeg() error {
	in := tensor.NewInput(0, tensor.DtypeFloat32, tensor.Shape{2})
	neg1 := tensor.NewElementwise("neg", in)
	neg2 := tensor.NewElementwise("neg", neg1)

	opt := tensor.NewOptimizer()
	g := opt.Optimize(tensor.NewGraph(neg2))
	if g.Root.Kind != tensor.OpInput {
		return fmt.Errorf("-(-A) was not rewritten to OpInput")
	}
	return nil
}

func testRewriteDoubleTranspose() error {
	in := tensor.NewInput(0, tensor.DtypeFloat32, tensor.Shape{2, 3})
	t1 := tensor.NewTranspose(in, []int{1, 0})
	t2 := tensor.NewTranspose(t1, []int{1, 0})

	opt := tensor.NewOptimizer()
	g := opt.Optimize(tensor.NewGraph(t2))
	if g.Root.Kind != tensor.OpInput {
		return fmt.Errorf("T(T(A)) was not rewritten to OpInput")
	}
	return nil
}

func testLeviCivita3D() error {
	lc, err := tensor.LeviCivita(3, tensor.DtypeFloat64)
	if err != nil {
		return fmt.Errorf("LeviCivita(3) failed: %v", err)
	}
	if lc.Rank() != 3 || !shapeEqual(lc.Shape(), tensor.Shape{3, 3, 3}) {
		return fmt.Errorf("invalid shape for 3D Levi-Civita: %v", lc.Shape())
	}

	e012 := atFloat64(lc, 0, 1, 2)
	e102 := atFloat64(lc, 1, 0, 2)
	e001 := atFloat64(lc, 0, 0, 1)
	e201 := atFloat64(lc, 2, 0, 1)

	if e012 != 1.0 || e102 != -1.0 || e001 != 0.0 || e201 != 1.0 {
		return fmt.Errorf("parity mismatch: e012=%f, e102=%f, e001=%f, e201=%f", e012, e102, e001, e201)
	}

	nonZeros := 0
	for _, v := range lc.Float64s() {
		if v != 0 {
			nonZeros++
		}
	}
	if nonZeros != 6 {
		return fmt.Errorf("expected 6 non-zero elements, got %d", nonZeros)
	}
	return nil
}

func testLeviCivita4D() error {
	lc, err := tensor.LeviCivita(4, tensor.DtypeFloat64)
	if err != nil {
		return fmt.Errorf("LeviCivita(4) failed: %v", err)
	}
	if lc.Rank() != 4 || !shapeEqual(lc.Shape(), tensor.Shape{4, 4, 4, 4}) {
		return fmt.Errorf("invalid shape for 4D Levi-Civita: %v", lc.Shape())
	}

	e0123 := atFloat64(lc, 0, 1, 2, 3)
	e1023 := atFloat64(lc, 1, 0, 2, 3)
	e0012 := atFloat64(lc, 0, 0, 1, 2)

	if e0123 != 1.0 || e1023 != -1.0 || e0012 != 0.0 {
		return fmt.Errorf("4D parity mismatch: e0123=%f, e1023=%f, e0012=%f", e0123, e1023, e0012)
	}

	nonZeros := 0
	for _, v := range lc.Float64s() {
		if v != 0 {
			nonZeros++
		}
	}
	if nonZeros != 24 {
		return fmt.Errorf("expected 24 non-zero elements, got %d", nonZeros)
	}
	return nil
}

func testMetricInversion() error {
	eta := tensor.New(tensor.DtypeFloat64, tensor.Shape{4, 4})
	etaF := eta.Float64s()
	etaF[0] = -1.0
	etaF[5] = 1.0
	etaF[10] = 1.0
	etaF[15] = 1.0

	invEta, err := tensor.InvertMetric2D(eta)
	if err != nil {
		return fmt.Errorf("InvertMetric2D failed: %v", err)
	}

	invF := invEta.Float64s()
	if invF[0] != -1.0 || invF[5] != 1.0 || invF[10] != 1.0 || invF[15] != 1.0 {
		return fmt.Errorf("inverse metric mismatch: diag = [%f, %f, %f, %f]", invF[0], invF[5], invF[10], invF[15])
	}
	return nil
}

func testIndexRaisingLowering() error {
	eta := tensor.New(tensor.DtypeFloat64, tensor.Shape{4, 4})
	etaF := eta.Float64s()
	etaF[0], etaF[5], etaF[10], etaF[15] = -1.0, 1.0, 1.0, 1.0

	invEta, _ := tensor.InvertMetric2D(eta)

	pCov := tensor.New(tensor.DtypeFloat64, tensor.Shape{4})
	pF := pCov.Float64s()
	pF[0], pF[1], pF[2], pF[3] = -10.0, 1.0, 2.0, 3.0

	pContra, err := tensor.MetricRaise(pCov, invEta, 0)
	if err != nil {
		return fmt.Errorf("MetricRaise failed: %v", err)
	}
	contraF := pContra.Float64s()
	if contraF[0] != 10.0 || contraF[1] != 1.0 || contraF[2] != 2.0 || contraF[3] != 3.0 {
		return fmt.Errorf("raised index mismatch: p^mu = %v", contraF)
	}

	pLowered, err := tensor.MetricLower(pContra, eta, 0)
	if err != nil {
		return fmt.Errorf("MetricLower failed: %v", err)
	}
	loweredF := pLowered.Float64s()
	if loweredF[0] != -10.0 || loweredF[1] != 1.0 || loweredF[2] != 2.0 || loweredF[3] != 3.0 {
		return fmt.Errorf("lowered index mismatch: p_mu = %v", loweredF)
	}
	return nil
}

func testRelativisticInvariant() error {
	eta := tensor.New(tensor.DtypeFloat64, tensor.Shape{4, 4})
	etaF := eta.Float64s()
	etaF[0], etaF[5], etaF[10], etaF[15] = -1.0, 1.0, 1.0, 1.0
	invEta, _ := tensor.InvertMetric2D(eta)

	pCov := tensor.New(tensor.DtypeFloat64, tensor.Shape{4})
	pF := pCov.Float64s()
	pF[0], pF[1], pF[2], pF[3] = -5.0, 3.0, 0.0, 0.0

	pContra, _ := tensor.MetricRaise(pCov, invEta, 0)

	invariant, err := tensor.Einsum("i,i->", pContra, pCov)
	if err != nil {
		return fmt.Errorf("invariant contraction failed: %v", err)
	}
	if invariant.Float64s()[0] != -16.0 {
		return fmt.Errorf("relativistic invariant mismatch: expected -16.0, got %f", invariant.Float64s()[0])
	}
	return nil
}

func testChristoffelFlat() error {
	dim := 4
	metric := tensor.New(tensor.DtypeFloat64, tensor.Shape{dim, dim})
	for i := 0; i < dim; i++ {
		metric.Float64s()[i*dim+i] = 1.0
	}
	dG := tensor.New(tensor.DtypeFloat64, tensor.Shape{dim, dim, dim})

	gamma, err := tensor.ChristoffelSymbols(metric, dG)
	if err != nil {
		return fmt.Errorf("ChristoffelSymbols failed: %v", err)
	}
	for _, v := range gamma.Float64s() {
		if v != 0 {
			return fmt.Errorf("Christoffel symbol in flat space must be 0, got %f", v)
		}
	}
	return nil
}

func testRiemannFlat() error {
	dim := 4
	gamma := tensor.New(tensor.DtypeFloat64, tensor.Shape{dim, dim, dim})
	dGamma := tensor.New(tensor.DtypeFloat64, tensor.Shape{dim, dim, dim, dim})

	riemann, err := tensor.RiemannCurvature(gamma, dGamma)
	if err != nil {
		return fmt.Errorf("RiemannCurvature failed: %v", err)
	}
	for _, v := range riemann.Float64s() {
		if v != 0 {
			return fmt.Errorf("Riemann curvature in flat space must be 0, got %f", v)
		}
	}

	ricci, err := tensor.RicciTensor(riemann)
	if err != nil {
		return fmt.Errorf("RicciTensor failed: %v", err)
	}
	for _, v := range ricci.Float64s() {
		if v != 0 {
			return fmt.Errorf("Ricci curvature in flat space must be 0, got %f", v)
		}
	}

	invG := tensor.New(tensor.DtypeFloat64, tensor.Shape{dim, dim})
	for i := 0; i < dim; i++ {
		invG.Float64s()[i*dim+i] = 1.0
	}
	scalar, err := tensor.RicciScalar(ricci, invG)
	if err != nil || scalar.Float64s()[0] != 0.0 {
		return fmt.Errorf("Ricci scalar in flat space must be 0, got %v", scalar.Float64s())
	}
	return nil
}

func testWedgeProduct() error {
	a := tensor.New(tensor.DtypeFloat64, tensor.Shape{3})
	b := tensor.New(tensor.DtypeFloat64, tensor.Shape{3})
	a.Float64s()[0], a.Float64s()[1], a.Float64s()[2] = 1.0, 2.0, 3.0
	b.Float64s()[0], b.Float64s()[1], b.Float64s()[2] = 4.0, 5.0, 6.0

	ab, err := tensor.WedgeProduct(a, b)
	if err != nil {
		return fmt.Errorf("WedgeProduct failed: %v", err)
	}
	ba, err := tensor.WedgeProduct(b, a)
	if err != nil {
		return fmt.Errorf("WedgeProduct(B,A) failed: %v", err)
	}

	for i := range ab.Float64s() {
		if math.Abs(ab.Float64s()[i]+ba.Float64s()[i]) > 1e-6 {
			return fmt.Errorf("wedge product antisymmetry violation at idx %d: A^B=%f, B^A=%f",
				i, ab.Float64s()[i], ba.Float64s()[i])
		}
	}

	aa, err := tensor.WedgeProduct(a, a)
	if err != nil {
		return fmt.Errorf("WedgeProduct(A,A) failed: %v", err)
	}
	for _, v := range aa.Float64s() {
		if math.Abs(v) > 1e-6 {
			return fmt.Errorf("A ^ A != 0, got %f", v)
		}
	}
	return nil
}

func testMultiDtypeFloat64() error {
	a := tensor.New(tensor.DtypeFloat64, tensor.Shape{2, 2})
	b := tensor.New(tensor.DtypeFloat64, tensor.Shape{2, 2})
	a.SetLabels([]string{"i", "k"})
	b.SetLabels([]string{"k", "j"})
	a.Float64s()[0], a.Float64s()[3] = 2.0, 3.0
	b.Float64s()[0], b.Float64s()[3] = 4.0, 5.0

	res, err := tensor.MatMul(a, b)
	if err != nil {
		return fmt.Errorf("Float64 MatMul failed: %v", err)
	}
	if res.Float64s()[0] != 8.0 || res.Float64s()[3] != 15.0 {
		return fmt.Errorf("Float64 MatMul output mismatch: %v", res.Float64s())
	}
	return nil
}

func testMultiDtypeComplex() error {
	a := tensor.New(tensor.DtypeComplex128, tensor.Shape{2, 2})
	b := tensor.New(tensor.DtypeComplex128, tensor.Shape{2, 2})
	a.SetLabels([]string{"i", "k"})
	b.SetLabels([]string{"k", "j"})
	a.Complex128s()[0] = complex(0, 1)
	b.Complex128s()[0] = complex(0, 1)
	a.Complex128s()[3] = complex(2, 3)
	b.Complex128s()[3] = complex(2, -3)

	c, err := tensor.MatMul(a, b)
	if err != nil {
		return fmt.Errorf("Complex128 MatMul failed: %v", err)
	}
	if cmplx.Abs(c.Complex128s()[0]-complex(-1, 0)) > 1e-6 {
		return fmt.Errorf("i * i != -1, got %v", c.Complex128s()[0])
	}
	if cmplx.Abs(c.Complex128s()[3]-complex(13, 0)) > 1e-6 {
		return fmt.Errorf("(2+3i)*(2-3i) != 13, got %v", c.Complex128s()[3])
	}
	return nil
}

func testMultiDtypeInt() error {
	a := tensor.New(tensor.DtypeInt64, tensor.Shape{2, 2})
	b := tensor.New(tensor.DtypeInt64, tensor.Shape{2, 2})
	a.SetLabels([]string{"i", "k"})
	b.SetLabels([]string{"k", "j"})
	a.Int64s()[0], a.Int64s()[1] = 10, 20
	b.Int64s()[0], b.Int64s()[2] = 2, 3

	c, err := tensor.MatMul(a, b)
	if err != nil {
		return fmt.Errorf("Int64 MatMul failed: %v", err)
	}
	if c.Int64s()[0] != 80 {
		return fmt.Errorf("Int64 MatMul expected 80, got %d", c.Int64s()[0])
	}
	return nil
}

func testGemmAVX2() error {
	sz := 64
	a := tensor.New(tensor.DtypeFloat32, tensor.Shape{sz, sz})
	b := tensor.New(tensor.DtypeFloat32, tensor.Shape{sz, sz})
	a.SetLabels([]string{"i", "k"})
	b.SetLabels([]string{"k", "j"})
	for i := range a.Float32s() {
		a.Float32s()[i] = 1.0
		b.Float32s()[i] = 2.0
	}

	c, err := tensor.MatMul(a, b)
	if err != nil {
		return fmt.Errorf("AVX2 MatMul failed: %v", err)
	}
	for _, v := range c.Float32s() {
		if v != 128.0 {
			return fmt.Errorf("AVX2 MatMul expected 128.0, got %f", v)
		}
	}
	return nil
}

func testMathDispatch() error {
	tensor.InitMathDispatch(true)
	t := tensor.New(tensor.DtypeFloat32, tensor.Shape{4})
	t.Float32s()[0] = 1.0
	t.Float32s()[1] = 2.0

	res, err := tensor.Sin(t)
	if err != nil {
		return fmt.Errorf("Sin with fast math failed: %v", err)
	}
	if len(res.Float32s()) != 4 {
		return fmt.Errorf("Sin length mismatch: %d", len(res.Float32s()))
	}
	return nil
}
