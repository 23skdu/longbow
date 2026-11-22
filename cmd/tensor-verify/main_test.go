package main

import (
	"testing"

	"github.com/23skdu/longbow/internal/tensor"
)

// TestShapeEqual runs the test shape equal operation.
func TestShapeEqual(t *testing.T) {
	tests := []struct {
		name string
		a, b tensor.Shape
		want bool
	}{
		{"equal", tensor.Shape{2, 3}, tensor.Shape{2, 3}, true},
		{"different dims", tensor.Shape{2, 3}, tensor.Shape{3, 2}, false},
		{"different lengths", tensor.Shape{2}, tensor.Shape{2, 3}, false},
		{"empty", tensor.Shape{}, tensor.Shape{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shapeEqual(tt.a, tt.b); got != tt.want {
				t.Errorf("shapeEqual(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// TestAtFloat64Float32 runs the test at float64 float32 operation.
func TestAtFloat64Float32(t *testing.T) {
	tensor.InitMathDispatch(true)
	tt := tensor.New(tensor.DtypeFloat32, tensor.Shape{2, 2})
	tt.Float32s()[0] = 1.5
	tt.Float32s()[3] = 4.5

	if got := atFloat64(tt, 0, 0); got != 1.5 {
		t.Errorf("atFloat64(0,0) = %v, want 1.5", got)
	}
	if got := atFloat64(tt, 1, 1); got != 4.5 {
		t.Errorf("atFloat64(1,1) = %v, want 4.5", got)
	}
}

// TestAtFloat64Float64 runs the test at float64 float64 operation.
func TestAtFloat64Float64(t *testing.T) {
	tensor.InitMathDispatch(true)
	tt := tensor.New(tensor.DtypeFloat64, tensor.Shape{2})
	tt.Float64s()[0] = 7.7
	tt.Float64s()[1] = 3.3

	if got := atFloat64(tt, 0); got != 7.7 {
		t.Errorf("atFloat64(0) = %v, want 7.7", got)
	}
	if got := atFloat64(tt, 1); got != 3.3 {
		t.Errorf("atFloat64(1) = %v, want 3.3", got)
	}
}

// TestAtFloat64UnsupportedDtype runs the test at float64 unsupported dtype operation.
func TestAtFloat64UnsupportedDtype(t *testing.T) {
	tensor.InitMathDispatch(true)
	tt := tensor.New(tensor.DtypeInt64, tensor.Shape{2})
	tt.Int64s()[0] = 42

	if got := atFloat64(tt, 0); got != 0 {
		t.Errorf("atFloat64 for unsupported dtype = %v, want 0", got)
	}
}

// TestTestCoreCreation runs the test test core creation operation.
func TestTestCoreCreation(t *testing.T) {
	if err := testCoreCreation(); err != nil {
		t.Errorf("testCoreCreation failed: %v", err)
	}
}

// TestTestAllDtypes runs the test test all dtypes operation.
func TestTestAllDtypes(t *testing.T) {
	if err := testAllDtypes(); err != nil {
		t.Errorf("testAllDtypes failed: %v", err)
	}
}

// TestTestMemoryLayoutAndSlicing runs the test test memory layout and slicing operation.
func TestTestMemoryLayoutAndSlicing(t *testing.T) {
	if err := testMemoryLayoutAndSlicing(); err != nil {
		t.Errorf("testMemoryLayoutAndSlicing failed: %v", err)
	}
}

// TestTestScalarAccessors runs the test test scalar accessors operation.
func TestTestScalarAccessors(t *testing.T) {
	if err := testScalarAccessors(); err != nil {
		t.Errorf("testScalarAccessors failed: %v", err)
	}
}

// TestTestReshapeTransposePermute runs the test test reshape transpose permute operation.
func TestTestReshapeTransposePermute(t *testing.T) {
	if err := testReshapeTransposePermute(); err != nil {
		t.Errorf("testReshapeTransposePermute failed: %v", err)
	}
}

// TestTestIndexLabels runs the test test index labels operation.
func TestTestIndexLabels(t *testing.T) {
	if err := testIndexLabels(); err != nil {
		t.Errorf("testIndexLabels failed: %v", err)
	}
}

// TestTestElementwiseBinary runs the test test elementwise binary operation.
func TestTestElementwiseBinary(t *testing.T) {
	if err := testElementwiseBinary(); err != nil {
		t.Errorf("testElementwiseBinary failed: %v", err)
	}
}

// TestTestBroadcasting runs the test test broadcasting operation.
func TestTestBroadcasting(t *testing.T) {
	if err := testBroadcasting(); err != nil {
		t.Errorf("testBroadcasting failed: %v", err)
	}
}

// TestTestElementwiseUnary runs the test test elementwise unary operation.
func TestTestElementwiseUnary(t *testing.T) {
	if err := testElementwiseUnary(); err != nil {
		t.Errorf("testElementwiseUnary failed: %v", err)
	}
}

// TestTestHyperbolicAndErf runs the test test hyperbolic and erf operation.
func TestTestHyperbolicAndErf(t *testing.T) {
	if err := testHyperbolicAndErf(); err != nil {
		t.Errorf("testHyperbolicAndErf failed: %v", err)
	}
}

// TestTestTensorReductions runs the test test tensor reductions operation.
func TestTestTensorReductions(t *testing.T) {
	if err := testTensorReductions(); err != nil {
		t.Errorf("testTensorReductions failed: %v", err)
	}
}

// TestTestMatMul runs the test test mat mul operation.
func TestTestMatMul(t *testing.T) {
	if err := testMatMul(); err != nil {
		t.Errorf("testMatMul failed: %v", err)
	}
}

// TestTestDotAndOuter runs the test test dot and outer operation.
func TestTestDotAndOuter(t *testing.T) {
	if err := testDotAndOuter(); err != nil {
		t.Errorf("testDotAndOuter failed: %v", err)
	}
}

// TestTestTensorContract runs the test test tensor contract operation.
func TestTestTensorContract(t *testing.T) {
	if err := testTensorContract(); err != nil {
		t.Errorf("testTensorContract failed: %v", err)
	}
}

// TestTestEinsumMatMul runs the test test einsum mat mul operation.
func TestTestEinsumMatMul(t *testing.T) {
	if err := testEinsumMatMul(); err != nil {
		t.Errorf("testEinsumMatMul failed: %v", err)
	}
}

// TestTestEinsumDot runs the test test einsum dot operation.
func TestTestEinsumDot(t *testing.T) {
	if err := testEinsumDot(); err != nil {
		t.Errorf("testEinsumDot failed: %v", err)
	}
}

// TestTestEinsumOuter runs the test test einsum outer operation.
func TestTestEinsumOuter(t *testing.T) {
	if err := testEinsumOuter(); err != nil {
		t.Errorf("testEinsumOuter failed: %v", err)
	}
}

// TestTestEinsumTranspose runs the test test einsum transpose operation.
func TestTestEinsumTranspose(t *testing.T) {
	if err := testEinsumTranspose(); err != nil {
		t.Errorf("testEinsumTranspose failed: %v", err)
	}
}

// TestTestEinsumDiagonal runs the test test einsum diagonal operation.
func TestTestEinsumDiagonal(t *testing.T) {
	if err := testEinsumDiagonal(); err != nil {
		t.Errorf("testEinsumDiagonal failed: %v", err)
	}
}

// TestTestEinsumTrace runs the test test einsum trace operation.
func TestTestEinsumTrace(t *testing.T) {
	if err := testEinsumTrace(); err != nil {
		t.Errorf("testEinsumTrace failed: %v", err)
	}
}

// TestTestEinsumMultiChain runs the test test einsum multi chain operation.
func TestTestEinsumMultiChain(t *testing.T) {
	if err := testEinsumMultiChain(); err != nil {
		t.Errorf("testEinsumMultiChain failed: %v", err)
	}
}

// TestTestEinsumOptimizePath runs the test test einsum optimize path operation.
func TestTestEinsumOptimizePath(t *testing.T) {
	if err := testEinsumOptimizePath(); err != nil {
		t.Errorf("testEinsumOptimizePath failed: %v", err)
	}
}

// TestTestDAGConstruction runs the test test dag construction operation.
func TestTestDAGConstruction(t *testing.T) {
	if err := testDAGConstruction(); err != nil {
		t.Errorf("testDAGConstruction failed: %v", err)
	}
}

// TestTestOptimizerCSE runs the test test optimizer cse operation.
func TestTestOptimizerCSE(t *testing.T) {
	if err := testOptimizerCSE(); err != nil {
		t.Errorf("testOptimizerCSE failed: %v", err)
	}
}

// TestTestConstantFolding runs the test test constant folding operation.
func TestTestConstantFolding(t *testing.T) {
	if err := testConstantFolding(); err != nil {
		t.Errorf("testConstantFolding failed: %v", err)
	}
}

// TestTestRewriteMulZero runs the test test rewrite mul zero operation.
func TestTestRewriteMulZero(t *testing.T) {
	if err := testRewriteMulZero(); err != nil {
		t.Errorf("testRewriteMulZero failed: %v", err)
	}
}

// TestTestRewriteAddZero runs the test test rewrite add zero operation.
func TestTestRewriteAddZero(t *testing.T) {
	if err := testRewriteAddZero(); err != nil {
		t.Errorf("testRewriteAddZero failed: %v", err)
	}
}

// TestTestRewriteDoubleNeg runs the test test rewrite double neg operation.
func TestTestRewriteDoubleNeg(t *testing.T) {
	if err := testRewriteDoubleNeg(); err != nil {
		t.Errorf("testRewriteDoubleNeg failed: %v", err)
	}
}

// TestTestRewriteDoubleTranspose runs the test test rewrite double transpose operation.
func TestTestRewriteDoubleTranspose(t *testing.T) {
	if err := testRewriteDoubleTranspose(); err != nil {
		t.Errorf("testRewriteDoubleTranspose failed: %v", err)
	}
}

// TestTestLeviCivita3D runs the test test levi civita3 d operation.
func TestTestLeviCivita3D(t *testing.T) {
	if err := testLeviCivita3D(); err != nil {
		t.Errorf("testLeviCivita3D failed: %v", err)
	}
}

// TestTestLeviCivita4D runs the test test levi civita4 d operation.
func TestTestLeviCivita4D(t *testing.T) {
	if err := testLeviCivita4D(); err != nil {
		t.Errorf("testLeviCivita4D failed: %v", err)
	}
}

// TestTestMetricInversion runs the test test metric inversion operation.
func TestTestMetricInversion(t *testing.T) {
	if err := testMetricInversion(); err != nil {
		t.Errorf("testMetricInversion failed: %v", err)
	}
}

// TestTestIndexRaisingLowering runs the test test index raising lowering operation.
func TestTestIndexRaisingLowering(t *testing.T) {
	if err := testIndexRaisingLowering(); err != nil {
		t.Errorf("testIndexRaisingLowering failed: %v", err)
	}
}

// TestTestRelativisticInvariant runs the test test relativistic invariant operation.
func TestTestRelativisticInvariant(t *testing.T) {
	if err := testRelativisticInvariant(); err != nil {
		t.Errorf("testRelativisticInvariant failed: %v", err)
	}
}

// TestTestChristoffelFlat runs the test test christoffel flat operation.
func TestTestChristoffelFlat(t *testing.T) {
	if err := testChristoffelFlat(); err != nil {
		t.Errorf("testChristoffelFlat failed: %v", err)
	}
}

// TestTestRiemannFlat runs the test test riemann flat operation.
func TestTestRiemannFlat(t *testing.T) {
	if err := testRiemannFlat(); err != nil {
		t.Errorf("testRiemannFlat failed: %v", err)
	}
}

// TestTestWedgeProduct runs the test test wedge product operation.
func TestTestWedgeProduct(t *testing.T) {
	if err := testWedgeProduct(); err != nil {
		t.Errorf("testWedgeProduct failed: %v", err)
	}
}

// TestTestMultiDtypeFloat64 runs the test test multi dtype float64 operation.
func TestTestMultiDtypeFloat64(t *testing.T) {
	if err := testMultiDtypeFloat64(); err != nil {
		t.Errorf("testMultiDtypeFloat64 failed: %v", err)
	}
}

// TestTestMultiDtypeComplex runs the test test multi dtype complex operation.
func TestTestMultiDtypeComplex(t *testing.T) {
	if err := testMultiDtypeComplex(); err != nil {
		t.Errorf("testMultiDtypeComplex failed: %v", err)
	}
}

// TestTestMultiDtypeInt runs the test test multi dtype int operation.
func TestTestMultiDtypeInt(t *testing.T) {
	if err := testMultiDtypeInt(); err != nil {
		t.Errorf("testMultiDtypeInt failed: %v", err)
	}
}

// TestTestGemmAVX2 runs the test test gemm avx2 operation.
func TestTestGemmAVX2(t *testing.T) {
	if err := testGemmAVX2(); err != nil {
		t.Errorf("testGemmAVX2 failed: %v", err)
	}
}

// TestTestMathDispatch runs the test test math dispatch operation.
func TestTestMathDispatch(t *testing.T) {
	if err := testMathDispatch(); err != nil {
		t.Errorf("testMathDispatch failed: %v", err)
	}
}
