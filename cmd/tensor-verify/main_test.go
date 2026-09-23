package main

import (
	"testing"

	"github.com/23skdu/longbow/internal/tensor"
)

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

func TestAtFloat64UnsupportedDtype(t *testing.T) {
	tensor.InitMathDispatch(true)
	tt := tensor.New(tensor.DtypeInt64, tensor.Shape{2})
	tt.Int64s()[0] = 42

	if got := atFloat64(tt, 0); got != 0 {
		t.Errorf("atFloat64 for unsupported dtype = %v, want 0", got)
	}
}

func TestTestCoreCreation(t *testing.T) {
	if err := testCoreCreation(); err != nil {
		t.Errorf("testCoreCreation failed: %v", err)
	}
}

func TestTestAllDtypes(t *testing.T) {
	if err := testAllDtypes(); err != nil {
		t.Errorf("testAllDtypes failed: %v", err)
	}
}

func TestTestMemoryLayoutAndSlicing(t *testing.T) {
	if err := testMemoryLayoutAndSlicing(); err != nil {
		t.Errorf("testMemoryLayoutAndSlicing failed: %v", err)
	}
}

func TestTestScalarAccessors(t *testing.T) {
	if err := testScalarAccessors(); err != nil {
		t.Errorf("testScalarAccessors failed: %v", err)
	}
}

func TestTestReshapeTransposePermute(t *testing.T) {
	if err := testReshapeTransposePermute(); err != nil {
		t.Errorf("testReshapeTransposePermute failed: %v", err)
	}
}

func TestTestIndexLabels(t *testing.T) {
	if err := testIndexLabels(); err != nil {
		t.Errorf("testIndexLabels failed: %v", err)
	}
}

func TestTestElementwiseBinary(t *testing.T) {
	if err := testElementwiseBinary(); err != nil {
		t.Errorf("testElementwiseBinary failed: %v", err)
	}
}

func TestTestBroadcasting(t *testing.T) {
	if err := testBroadcasting(); err != nil {
		t.Errorf("testBroadcasting failed: %v", err)
	}
}

func TestTestElementwiseUnary(t *testing.T) {
	if err := testElementwiseUnary(); err != nil {
		t.Errorf("testElementwiseUnary failed: %v", err)
	}
}

func TestTestHyperbolicAndErf(t *testing.T) {
	if err := testHyperbolicAndErf(); err != nil {
		t.Errorf("testHyperbolicAndErf failed: %v", err)
	}
}

func TestTestTensorReductions(t *testing.T) {
	if err := testTensorReductions(); err != nil {
		t.Errorf("testTensorReductions failed: %v", err)
	}
}

func TestTestMatMul(t *testing.T) {
	if err := testMatMul(); err != nil {
		t.Errorf("testMatMul failed: %v", err)
	}
}

func TestTestDotAndOuter(t *testing.T) {
	if err := testDotAndOuter(); err != nil {
		t.Errorf("testDotAndOuter failed: %v", err)
	}
}

func TestTestTensorContract(t *testing.T) {
	if err := testTensorContract(); err != nil {
		t.Errorf("testTensorContract failed: %v", err)
	}
}

func TestTestEinsumMatMul(t *testing.T) {
	if err := testEinsumMatMul(); err != nil {
		t.Errorf("testEinsumMatMul failed: %v", err)
	}
}

func TestTestEinsumDot(t *testing.T) {
	if err := testEinsumDot(); err != nil {
		t.Errorf("testEinsumDot failed: %v", err)
	}
}

func TestTestEinsumOuter(t *testing.T) {
	if err := testEinsumOuter(); err != nil {
		t.Errorf("testEinsumOuter failed: %v", err)
	}
}

func TestTestEinsumTranspose(t *testing.T) {
	if err := testEinsumTranspose(); err != nil {
		t.Errorf("testEinsumTranspose failed: %v", err)
	}
}

func TestTestEinsumDiagonal(t *testing.T) {
	if err := testEinsumDiagonal(); err != nil {
		t.Errorf("testEinsumDiagonal failed: %v", err)
	}
}

func TestTestEinsumTrace(t *testing.T) {
	if err := testEinsumTrace(); err != nil {
		t.Errorf("testEinsumTrace failed: %v", err)
	}
}

func TestTestEinsumMultiChain(t *testing.T) {
	if err := testEinsumMultiChain(); err != nil {
		t.Errorf("testEinsumMultiChain failed: %v", err)
	}
}

func TestTestEinsumOptimizePath(t *testing.T) {
	if err := testEinsumOptimizePath(); err != nil {
		t.Errorf("testEinsumOptimizePath failed: %v", err)
	}
}

func TestTestDAGConstruction(t *testing.T) {
	if err := testDAGConstruction(); err != nil {
		t.Errorf("testDAGConstruction failed: %v", err)
	}
}

func TestTestOptimizerCSE(t *testing.T) {
	if err := testOptimizerCSE(); err != nil {
		t.Errorf("testOptimizerCSE failed: %v", err)
	}
}

func TestTestConstantFolding(t *testing.T) {
	if err := testConstantFolding(); err != nil {
		t.Errorf("testConstantFolding failed: %v", err)
	}
}

func TestTestRewriteMulZero(t *testing.T) {
	if err := testRewriteMulZero(); err != nil {
		t.Errorf("testRewriteMulZero failed: %v", err)
	}
}

func TestTestRewriteAddZero(t *testing.T) {
	if err := testRewriteAddZero(); err != nil {
		t.Errorf("testRewriteAddZero failed: %v", err)
	}
}

func TestTestRewriteDoubleNeg(t *testing.T) {
	if err := testRewriteDoubleNeg(); err != nil {
		t.Errorf("testRewriteDoubleNeg failed: %v", err)
	}
}

func TestTestRewriteDoubleTranspose(t *testing.T) {
	if err := testRewriteDoubleTranspose(); err != nil {
		t.Errorf("testRewriteDoubleTranspose failed: %v", err)
	}
}

func TestTestLeviCivita3D(t *testing.T) {
	if err := testLeviCivita3D(); err != nil {
		t.Errorf("testLeviCivita3D failed: %v", err)
	}
}

func TestTestLeviCivita4D(t *testing.T) {
	if err := testLeviCivita4D(); err != nil {
		t.Errorf("testLeviCivita4D failed: %v", err)
	}
}

func TestTestMetricInversion(t *testing.T) {
	if err := testMetricInversion(); err != nil {
		t.Errorf("testMetricInversion failed: %v", err)
	}
}

func TestTestIndexRaisingLowering(t *testing.T) {
	if err := testIndexRaisingLowering(); err != nil {
		t.Errorf("testIndexRaisingLowering failed: %v", err)
	}
}

func TestTestRelativisticInvariant(t *testing.T) {
	if err := testRelativisticInvariant(); err != nil {
		t.Errorf("testRelativisticInvariant failed: %v", err)
	}
}

func TestTestChristoffelFlat(t *testing.T) {
	if err := testChristoffelFlat(); err != nil {
		t.Errorf("testChristoffelFlat failed: %v", err)
	}
}

func TestTestRiemannFlat(t *testing.T) {
	if err := testRiemannFlat(); err != nil {
		t.Errorf("testRiemannFlat failed: %v", err)
	}
}

func TestTestWedgeProduct(t *testing.T) {
	if err := testWedgeProduct(); err != nil {
		t.Errorf("testWedgeProduct failed: %v", err)
	}
}

func TestTestMultiDtypeFloat64(t *testing.T) {
	if err := testMultiDtypeFloat64(); err != nil {
		t.Errorf("testMultiDtypeFloat64 failed: %v", err)
	}
}

func TestTestMultiDtypeComplex(t *testing.T) {
	if err := testMultiDtypeComplex(); err != nil {
		t.Errorf("testMultiDtypeComplex failed: %v", err)
	}
}

func TestTestMultiDtypeInt(t *testing.T) {
	if err := testMultiDtypeInt(); err != nil {
		t.Errorf("testMultiDtypeInt failed: %v", err)
	}
}

func TestTestGemmAVX2(t *testing.T) {
	if err := testGemmAVX2(); err != nil {
		t.Errorf("testGemmAVX2 failed: %v", err)
	}
}

func TestTestMathDispatch(t *testing.T) {
	if err := testMathDispatch(); err != nil {
		t.Errorf("testMathDispatch failed: %v", err)
	}
}
