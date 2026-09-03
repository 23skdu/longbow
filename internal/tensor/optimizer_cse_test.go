package tensor

import (
	"testing"
)

func TestOptimizerCSE(t *testing.T) {
	// Build graph: (A + B) * (A + B)
	inA := NewInput(0, DtypeFloat32, Shape{4, 4})
	inB := NewInput(1, DtypeFloat32, Shape{4, 4})

	add1 := NewElementwise("add", inA, inB)
	add2 := NewElementwise("add", inA, inB)
	mul := NewElementwise("mul", add1, add2)

	g := NewGraph(mul)
	opt := NewOptimizer()
	optG := opt.Optimize(g)

	// After optimization with CSE, both children of mul should point to the exact same node!
	if optG.Root.Kind != OpElementwise || optG.Root.ElemOp != "mul" {
		t.Fatalf("expected mul root, got %v", optG.Root.Kind)
	}
	if optG.Root.Children[0] != optG.Root.Children[1] {
		t.Errorf("CSE failed: expected children of mul to be identical pointer, got %p and %p",
			optG.Root.Children[0], optG.Root.Children[1])
	}
}

func TestOptimizerConstantFolding(t *testing.T) {
	// Build graph with constant tensors: Const(3) + Const(4) -> folded to Const(7)
	t1 := New(DtypeFloat32, Shape{2})
	t1.Float32s()[0] = 3.0
	t1.Float32s()[1] = 5.0

	t2 := New(DtypeFloat32, Shape{2})
	t2.Float32s()[0] = 4.0
	t2.Float32s()[1] = 2.0

	c1 := NewConstant(t1)
	c2 := NewConstant(t2)
	add := NewElementwise("add", c1, c2)

	g := NewGraph(add)
	opt := NewOptimizer()
	optG := opt.Optimize(g)

	if optG.Root.Kind != OpConstant {
		t.Fatalf("expected root to be folded into OpConstant, got %v", optG.Root.Kind)
	}
	if optG.Root.ConstVal == nil {
		t.Fatalf("expected non-nil ConstVal in folded node")
	}

	res := optG.Root.ConstVal.Float32s()
	if res[0] != 7.0 || res[1] != 7.0 {
		t.Errorf("expected [7.0, 7.0], got [%f, %f]", res[0], res[1])
	}
}
