package tensor

import (
	"testing"
)

func TestOptimizeMulByZero(t *testing.T) {
	opt := NewOptimizer()
	zero := NewConstant(New(DtypeFloat32, Shape{2}))
	in := NewInput(0, DtypeFloat32, Shape{2})
	mul := NewElementwise("mul", in, zero)
	g := opt.Optimize(NewGraph(mul))
	if g.Root.Kind != OpConstant {
		t.Errorf("expected OpConstant after mul by zero, got %v", g.Root.Kind)
	}
}

func TestOptimizeAddZero(t *testing.T) {
	opt := NewOptimizer()
	zero := NewConstant(New(DtypeFloat32, Shape{2}))
	in := NewInput(0, DtypeFloat32, Shape{2})
	add := NewElementwise("add", in, zero)
	g := opt.Optimize(NewGraph(add))
	if g.Root.Kind != OpInput {
		t.Errorf("expected OpInput after add zero, got %v", g.Root.Kind)
	}
}

func TestOptimizeDoubleNeg(t *testing.T) {
	opt := NewOptimizer()
	in := NewInput(0, DtypeFloat32, Shape{2})
	neg1 := NewElementwise("neg", in)
	neg2 := NewElementwise("neg", neg1)
	g := opt.Optimize(NewGraph(neg2))
	if g.Root.Kind != OpInput {
		t.Errorf("expected OpInput after double neg, got %v", g.Root.Kind)
	}
}

func TestOptimizeTransposeIdentity(t *testing.T) {
	opt := NewOptimizer()
	in := NewInput(0, DtypeFloat32, Shape{2, 3})
	t1 := NewTranspose(in, []int{1, 0})
	t2 := NewTranspose(t1, []int{1, 0})
	g := opt.Optimize(NewGraph(t2))
	if g.Root.Kind != OpInput {
		t.Errorf("expected OpInput after T(T(A)), got %v", g.Root.Kind)
	}
}

func TestFindCommonSubexpressions(t *testing.T) {
	in := NewInput(0, DtypeFloat32, Shape{2})
	a := NewElementwise("sin", in)
	b := NewElementwise("sin", in)
	root := NewElementwise("add", a, b)
	cse := FindCommonSubexpressions(root)
	if len(cse) == 0 {
		t.Error("expected at least one CSE group")
	}
}

func TestNoCSE(t *testing.T) {
	in0 := NewInput(0, DtypeFloat32, Shape{2})
	in1 := NewInput(1, DtypeFloat32, Shape{2})
	sin := NewElementwise("sin", in0)
	cos := NewElementwise("cos", in1)
	root := NewElementwise("add", sin, cos)
	cse := FindCommonSubexpressions(root)
	for _, group := range cse {
		if len(group) > 1 {
			// Only the shared "in0" as child of both sin and cos can cause false CSE;
			// different nodes should not be grouped.
			if group[0].Kind == OpInput {
				continue
			}
			t.Errorf("unexpected CSE of kind %v: size %d", group[0].Kind, len(group))
		}
	}
}

func TestIsIdentityPerm(t *testing.T) {
	if !isIdentityPerm([]int{0, 1, 2}) {
		t.Error("0,1,2 should be identity")
	}
	if isIdentityPerm([]int{1, 0}) {
		t.Error("1,0 should not be identity")
	}
}

func TestComposePerm(t *testing.T) {
	p := composePerm([]int{0, 1}, []int{1, 0})
	if p[0] != 1 || p[1] != 0 {
		t.Errorf("compose([0,1],[1,0]) = %v, want [1,0]", p)
	}
}
