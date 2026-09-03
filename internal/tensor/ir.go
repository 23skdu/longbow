package tensor

import (
	"fmt"
)

// OpKind identifies the kind of tensor operation.
type OpKind int

const (
	OpContract   OpKind = iota // tensor contraction (sum over shared indices)
	OpTranspose                // axis permutation
	OpReshape                  // shape change (no data movement)
	OpElementwise              // element-wise unary/binary operation
	OpReduce                   // reduction along axis (sum, max, min)
	OpConstant                 // constant tensor (leaf)
	OpInput                    // input tensor (leaf)
)

// IRNode is a node in the tensor expression DAG.
type IRNode struct {
	Kind     OpKind
	Dtype    Dtype
	Shape    Shape
	Children []*IRNode

	// OpContract
	SumLabels []string // indices being contracted
	OutLabels []string // output index order

	// OpTranspose
	Perm []int // new axis order

	// OpReduce
	ReduceAxis int
	ReduceOp   string // "sum", "max", "min"

	// OpElementwise
	ElemOp string // "add", "mul", "sub", "div", "sin", "cos", "tan", "exp", "log", "neg", "pow"

	// OpInput / OpConstant
	InputIdx int       // index into input tensor list
	ConstVal *Tensor   // constant tensor value
}

// NewInput creates an IR leaf node for an input tensor.
func NewInput(idx int, dtype Dtype, shape Shape) *IRNode {
	return &IRNode{
		Kind:     OpInput,
		Dtype:    dtype,
		Shape:    cloneShape(shape),
		InputIdx: idx,
	}
}

// NewConstant creates an IR leaf node for a constant tensor.
func NewConstant(t *Tensor) *IRNode {
	return &IRNode{
		Kind:     OpConstant,
		Dtype:    t.Dtype(),
		Shape:    cloneShape(t.Shape()),
		ConstVal: t,
	}
}

// NewContract creates a contraction node (einsum-style).
func NewContract(a, b *IRNode, sumLabels, outLabels []string) *IRNode {
	// Infer output shape: contains non-summed axes from both inputs
	outShape := inferContractShape(a.Shape, b.Shape, a.Labels(), b.Labels(), sumLabels, outLabels)
	dtype := Promote(a.Dtype, b.Dtype)
	return &IRNode{
		Kind:      OpContract,
		Dtype:     dtype,
		Shape:     outShape,
		Children:  []*IRNode{a, b},
		SumLabels: sumLabels,
		OutLabels: outLabels,
	}
}

// NewTranspose creates a transpose node.
func NewTranspose(a *IRNode, perm []int) *IRNode {
	newShape := make(Shape, len(perm))
	for i, p := range perm {
		newShape[i] = a.Shape[p]
	}
	return &IRNode{
		Kind:     OpTranspose,
		Dtype:    a.Dtype,
		Shape:    newShape,
		Children: []*IRNode{a},
		Perm:     perm,
	}
}

// NewReshape creates a reshape node.
func NewReshape(a *IRNode, shape Shape) *IRNode {
	return &IRNode{
		Kind:     OpReshape,
		Dtype:    a.Dtype,
		Shape:    cloneShape(shape),
		Children: []*IRNode{a},
	}
}

// NewElementwise creates an element-wise operation node.
func NewElementwise(op string, args ...*IRNode) *IRNode {
	dtype := args[0].Dtype
	for i := 1; i < len(args); i++ {
		dtype = Promote(dtype, args[i].Dtype)
	}
	shape := args[0].Shape
	return &IRNode{
		Kind:     OpElementwise,
		Dtype:    dtype,
		Shape:    cloneShape(shape),
		Children: args,
		ElemOp:   op,
	}
}

// NewReduce creates a reduction node.
func NewReduce(a *IRNode, axis int, op string) *IRNode {
	outShape := make(Shape, 0, len(a.Shape)-1)
	for i, d := range a.Shape {
		if i != axis {
			outShape = append(outShape, d)
		}
	}
	if len(outShape) == 0 {
		outShape = Shape{1}
	}
	return &IRNode{
		Kind:       OpReduce,
		Dtype:      a.Dtype,
		Shape:      outShape,
		Children:   []*IRNode{a},
		ReduceAxis: axis,
		ReduceOp:   op,
	}
}

// Labels returns the index labels for this node (used for contraction matching).
func (n *IRNode) Labels() []string {
	// Default labels: a, b, c, ...
	if len(n.OutLabels) > 0 {
		return n.OutLabels
	}
	lbls := make([]string, n.Rank())
	for i := range lbls {
		lbls[i] = string(rune('a' + i))
	}
	return lbls
}

// Rank returns the number of axes.
func (n *IRNode) Rank() int { return len(n.Shape) }

// NumElements returns the total number of elements.
func (n *IRNode) NumElements() int { return numElements(n.Shape) }

// String returns a human-readable representation.
func (n *IRNode) String() string {
	switch n.Kind {
	case OpInput:
		return fmt.Sprintf("input[%d]%v", n.InputIdx, n.Shape)
	case OpConstant:
		return fmt.Sprintf("const%v", n.Shape)
	case OpContract:
		return fmt.Sprintf("contract%v", n.Shape)
	case OpTranspose:
		return fmt.Sprintf("transpose%v%v", n.Perm, n.Shape)
	case OpReshape:
		return fmt.Sprintf("reshape%v", n.Shape)
	case OpElementwise:
		return fmt.Sprintf("%s%v", n.ElemOp, n.Shape)
	case OpReduce:
		return fmt.Sprintf("reduce(%s,axis=%d)%v", n.ReduceOp, n.ReduceAxis, n.Shape)
	default:
		return "unknown"
	}
}

// Graph holds a tensor expression DAG.
type Graph struct {
	Root  *IRNode
	Input []*IRNode // leaf input nodes in order
}

// NewGraph creates a graph from a root node.
func NewGraph(root *IRNode) *Graph {
	g := &Graph{Root: root}
	collectInputs(root, &g.Input)
	return g
}

func collectInputs(n *IRNode, inputs *[]*IRNode) {
	if n.Kind == OpInput {
		// deduplicate by pointer
		for _, in := range *inputs {
			if in == n {
				return
			}
		}
		*inputs = append(*inputs, n)
		return
	}
	for _, c := range n.Children {
		collectInputs(c, inputs)
	}
}

func inferContractShape(aShape, bShape Shape, aLabels, bLabels, _ []string, outLabels []string) Shape {
	dimMap := map[string]int{}
	for i, lbl := range aLabels {
		dimMap[lbl] = aShape[i]
	}
	for i, lbl := range bLabels {
		dimMap[lbl] = bShape[i]
	}
	out := make(Shape, len(outLabels))
	for i, lbl := range outLabels {
		out[i] = dimMap[lbl]
	}
	return out
}
