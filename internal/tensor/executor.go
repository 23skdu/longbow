package tensor

import "fmt"

// Executor executes a tensor IR graph against concrete input tensors.
type Executor struct {
	GPUThreshold int // minimum number of elements to consider GPU offload
	useGPU       bool
}

// NewExecutor creates an executor with default settings.
func NewExecutor() *Executor {
	return &Executor{
		GPUThreshold: 4096, // offload to GPU for tensors >= 4096 elements
	}
}

// SetGPU enables or disables GPU offload.
func (e *Executor) SetGPU(enabled bool) { e.useGPU = enabled }

// Run evaluates a tensor graph with the given input tensors.
// inputs[i] must correspond to IRNode with InputIdx == i.
func (e *Executor) Run(g *Graph, inputs []*Tensor) (*Tensor, error) {
	if g == nil {
		return nil, fmt.Errorf("tensor: executor: nil graph")
	}
	order := topologicalSort(g.Root)
	values := map[*IRNode]*Tensor{}
	for _, n := range order {
		v, err := e.eval(n, inputs, values)
		if err != nil {
			return nil, fmt.Errorf("tensor: executor: %w", err)
		}
		values[n] = v
	}
	return values[g.Root], nil
}

func (e *Executor) eval(n *IRNode, inputs []*Tensor, values map[*IRNode]*Tensor) (*Tensor, error) {
	switch n.Kind {
	case OpInput:
		if n.InputIdx < 0 || n.InputIdx >= len(inputs) {
			return nil, fmt.Errorf("input index %d out of range (have %d inputs)", n.InputIdx, len(inputs))
		}
		return inputs[n.InputIdx], nil

	case OpConstant:
		return n.ConstVal, nil

	case OpTranspose:
		a, err := e.resolve(n.Children[0], inputs, values)
		if err != nil {
			return nil, err
		}
		return Transpose(a, n.Perm)

	case OpReshape:
		a, err := e.resolve(n.Children[0], inputs, values)
		if err != nil {
			return nil, err
		}
		return a.Reshape(n.Shape), nil

	case OpElementwise:
		args := make([]*Tensor, len(n.Children))
		for i, c := range n.Children {
			v, err := e.resolve(c, inputs, values)
			if err != nil {
				return nil, err
			}
			args[i] = v
		}
		return e.execElementwise(n.ElemOp, args)

	case OpContract:
		a, err := e.resolve(n.Children[0], inputs, values)
		if err != nil {
			return nil, err
		}
		b, err := e.resolve(n.Children[1], inputs, values)
		if err != nil {
			return nil, err
		}
		return TensorContract(a, b, n.SumLabels, n.OutLabels)

	case OpReduce:
		a, err := e.resolve(n.Children[0], inputs, values)
		if err != nil {
			return nil, err
		}
		return ReduceSum(a, n.ReduceAxis)

	default:
		return nil, fmt.Errorf("unknown op kind %v", n.Kind)
	}
}

func (e *Executor) resolve(n *IRNode, inputs []*Tensor, values map[*IRNode]*Tensor) (*Tensor, error) {
	if v, ok := values[n]; ok {
		return v, nil
	}
	return e.eval(n, inputs, values)
}

func (e *Executor) execElementwise(op string, args []*Tensor) (*Tensor, error) {
	switch op {
	case "add":
		return Add(args[0], args[1])
	case "sub":
		return Sub(args[0], args[1])
	case "mul":
		return Mul(args[0], args[1])
	case "div":
		return Div(args[0], args[1])
	case "neg":
		return Neg(args[0])
	case "sin":
		return Sin(args[0])
	case "cos":
		return Cos(args[0])
	case "tan":
		return Tan(args[0])
	case "exp":
		return Exp(args[0])
	case "log":
		return Log(args[0])
	case "sqrt":
		return Sqrt(args[0])
	case "pow":
		return Pow(args[0], args[1])
	case "asin":
		return Asin(args[0])
	case "acos":
		return Acos(args[0])
	case "atan":
		return Atan(args[0])
	case "sinh":
		return Sinh(args[0])
	case "cosh":
		return Cosh(args[0])
	case "tanh":
		return Tanh(args[0])
	default:
		return nil, fmt.Errorf("unknown elementwise op %q", op)
	}
}

// topologicalSort returns nodes in execution order (parents before children).
func topologicalSort(root *IRNode) []*IRNode {
	var order []*IRNode
	seen := map[*IRNode]bool{}
	var dfs func(n *IRNode)
	dfs = func(n *IRNode) {
		if n == nil || seen[n] {
			return
		}
		seen[n] = true
		for _, c := range n.Children {
			dfs(c)
		}
		order = append(order, n)
	}
	dfs(root)
	return order
}

// Cost estimates the computational cost of a node in FLOPs.
// Used by the auto-scheduler to decide CPU vs GPU placement.
func Cost(n *IRNode) int {
	switch n.Kind {
	case OpContract:
		aSize := n.Children[0].NumElements()
		bSize := n.Children[1].NumElements()
		outSize := n.NumElements()
		contractVol := 1
		for _, lbl := range n.SumLabels {
			for _, c := range n.Children {
				for i, l := range c.Labels() {
					if l == lbl {
						contractVol *= c.Shape[i]
					}
				}
			}
		}
		return aSize + bSize + outSize*contractVol
	case OpElementwise:
		return n.NumElements() * 2
	case OpReduce:
		return n.NumElements() * 2
	case OpTranspose:
		return n.NumElements()
	case OpReshape:
		return 0
	default:
		return n.NumElements()
	}
}
