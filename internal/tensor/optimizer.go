package tensor

import "fmt"

// RewriteRule identifies a transformation on the IR.
type RewriteRule int

const (
	RuleNone              RewriteRule = iota
	RuleMulByZero                    // A * 0 -> 0
	RuleAddZero                     // A + 0 -> A
	RuleDoubleNeg                   // -(-A) -> A
	RuleTransposeOfTranspose        // T(T(A)) -> A
	RuleReshapeOfReshape            // R(R(A)) -> A (if shapes allow)
	RuleExpOfLog                    // exp(log(A)) -> A (domain permitting)
	RuleLogOfExp                    // log(exp(A)) -> A
	RuleConstantFolding             // Op(Const, Const) -> Const
	RuleCSE                         // Common Subexpression Elimination
)

// Optimizer rewrites a tensor IR graph for efficiency.
type Optimizer struct {
	Rules []RewriteRule
}

func NewOptimizer() *Optimizer {
	return &Optimizer{
		Rules: []RewriteRule{
			RuleMulByZero,
			RuleAddZero,
			RuleDoubleNeg,
			RuleTransposeOfTranspose,
			RuleConstantFolding,
		},
	}
}

// Optimize applies algebraic rewrite rules and CSE to simplify a graph.
func (opt *Optimizer) Optimize(g *Graph) *Graph {
	if g == nil {
		return nil
	}
	root := opt.rewrite(g.Root)

	// Apply CSE deduplication pass
	canonMap := map[string]*IRNode{}
	root = opt.applyCSE(root, canonMap)

	return NewGraph(root)
}

func (opt *Optimizer) applyCSE(n *IRNode, canonMap map[string]*IRNode) *IRNode {
	if n == nil {
		return nil
	}
	for i, c := range n.Children {
		n.Children[i] = opt.applyCSE(c, canonMap)
	}
	key := deepNodeKey(n)
	if existing, ok := canonMap[key]; ok {
		TensorOptimizerPassesTotal.WithLabelValues("cse").Inc()
		return existing
	}
	canonMap[key] = n
	return n
}

func deepNodeKey(n *IRNode) string {
	if n == nil {
		return ""
	}
	childKeys := ""
	for _, c := range n.Children {
		childKeys += fmt.Sprintf("[%p]", c)
	}
	return nodeKey(n) + ":" + childKeys
}

func (opt *Optimizer) rewrite(n *IRNode) *IRNode {
	if n == nil {
		return nil
	}
	children := make([]*IRNode, len(n.Children))
	for i, c := range n.Children {
		children[i] = opt.rewrite(c)
	}
	newNode := copyNode(n)
	newNode.Children = children

	for _, rule := range opt.Rules {
		if result := opt.apply(rule, newNode); result != nil {
			return result
		}
	}
	return newNode
}

func (opt *Optimizer) apply(rule RewriteRule, n *IRNode) *IRNode {
	switch rule {
	case RuleMulByZero:
		res := rewriteMulByZero(n)
		if res != nil {
			TensorOptimizerPassesTotal.WithLabelValues("mul_by_zero").Inc()
		}
		return res
	case RuleAddZero:
		res := rewriteAddZero(n)
		if res != nil {
			TensorOptimizerPassesTotal.WithLabelValues("add_zero").Inc()
		}
		return res
	case RuleDoubleNeg:
		res := rewriteDoubleNeg(n)
		if res != nil {
			TensorOptimizerPassesTotal.WithLabelValues("double_neg").Inc()
		}
		return res
	case RuleTransposeOfTranspose:
		res := rewriteTransposeOfTranspose(n)
		if res != nil {
			TensorOptimizerPassesTotal.WithLabelValues("transpose_of_transpose").Inc()
		}
		return res
	case RuleConstantFolding:
		res := rewriteConstantFolding(n)
		if res != nil {
			TensorOptimizerPassesTotal.WithLabelValues("constant_folding").Inc()
			TensorOptimizerFlopsSavedTotal.Add(float64(numElements(n.Shape)))
		}
		return res
	default:
		return nil
	}
}

func rewriteConstantFolding(n *IRNode) *IRNode {
	switch n.Kind {
	case OpTranspose:
		if len(n.Children) == 1 && n.Children[0].Kind == OpConstant && n.Children[0].ConstVal != nil {
			if res, err := Transpose(n.Children[0].ConstVal, n.Perm); err == nil {
				return NewConstant(res)
			}
		}
	case OpReshape:
		if len(n.Children) == 1 && n.Children[0].Kind == OpConstant && n.Children[0].ConstVal != nil {
			return NewConstant(n.Children[0].ConstVal.Reshape(n.Shape))
		}
	case OpElementwise:
		allConst := true
		for _, c := range n.Children {
			if c.Kind != OpConstant || c.ConstVal == nil {
				allConst = false
				break
			}
		}
		if allConst && len(n.Children) > 0 {
			args := make([]*Tensor, len(n.Children))
			for i, c := range n.Children {
				args[i] = c.ConstVal
			}
			exec := NewExecutor()
			if res, err := exec.execElementwise(n.ElemOp, args); err == nil {
				return NewConstant(res)
			}
		}
	}
	return nil
}

func rewriteMulByZero(n *IRNode) *IRNode {
	if n.Kind != OpElementwise || n.ElemOp != "mul" || len(n.Children) != 2 {
		return nil
	}
	for _, c := range n.Children {
		if isZeroTensor(c) {
			return NewConstant(New(n.Dtype, cloneShape(n.Shape)))
		}
	}
	return nil
}

func rewriteAddZero(n *IRNode) *IRNode {
	if n.Kind != OpElementwise || n.ElemOp != "add" || len(n.Children) != 2 {
		return nil
	}
	if isZeroTensor(n.Children[0]) {
		return n.Children[1]
	}
	if isZeroTensor(n.Children[1]) {
		return n.Children[0]
	}
	return nil
}

func rewriteDoubleNeg(n *IRNode) *IRNode {
	if n.Kind != OpElementwise || n.ElemOp != "neg" || len(n.Children) != 1 {
		return nil
	}
	child := n.Children[0]
	if child.Kind == OpElementwise && child.ElemOp == "neg" && len(child.Children) == 1 {
		return child.Children[0]
	}
	return nil
}

func rewriteTransposeOfTranspose(n *IRNode) *IRNode {
	if n.Kind != OpTranspose || len(n.Children) != 1 {
		return nil
	}
	child := n.Children[0]
	if child.Kind != OpTranspose || len(child.Children) != 1 {
		return nil
	}
	grandchild := child.Children[0]
	// Compose permutations: perm(child) then perm(n)
	composed := composePerm(n.Perm, child.Perm)
	if isIdentityPerm(composed) {
		return grandchild
	}
	return NewTranspose(grandchild, composed)
}

func composePerm(outer, inner []int) []int {
	if len(outer) != len(inner) {
		return outer
	}
	out := make([]int, len(outer))
	for i, p := range outer {
		out[i] = inner[p]
	}
	return out
}

func isIdentityPerm(p []int) bool {
	for i, v := range p {
		if i != v {
			return false
		}
	}
	return true
}

func isZeroTensor(n *IRNode) bool {
	if n.Kind != OpConstant {
		return false
	}
	if n.ConstVal == nil {
		return false
	}
	switch n.Dtype {
	case DtypeFloat32:
		for _, v := range n.ConstVal.Float32s() {
			if v != 0 {
				return false
			}
		}
	default:
		return false
	}
	return true
}

func copyNode(n *IRNode) *IRNode {
	if n == nil {
		return nil
	}
	out := &IRNode{
		Kind:       n.Kind,
		Dtype:      n.Dtype,
		Shape:      cloneShape(n.Shape),
		SumLabels:  append([]string{}, n.SumLabels...),
		OutLabels:  append([]string{}, n.OutLabels...),
		Perm:       append([]int{}, n.Perm...),
		ReduceAxis: n.ReduceAxis,
		ReduceOp:   n.ReduceOp,
		ElemOp:     n.ElemOp,
		InputIdx:   n.InputIdx,
		ConstVal:   n.ConstVal,
	}
	return out
}

// FindCommonSubexpressions detects identical subgraphs and returns a mapping
// from canonical node to list of equivalent nodes (CSE).
func FindCommonSubexpressions(root *IRNode) map[*IRNode][]*IRNode {
	table := map[string][]*IRNode{}
	collectNodes(root, table)
	result := map[*IRNode][]*IRNode{}
	for _, group := range table {
		if len(group) > 1 {
			result[group[0]] = group
		}
	}
	return result
}

func collectNodes(n *IRNode, table map[string][]*IRNode) {
	if n == nil {
		return
	}
	key := nodeKey(n)
	table[key] = append(table[key], n)
	for _, c := range n.Children {
		collectNodes(c, table)
	}
}

func nodeKey(n *IRNode) string {
	switch n.Kind {
	case OpInput:
		return fmt.Sprintf("input[%d]", n.InputIdx)
	case OpConstant:
		return fmt.Sprintf("const[%s%v]", n.Dtype, n.Shape)
	case OpContract:
		return fmt.Sprintf("contract[%v%v]", n.SumLabels, n.OutLabels)
	case OpTranspose:
		return fmt.Sprintf("transpose%v", n.Perm)
	case OpReshape:
		return fmt.Sprintf("reshape%v", n.Shape)
	case OpElementwise:
		return fmt.Sprintf("elem[%s]", n.ElemOp)
	case OpReduce:
		return fmt.Sprintf("reduce[%s%d]", n.ReduceOp, n.ReduceAxis)
	default:
		return "unknown"
	}
}
