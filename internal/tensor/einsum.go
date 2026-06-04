package tensor

import (
	"fmt"
	"strings"
)

// EinsumOp describes a parsed Einstein summation operation.
type EinsumOp struct {
	Inputs  [][]string // index labels per input tensor
	Output  []string   // output index labels
	NumIndices int
}

// ParseEinsum parses an Einstein summation string like "ij,jk->ik".
func ParseEinsum(expr string) (*EinsumOp, error) {
	arrow := strings.Index(expr, "->")
	var left, right string
	if arrow >= 0 {
		left = strings.TrimSpace(expr[:arrow])
		right = strings.TrimSpace(expr[arrow+2:])
	} else {
		left = strings.TrimSpace(expr)
		right = ""
	}

	parts := strings.Split(left, ",")
	if len(parts) == 0 {
		return nil, fmt.Errorf("einsum: empty expression")
	}

	inputs := make([][]string, len(parts))
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			return nil, fmt.Errorf("einsum: empty input %d", i)
		}
		inputs[i] = labelsFromString(p)
	}

	seen := map[string]int{}
	for _, labels := range inputs {
		for _, lbl := range labels {
			seen[lbl]++
		}
	}

	var output []string
	if right != "" {
		output = labelsFromString(right)
	} else {
		// Implicit: sum over labels appearing exactly once
		for lbl := range seen {
			if seen[lbl] == 1 {
				output = append(output, lbl)
			}
		}
	}

	// Count unique indices
	idxSet := map[string]int{}
	for _, lbl := range output {
		idxSet[lbl] = 1
	}
	for _, labels := range inputs {
		for _, lbl := range labels {
			idxSet[lbl] = 1
		}
	}
	numIndices := len(idxSet)

	op := &EinsumOp{
		Inputs:  inputs,
		Output:  output,
		NumIndices: numIndices,
	}

	return op, nil
}

func labelsFromString(s string) []string {
	out := make([]string, 0, len(s))
	for _, r := range s {
		out = append(out, string(r))
	}
	return out
}

// Validate checks that an EinsumOp is valid for the given tensor shapes.
func (op *EinsumOp) Validate(shapes []Shape) error {
	if len(shapes) != len(op.Inputs) {
		return fmt.Errorf("einsum: got %d tensors but expression has %d inputs", len(shapes), len(op.Inputs))
	}
	for i, labels := range op.Inputs {
		if len(labels) != len(shapes[i]) {
			return fmt.Errorf("einsum: input %d has %d labels but shape has %d axes", i, len(labels), len(shapes[i]))
		}
	}
	// Check that contracted dimensions match
	dimMap := map[string]int{}
	for i, labels := range op.Inputs {
		for j, lbl := range labels {
			if prev, ok := dimMap[lbl]; ok {
				if prev != shapes[i][j] {
					return fmt.Errorf("einsum: dimension mismatch for label %q: %d vs %d", lbl, prev, shapes[i][j])
				}
			}
			dimMap[lbl] = shapes[i][j]
		}
	}
	return nil
}

// InferOutputShape computes the output shape from an EinsumOp and input shapes.
func (op *EinsumOp) InferOutputShape(shapes []Shape) Shape {
	out := make(Shape, len(op.Output))
	dimMap := map[string]int{}
	for i, labels := range op.Inputs {
		for j, lbl := range labels {
			dimMap[lbl] = shapes[i][j]
		}
	}
	for i, lbl := range op.Output {
		out[i] = dimMap[lbl]
	}
	return out
}

// Contract describes a pairwise tensor contraction.
type Contract struct {
	LHS       int   // index into operand list
	RHS       int   // index into operand list
	SumLabels []string // labels being summed (contracted)
	OutLabels []string // labels in the output
}

// Path represents a sequence of pairwise contractions (an einsum path).
type Path struct {
	Contracts []Contract
	Final     []string // output label order
}

type operand struct {
	idx    int
	labels []string
	shape  Shape
}

// OptimizePath uses greedy search to find a contraction order minimizing FLOPs.
func (op *EinsumOp) OptimizePath(shapes []Shape) *Path {
	n := len(op.Inputs)
	ops := make([]operand, n)
	for i := 0; i < n; i++ {
		ops[i] = operand{
			idx:    i,
			labels: op.Inputs[i],
			shape:  shapes[i],
		}
	}

	dimMap := map[string]int{}
	for i, labels := range op.Inputs {
		for j, lbl := range labels {
			dimMap[lbl] = shapes[i][j]
		}
	}

	var path Path
	for len(ops) > 1 {
		bestI, bestJ := 0, 1
		bestCost := int(^uint(0) >> 1)

		for i := 0; i < len(ops); i++ {
			for j := i + 1; j < len(ops); j++ {
				cost, _ := contractCost(ops[i], ops[j], dimMap)
				if cost < bestCost {
					bestCost = cost
					bestI, bestJ = i, j
				}
			}
		}

		// Contract ops[bestI] and ops[bestJ]
		_, outLabels := contractCost(ops[bestI], ops[bestJ], dimMap)
		sumLabels := commonLabels(ops[bestI].labels, ops[bestJ].labels)

		// Determine output shape
		outShape := make(Shape, len(outLabels))
		for k, lbl := range outLabels {
			outShape[k] = dimMap[lbl]
		}

		path.Contracts = append(path.Contracts, Contract{
			LHS:       ops[bestI].idx,
			RHS:       ops[bestJ].idx,
			SumLabels: sumLabels,
			OutLabels: outLabels,
		})

		// Replace with intermediate
		newOp := operand{
			idx:    -(len(path.Contracts)),
			labels: outLabels,
			shape:  outShape,
		}
		ops = append(ops[:bestJ], ops[bestJ+1:]...)
		ops = append(ops[:bestI], ops[bestI+1:]...)
		ops = append(ops, newOp)
	}

	path.Final = op.Output
	return &path
}

func contractCost(a, b operand, dimMap map[string]int) (int, []string) {
	common := commonLabels(a.labels, b.labels)
	outLabels := unionLabels(excludeLabels(a.labels, common), excludeLabels(b.labels, common))

	// Cost = product of all dimensions involved
	cost := 1
	for _, lbl := range a.labels {
		cost *= dimMap[lbl]
	}
	for _, lbl := range b.labels {
		cost *= dimMap[lbl]
	}
	return cost, outLabels
}

func commonLabels(a, b []string) []string {
	set := map[string]bool{}
	for _, s := range a {
		set[s] = true
	}
	var out []string
	for _, s := range b {
		if set[s] {
			out = append(out, s)
		}
	}
	return out
}

func excludeLabels(labels, exclude []string) []string {
	set := map[string]bool{}
	for _, s := range exclude {
		set[s] = true
	}
	var out []string
	for _, s := range labels {
		if !set[s] {
			out = append(out, s)
		}
	}
	return out
}

func unionLabels(a, b []string) []string {
	set := map[string]bool{}
	for _, s := range a {
		set[s] = true
	}
	for _, s := range b {
		set[s] = true
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	return out
}
