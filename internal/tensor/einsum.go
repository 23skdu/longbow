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

// Einsum evaluates an Einstein summation expression on the given tensors.
// Supports multi-tensor contractions, permutations, reductions, diagonals ("ii->i"), and trace ("ii->").
func Einsum(expr string, tensors ...*Tensor) (*Tensor, error) {
	if len(tensors) == 0 {
		return nil, fmt.Errorf("einsum: no input tensors provided")
	}
	op, err := ParseEinsum(expr)
	if err != nil {
		return nil, err
	}
	shapes := make([]Shape, len(tensors))
	for i, t := range tensors {
		if t == nil {
			return nil, fmt.Errorf("einsum: tensor %d is nil", i)
		}
		shapes[i] = t.Shape()
	}
	if err := op.Validate(shapes); err != nil {
		return nil, err
	}

	// 1. Process self-reductions/diagonals on each tensor if duplicate labels exist
	currentTensors := make([]*Tensor, len(tensors))
	copy(currentTensors, tensors)
	currentLabels := make([][]string, len(op.Inputs))
	for i := range op.Inputs {
		currentLabels[i] = make([]string, len(op.Inputs[i]))
		copy(currentLabels[i], op.Inputs[i])
	}

	for i := 0; i < len(currentTensors); i++ {
		t := currentTensors[i]
		labels := currentLabels[i]

		hasDup := true
		for hasDup {
			hasDup = false
			labelCounts := map[string][]int{}
			for ax, lbl := range labels {
				labelCounts[lbl] = append(labelCounts[lbl], ax)
			}
			for lbl, axes := range labelCounts {
				if len(axes) >= 2 {
					ax1, ax2 := axes[0], axes[1]
					inOutput := false
					for _, outLbl := range op.Output {
						if outLbl == lbl {
							inOutput = true
							break
						}
					}
					if inOutput {
						diag, err := t.Diagonal(ax1, ax2)
						if err != nil {
							return nil, err
						}
						t = diag
						var newLabels []string
						for ax, l := range labels {
							if ax != ax1 && ax != ax2 {
								newLabels = append(newLabels, l)
							}
						}
						newLabels = append(newLabels, lbl)
						labels = newLabels
					} else {
						tr, err := t.Trace(ax1, ax2)
						if err != nil {
							return nil, err
						}
						t = tr
						var newLabels []string
						for ax, l := range labels {
							if ax != ax1 && ax != ax2 {
								newLabels = append(newLabels, l)
							}
						}
						labels = newLabels
					}
					hasDup = true
					break
				}
			}
		}
		currentTensors[i] = t
		currentLabels[i] = labels
	}

	// Single tensor case
	if len(currentTensors) == 1 {
		t := currentTensors[0]
		inLabels := currentLabels[0]

		outSet := map[string]bool{}
		for _, l := range op.Output {
			outSet[l] = true
		}
		for ax := len(inLabels) - 1; ax >= 0; ax-- {
			if !outSet[inLabels[ax]] {
				red, err := ReduceSum(t, ax)
				if err != nil {
					return nil, err
				}
				t = red
				inLabels = append(inLabels[:ax], inLabels[ax+1:]...)
			}
		}

		if len(inLabels) == len(op.Output) && len(op.Output) > 1 {
			perm := make([]int, len(op.Output))
			needPerm := false
			for j, outLbl := range op.Output {
				found := false
				for i, inLbl := range inLabels {
					if inLbl == outLbl {
						perm[j] = i
						found = true
						if i != j {
							needPerm = true
						}
						break
					}
				}
				if !found {
					return nil, fmt.Errorf("einsum: output label %q not in input labels", outLbl)
				}
			}
			if needPerm {
				return Transpose(t, perm)
			}
		}
		return t, nil
	}

	// Multi-tensor pairwise contraction using path optimizer
	currShapes := make([]Shape, len(currentTensors))
	for i, t := range currentTensors {
		currShapes[i] = t.Shape()
	}
	adjustedOp := &EinsumOp{
		Inputs:     currentLabels,
		Output:     op.Output,
		NumIndices: op.NumIndices,
	}
	path := adjustedOp.OptimizePath(currShapes)

	tensorPool := make(map[int]*Tensor)
	labelPool := make(map[int][]string)
	for i, t := range currentTensors {
		tensorPool[i] = t
		labelPool[i] = currentLabels[i]
	}

	for stepIdx, c := range path.Contracts {
		left := tensorPool[c.LHS]
		right := tensorPool[c.RHS]
		leftLabels := labelPool[c.LHS]
		rightLabels := labelPool[c.RHS]

		leftView := NewFromData(left.Dtype(), left.Shape(), left.Data())
		leftView.SetLabels(leftLabels)
		rightView := NewFromData(right.Dtype(), right.Shape(), right.Data())
		rightView.SetLabels(rightLabels)

		res, err := TensorContract(leftView, rightView, c.SumLabels, c.OutLabels)
		if err != nil {
			return nil, fmt.Errorf("einsum: contraction step %d failed: %w", stepIdx, err)
		}
		tensorPool[-(stepIdx + 1)] = res
		labelPool[-(stepIdx + 1)] = c.OutLabels
	}

	finalTensor := tensorPool[-(len(path.Contracts))]
	finalLabels := labelPool[-(len(path.Contracts))]

	if finalTensor != nil && len(op.Output) == finalTensor.Rank() && len(op.Output) > 1 {
		perm := make([]int, len(op.Output))
		needPerm := false
		for j, outLbl := range op.Output {
			for i, inLbl := range finalLabels {
				if inLbl == outLbl {
					perm[j] = i
					if i != j {
						needPerm = true
					}
					break
				}
			}
		}
		if needPerm {
			return Transpose(finalTensor, perm)
		}
	}

	return finalTensor, nil
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
	seen := map[string]bool{}
	var out []string
	for _, s := range a {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	for _, s := range b {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}
