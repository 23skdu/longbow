package tensor

import (
	"fmt"

	"github.com/23skdu/longbow/internal/mathutil"
)

// TensorContract performs a tensor contraction (einsum-style) on two tensors.
// sumLabels are the index names to contract over; outLabels are the output index order.
func TensorContract(a, b *Tensor, sumLabels, outLabels []string) (*Tensor, error) {
	aLabels, bLabels := deduceLabels(a, b)

	// Find contraction axes
	aAxes := make([]int, 0, len(sumLabels))
	bAxes := make([]int, 0, len(sumLabels))
	for _, lbl := range sumLabels {
		for i, l := range aLabels {
			if l == lbl {
				aAxes = append(aAxes, i)
			}
		}
		for i, l := range bLabels {
			if l == lbl {
				bAxes = append(bAxes, i)
			}
		}
	}

	// Build remaining (output) axes
	aFree := make([]int, 0, len(aLabels))
	for i, lbl := range aLabels {
		if !contains(sumLabels, lbl) {
			aFree = append(aFree, i)
		}
	}
	bFree := make([]int, 0, len(bLabels))
	for i, lbl := range bLabels {
		if !contains(sumLabels, lbl) {
			bFree = append(bFree, i)
		}
	}

	// Compute output shape
	outShape := make(Shape, 0, len(aFree)+len(bFree))
	for _, i := range aFree {
		outShape = append(outShape, a.Shape()[i])
	}
	for _, i := range bFree {
		outShape = append(outShape, b.Shape()[i])
	}
	if len(outShape) == 0 {
		outShape = Shape{1}
	}

	// Reorder outLabels to match actual order
	if len(outLabels) == 0 {
		outLabels = make([]string, len(outShape))
		for i := range outLabels {
			outLabels[i] = fmt.Sprintf("x%d", i)
		}
	}

	out := New(Promote(a.Dtype(), b.Dtype()), outShape)
	if err := contractGeneric(a, b, out, aAxes, bAxes, aFree, bFree); err != nil {
		return nil, err
	}

	outLabelsActual := make([]string, 0, len(aFree)+len(bFree))
	for _, i := range aFree {
		outLabelsActual = append(outLabelsActual, aLabels[i])
	}
	for _, i := range bFree {
		outLabelsActual = append(outLabelsActual, bLabels[i])
	}
	out.SetLabels(outLabelsActual)

	TensorOperationsTotal.WithLabelValues("contract", "cpu", out.Dtype().String()).Inc()
	TensorBytesProcessedTotal.WithLabelValues("contract").Add(float64(a.NumElements()*a.Dtype().Size() + b.NumElements()*b.Dtype().Size()))

	// If target outLabels is specified and permutes outLabelsActual, transpose
	if len(outLabels) == len(outLabelsActual) && len(outLabels) > 1 {
		perm := make([]int, len(outLabels))
		needPerm := false
		for j, targetLbl := range outLabels {
			for i, actualLbl := range outLabelsActual {
				if actualLbl == targetLbl {
					perm[j] = i
					if i != j {
						needPerm = true
					}
					break
				}
			}
		}
		if needPerm {
			t, err := Transpose(out, perm)
			if err == nil {
				t.SetLabels(outLabels)
				return t, nil
			}
		}
	}

	return out, nil
}

func contractGeneric(a, b, out *Tensor, aAxes, bAxes, aFree, bFree []int) error {
	// Try CUDA acceleration if available
	if contractCUDA(a, b, out, aAxes, bAxes, aFree, bFree) {
		return nil
	}
	// Try SIMD-accelerated path for 2D matrix multiply
	if len(aFree) == 1 && len(bFree) == 1 && len(aAxes) == 1 {
		m := a.Shape()[aFree[0]]
		n := b.Shape()[bFree[0]]
		k := a.Shape()[aAxes[0]]
		if contractSIMDMatMul(a, b, out, m, n, k) {
			return nil
		}
	}
	switch {
	case a.Dtype() == DtypeFloat32 && b.Dtype() == DtypeFloat32:
		contractNumeric(a.Float32s(), a.Shape(), aFree, aAxes, b.Float32s(), b.Shape(), bFree, bAxes, out.Float32s(), 4)
	case a.Dtype() == DtypeFloat64 && b.Dtype() == DtypeFloat64:
		contractNumeric(a.Float64s(), a.Shape(), aFree, aAxes, b.Float64s(), b.Shape(), bFree, bAxes, out.Float64s(), 8)
	case a.Dtype() == DtypeComplex64 && b.Dtype() == DtypeComplex64:
		contractNumeric(a.Complex64s(), a.Shape(), aFree, aAxes, b.Complex64s(), b.Shape(), bFree, bAxes, out.Complex64s(), 8)
	case a.Dtype() == DtypeComplex128 && b.Dtype() == DtypeComplex128:
		contractNumeric(a.Complex128s(), a.Shape(), aFree, aAxes, b.Complex128s(), b.Shape(), bFree, bAxes, out.Complex128s(), 16)
	case a.Dtype() == DtypeInt64 && b.Dtype() == DtypeInt64:
		contractNumeric(a.Int64s(), a.Shape(), aFree, aAxes, b.Int64s(), b.Shape(), bFree, bAxes, out.Int64s(), 8)
	case a.Dtype() == DtypeInt32 && b.Dtype() == DtypeInt32:
		contractNumeric(a.Int32s(), a.Shape(), aFree, aAxes, b.Int32s(), b.Shape(), bFree, bAxes, out.Int32s(), 4)
	default:
		return fmt.Errorf("tensor: contraction not implemented for %s and %s", a.Dtype(), b.Dtype())
	}
	return nil
}

func contractNumeric[T float32 | float64 | complex64 | complex128 | int32 | int64](
	a []T, aShape Shape, aFree, aAxes []int,
	b []T, bShape Shape, bFree, bAxes []int,
	out []T, elemSize int,
) {
	// Compute strides for each operand
	aStrides := computeStrides(aShape, elemSize)
	bStrides := computeStrides(bShape, elemSize)
	outShape := make(Shape, 0, len(aFree)+len(bFree))
	for _, i := range aFree {
		outShape = append(outShape, aShape[i])
	}
	for _, i := range bFree {
		outShape = append(outShape, bShape[i])
	}
	outStrides := computeStrides(outShape, elemSize)

	// Compute the contraction volume
	contractSize := 1
	for _, i := range aAxes {
		contractSize *= aShape[i]
	}

	// Naive nested-loop contraction
	aFreeCount := len(aFree)
	bFreeCount := len(bFree)
	totalFree := aFreeCount + bFreeCount

	indices := make([]int, totalFree)
	aIdx := make([]int, len(aShape))
	bIdx := make([]int, len(bShape))
	outIdx := make([]int, totalFree)

	for {
		for i := 0; i < aFreeCount; i++ {
			aIdx[aFree[i]] = indices[i]
		}
		for i := 0; i < bFreeCount; i++ {
			bIdx[bFree[i]] = indices[aFreeCount+i]
		}

		outOff := offsetFromIndices(outIdx, outStrides, elemSize)

		var sum T
		totalIter := 1
		for _, ax := range aAxes {
			totalIter *= aShape[ax]
		}
		cIndices := make([]int, len(aAxes))
		for ci := 0; ci < totalIter; ci++ {
			rem := ci
			for k := len(aAxes) - 1; k >= 0; k-- {
				dim := aShape[aAxes[k]]
				cIndices[k] = rem % dim
				rem /= dim
			}
			for k, ax := range aAxes {
				aIdx[ax] = cIndices[k]
				bIdx[bAxes[k]] = cIndices[k]
			}

			aOff := offsetFromIndices(aIdx, aStrides, elemSize)
			bOff := offsetFromIndices(bIdx, bStrides, elemSize)
			sum += a[aOff/elemSize] * b[bOff/elemSize]
		}

		out[outOff/elemSize] = sum

		// Advance indices
		i := totalFree - 1
		for i >= 0 {
			indices[i]++
			outIdx[i] = indices[i]
			if indices[i] < outShape[i] {
				break
			}
			indices[i] = 0
			outIdx[i] = 0
			i--
		}
		if i < 0 {
			break
		}
	}
}

func offsetFromIndices(indices []int, strides Strides, _ int) int {
	off := 0
	for i, idx := range indices {
		off += idx * strides[i]
	}
	return off
}

// MatMul performs matrix multiplication (2D tensor contraction over the last axis of a and second-to-last of b).
func MatMul(a, b *Tensor) (*Tensor, error) {
	if a.Rank() != 2 || b.Rank() != 2 {
		return nil, fmt.Errorf("tensor: MatMul requires 2D tensors, got %dD and %dD", a.Rank(), b.Rank())
	}
	shapeA, shapeB := a.Shape(), b.Shape()
	if shapeA[1] != shapeB[0] {
		return nil, fmt.Errorf("tensor: MatMul shape mismatch: (%d,%d) x (%d,%d)", shapeA[0], shapeA[1], shapeB[0], shapeB[1])
	}
	return TensorContract(a, b, []string{"k"}, []string{"i", "j"})
}

// Add performs element-wise addition.
func Add(a, b *Tensor) (*Tensor, error) {
	return elementwiseBinary(a, b, "add", func(x, y float32) float32 { return x + y }, mathutil.AddBatch)
}

// Sub performs element-wise subtraction.
func Sub(a, b *Tensor) (*Tensor, error) {
	return elementwiseBinary(a, b, "sub", func(x, y float32) float32 { return x - y }, mathutil.SubBatch)
}

// Mul performs element-wise multiplication.
func Mul(a, b *Tensor) (*Tensor, error) {
	return elementwiseBinary(a, b, "mul", func(x, y float32) float32 { return x * y }, mathutil.MulBatch)
}

// Div performs element-wise division.
func Div(a, b *Tensor) (*Tensor, error) {
	return elementwiseBinary(a, b, "div", func(x, y float32) float32 { return x / y }, mathutil.DivBatch)
}

// Neg performs element-wise negation.
func Neg(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, "neg", func(x float32) float32 { return -x }, mathutil.NegBatch)
}

// Sin computes element-wise sine.
func Sin(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, "sin", func(x float32) float32 { return float32(sin(float64(x))) }, mathutil.SinBatch)
}

// Cos computes element-wise cosine.
func Cos(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, "cos", func(x float32) float32 { return float32(cos(float64(x))) }, mathutil.CosBatch)
}

// Tan computes element-wise tangent.
func Tan(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, "tan", func(x float32) float32 { return float32(tan(float64(x))) }, mathutil.TanBatch)
}

// Exp computes element-wise exponential.
func Exp(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, "exp", func(x float32) float32 { return float32(exp(float64(x))) }, mathutil.ExpBatch)
}

// Log computes element-wise natural logarithm.
func Log(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, "log", func(x float32) float32 { return float32(log(float64(x))) }, mathutil.LogBatch)
}

// Sqrt computes element-wise square root.
func Sqrt(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, "sqrt", func(x float32) float32 { return float32(sqrt(float64(x))) }, mathutil.SqrtBatch)
}

// Pow computes a raised to the power of b element-wise.
func Pow(a, b *Tensor) (*Tensor, error) {
	return elementwiseBinary(a, b, "pow", func(x, y float32) float32 { return float32(pow(float64(x), float64(y))) }, nil)
}

// Transpose permutes the axes of a tensor.
func Transpose(a *Tensor, perm []int) (*Tensor, error) {
	if len(perm) != a.Rank() {
		return nil, fmt.Errorf("tensor: Transpose: perm length %d != rank %d", len(perm), a.Rank())
	}
	newShape := make(Shape, a.Rank())
	for i, p := range perm {
		newShape[i] = a.Shape()[p]
	}
	out := New(a.Dtype(), newShape)

	switch a.Dtype() {
	case DtypeFloat32:
		adata := a.Float32s()
		outdata := out.Float32s()
		transposeFloat32(adata, a.Shape(), perm, outdata)
	default:
		return nil, fmt.Errorf("tensor: Transpose not implemented for %s", a.Dtype())
	}
	return out, nil
}

func transposeFloat32(data []float32, shape Shape, perm []int, out []float32) {
	rank := len(shape)
	strides := computeStrides(shape, 4)
	outStrides := computeStrides(computePermutedShape(shape, perm), 4)

	indices := make([]int, rank)
	outIdx := make([]int, rank)
	total := numElements(shape)
	for i := 0; i < total; i++ {
		// Map flat i to indices
		rem := i
		for d := rank - 1; d >= 0; d-- {
			indices[d] = rem % shape[d]
			rem /= shape[d]
		}
		srcOff := offsetFromIndices(indices, strides, 4)
		for d, p := range perm {
			outIdx[d] = indices[p]
		}
		dstOff := offsetFromIndices(outIdx, outStrides, 4)
		out[dstOff/4] = data[srcOff/4]
	}
}

func computePermutedShape(shape Shape, perm []int) Shape {
	out := make(Shape, len(perm))
	for i, p := range perm {
		out[i] = shape[p]
	}
	return out
}

// ReduceSum sums a tensor along an axis.
func ReduceSum(a *Tensor, axis int) (*Tensor, error) {
	outShape := make(Shape, 0, a.Rank()-1)
	for i, d := range a.Shape() {
		if i != axis {
			outShape = append(outShape, d)
		}
	}
	if len(outShape) == 0 {
		outShape = Shape{1}
	}
	out := New(a.Dtype(), outShape)

	switch a.Dtype() {
	case DtypeFloat32:
		reduceSumNumeric(a.Float32s(), a.Shape(), axis, out.Float32s(), 4)
	case DtypeFloat64:
		reduceSumNumeric(a.Float64s(), a.Shape(), axis, out.Float64s(), 8)
	case DtypeComplex64:
		reduceSumNumeric(a.Complex64s(), a.Shape(), axis, out.Complex64s(), 8)
	case DtypeComplex128:
		reduceSumNumeric(a.Complex128s(), a.Shape(), axis, out.Complex128s(), 16)
	case DtypeInt64:
		reduceSumNumeric(a.Int64s(), a.Shape(), axis, out.Int64s(), 8)
	case DtypeInt32:
		reduceSumNumeric(a.Int32s(), a.Shape(), axis, out.Int32s(), 4)
	default:
		return nil, fmt.Errorf("tensor: ReduceSum not implemented for %s", a.Dtype())
	}
	return out, nil
}

func reduceSumNumeric[T float32 | float64 | complex64 | complex128 | int32 | int64](
	data []T, shape Shape, axis int, out []T, elemSize int,
) {
	rank := len(shape)
	strides := computeStrides(shape, elemSize)
	outShape := make(Shape, 0, rank-1)
	for i, d := range shape {
		if i != axis {
			outShape = append(outShape, d)
		}
	}
	outStrides := computeStrides(outShape, elemSize)

	indices := make([]int, rank)
	outIdx := make([]int, rank-1)
	total := numElements(shape)
	for i := 0; i < total; i++ {
		rem := i
		for d := rank - 1; d >= 0; d-- {
			indices[d] = rem % shape[d]
			rem /= shape[d]
		}
		oi := 0
		for d := 0; d < rank; d++ {
			if d != axis {
				outIdx[oi] = indices[d]
				oi++
			}
		}
		off := offsetFromIndices(indices, strides, elemSize)
		outOff := offsetFromIndices(outIdx, outStrides, elemSize)
		out[outOff/elemSize] += data[off/elemSize]
	}
}

// Reshape returns a tensor with a new shape (same data, view semantics).
func Reshape(a *Tensor, shape Shape) *Tensor {
	return a.Reshape(shape)
}

// Contains returns true if the slice contains the string.
func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func deduceLabels(a, b *Tensor) ([]string, []string) {
	aLabels := make([]string, a.Rank())
	bLabels := make([]string, b.Rank())
	for i := range aLabels {
		aLabels[i] = fmt.Sprintf("a%d", i)
	}
	for i := range bLabels {
		bLabels[i] = fmt.Sprintf("b%d", i)
	}
	// Override with provided labels if available
	if a.Labels() != nil && len(a.Labels()) == a.Rank() {
		copy(aLabels, a.Labels())
	}
	if b.Labels() != nil && len(b.Labels()) == b.Rank() {
		copy(bLabels, b.Labels())
	}
	return aLabels, bLabels
}

type unaryFn func(float32) float32
type binaryFn func(float32, float32) float32

func elementwiseUnary(a *Tensor, opName string, fn32 unaryFn, batchF64 func([]float64) []float64) (*Tensor, error) {
	out := New(a.Dtype(), a.Shape())
	switch a.Dtype() {
	case DtypeFloat32:
		adata := a.Float32s()
		outdata := out.Float32s()
		if mathImpl == MathEML && batchF64 != nil && len(adata) >= 128 {
			f64s := make([]float64, len(adata))
			for i, v := range adata {
				f64s[i] = float64(v)
			}
			res64 := batchF64(f64s)
			for i, v := range res64 {
				outdata[i] = float32(v)
			}
			return out, nil
		}
		for i, v := range adata {
			outdata[i] = fn32(v)
		}
		return out, nil
	case DtypeFloat64:
		adata := a.Float64s()
		outdata := out.Float64s()
		if mathImpl == MathEML && batchF64 != nil {
			res64 := batchF64(adata)
			copy(outdata, res64)
			return out, nil
		}
		for i, v := range adata {
			switch opName {
			case "sin":
				outdata[i] = sin(v)
			case "cos":
				outdata[i] = cos(v)
			case "tan":
				outdata[i] = tan(v)
			case "exp":
				outdata[i] = exp(v)
			case "log":
				outdata[i] = log(v)
			case "sqrt":
				outdata[i] = sqrt(v)
			case "sinh":
				outdata[i] = sinh(v)
			case "cosh":
				outdata[i] = cosh(v)
			case "tanh":
				outdata[i] = tanh(v)
			case "asin":
				outdata[i] = asin(v)
			case "acos":
				outdata[i] = acos(v)
			case "atan":
				outdata[i] = atan(v)
			case "neg":
				outdata[i] = -v
			default:
				outdata[i] = float64(fn32(float32(v)))
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("tensor: element-wise unary not implemented for %s", a.Dtype())
	}
}

func elementwiseBinary(a, b *Tensor, opName string, fn binaryFn, batchF64 func([]float64, []float64) []float64) (*Tensor, error) {
	shape, err := broadcastShapes(a.Shape(), b.Shape())
	if err != nil {
		return nil, err
	}
	dtype := Promote(a.Dtype(), b.Dtype())
	out := New(dtype, shape)

	switch {
	case a.Dtype() == DtypeFloat32 && b.Dtype() == DtypeFloat32:
		adata := a.Float32s()
		bdata := b.Float32s()
		outdata := out.Float32s()
		if len(adata) == len(outdata) && len(bdata) == len(outdata) {
			if mathImpl == MathEML && batchF64 != nil && len(adata) >= 128 {
				f64A := make([]float64, len(adata))
				f64B := make([]float64, len(bdata))
				for i := range adata {
					f64A[i] = float64(adata[i])
					f64B[i] = float64(bdata[i])
				}
				res64 := batchF64(f64A, f64B)
				for i := range res64 {
					outdata[i] = float32(res64[i])
				}
				return out, nil
			}
			for i := range outdata {
				outdata[i] = fn(adata[i], bdata[i])
			}
		} else {
			elementwiseBinaryBroadcast(adata, a.Shape(), bdata, b.Shape(), outdata, fn)
		}
	case a.Dtype() == DtypeFloat64 && b.Dtype() == DtypeFloat64:
		adata := a.Float64s()
		bdata := b.Float64s()
		outdata := out.Float64s()
		if len(adata) == len(outdata) && len(bdata) == len(outdata) {
			if mathImpl == MathEML && batchF64 != nil {
				res64 := batchF64(adata, bdata)
				copy(outdata, res64)
				return out, nil
			}
			for i := range outdata {
				switch opName {
				case "add":
					outdata[i] = adata[i] + bdata[i]
				case "sub":
					outdata[i] = adata[i] - bdata[i]
				case "mul":
					outdata[i] = adata[i] * bdata[i]
				case "div":
					outdata[i] = adata[i] / bdata[i]
				case "pow":
					outdata[i] = pow(adata[i], bdata[i])
				default:
					outdata[i] = float64(fn(float32(adata[i]), float32(bdata[i])))
				}
			}
		} else {
			elementwiseBinaryBroadcastFloat64(adata, a.Shape(), bdata, b.Shape(), outdata, opName, fn)
		}
	default:
		return nil, fmt.Errorf("tensor: element-wise binary not implemented for %s+%s", a.Dtype(), b.Dtype())
	}
	return out, nil
}

func elementwiseBinaryBroadcastFloat64(a []float64, aShape Shape, b []float64, bShape Shape, out []float64, opName string, fn binaryFn) {
	aStrides := computeStrides(aShape, 8)
	bStrides := computeStrides(bShape, 8)

	rank := max(len(aShape), len(bShape))
	indices := make([]int, rank)
	aIdx := make([]int, len(aShape))
	bIdx := make([]int, len(bShape))

	total := numElements(outShapeForBroadcast(aShape, bShape))
	for i := 0; i < total; i++ {
		rem := i
		for d := rank - 1; d >= 0; d-- {
			indices[d] = rem % outShapeForBroadcast(aShape, bShape)[d]
			rem /= outShapeForBroadcast(aShape, bShape)[d]
		}
		for d := 0; d < len(aShape); d++ {
			aIdx[d] = indices[d+rank-len(aShape)] % aShape[d]
		}
		for d := 0; d < len(bShape); d++ {
			bIdx[d] = indices[d+rank-len(bShape)] % bShape[d]
		}
		aOff := offsetFromIndices(aIdx, aStrides, 8) / 8
		bOff := offsetFromIndices(bIdx, bStrides, 8) / 8
		va, vb := a[aOff], b[bOff]
		switch opName {
		case "add":
			out[i] = va + vb
		case "sub":
			out[i] = va - vb
		case "mul":
			out[i] = va * vb
		case "div":
			out[i] = va / vb
		case "pow":
			out[i] = pow(va, vb)
		default:
			out[i] = float64(fn(float32(va), float32(vb)))
		}
	}
}

func elementwiseBinaryBroadcast(a []float32, aShape Shape, b []float32, bShape Shape, out []float32, fn binaryFn) {
	aStrides := computeStrides(aShape, 4)
	bStrides := computeStrides(bShape, 4)

	rank := max(len(aShape), len(bShape))
	indices := make([]int, rank)
	aIdx := make([]int, len(aShape))
	bIdx := make([]int, len(bShape))

	total := numElements(outShapeForBroadcast(aShape, bShape))
	for i := 0; i < total; i++ {
		rem := i
		for d := rank - 1; d >= 0; d-- {
			indices[d] = rem % outShapeForBroadcast(aShape, bShape)[d]
			rem /= outShapeForBroadcast(aShape, bShape)[d]
		}
		for d := 0; d < len(aShape); d++ {
			aIdx[d] = indices[d+rank-len(aShape)] % aShape[d]
		}
		for d := 0; d < len(bShape); d++ {
			bIdx[d] = indices[d+rank-len(bShape)] % bShape[d]
		}
		aOff := offsetFromIndices(aIdx, aStrides, 4) / 4
		bOff := offsetFromIndices(bIdx, bStrides, 4) / 4
		out[i] = fn(a[aOff], b[bOff])
	}
}

func broadcastShapes(a, b Shape) (Shape, error) {
	rank := max(len(a), len(b))
	out := make(Shape, rank)
	pa, pb := rank-len(a), rank-len(b)
	for i := rank - 1; i >= 0; i-- {
		da := 1
		if i-pa >= 0 && i-pa < len(a) {
			da = a[i-pa]
		}
		db := 1
		if i-pb >= 0 && i-pb < len(b) {
			db = b[i-pb]
		}
		if da != db && da != 1 && db != 1 {
			return nil, fmt.Errorf("tensor: cannot broadcast shapes %v and %v", a, b)
		}
		out[i] = max(da, db)
	}
	return out, nil
}

func outShapeForBroadcast(a, b Shape) Shape {
	rank := max(len(a), len(b))
	out := make(Shape, rank)
	pa, pb := rank-len(a), rank-len(b)
	for i := rank - 1; i >= 0; i-- {
		da := 1
		if i-pa >= 0 && i-pa < len(a) {
			da = a[i-pa]
		}
		db := 1
		if i-pb >= 0 && i-pb < len(b) {
			db = b[i-pb]
		}
		out[i] = max(da, db)
	}
	return out
}

// Private math functions (will route to SIMD dispatch later)
var sin = func(x float64) float64 {
	v, _ := sinGo(x)
	return v
}
var cos = func(x float64) float64 {
	v, _ := cosGo(x)
	return v
}
var tan = func(x float64) float64 {
	v, _ := tanGo(x)
	return v
}
var exp = func(x float64) float64 {
	v, _ := expGo(x)
	return v
}
var log = func(x float64) float64 {
	v, _ := logGo(x)
	return v
}
var sqrt = func(x float64) float64 {
	v, _ := sqrtGo(x)
	return v
}
var pow = func(x, y float64) float64 {
	v, _ := powGo(x, y)
	return v
}
var sinh = func(x float64) float64 {
	e, _ := expGo(x)
	ne, _ := expGo(-x)
	return (e - ne) / 2
}
var cosh = func(x float64) float64 {
	e, _ := expGo(x)
	ne, _ := expGo(-x)
	return (e + ne) / 2
}
var tanh = func(x float64) float64 {
	e, _ := expGo(x)
	ne, _ := expGo(-x)
	return (e - ne) / (e + ne)
}
var asin = asinGo
var acos = acosGo
var atan = atanGo

// Pure Go fallbacks for math functions (will be replaced with SIMD/CUDA kernels)
func sinGo(x float64) (float64, error) {
	s, _ := sincosTaylor(x)
	return s, nil
}

func cosGo(x float64) (float64, error) {
	_, c := sincosTaylor(x)
	return c, nil
}

func sincosTaylor(x float64) (float64, float64) {
	// Reduce to [-pi, pi]
	for x > 3.141592653589793 {
		x -= 2 * 3.141592653589793
	}
	for x < -3.141592653589793 {
		x += 2 * 3.141592653589793
	}
	x2 := x * x
	// sin(x) = x - x^3/6 + x^5/120 - x^7/5040
	sin := x - x2*x/6 + x2*x2*x/120 - x2*x2*x2*x/5040
	// cos(x) = 1 - x^2/2 + x^4/24 - x^6/720
	cos := 1 - x2/2 + x2*x2/24 - x2*x2*x2/720
	return sin, cos
}

func tanGo(x float64) (float64, error) {
	s, c := sincosTaylor(x)
	return s / c, nil
}

func expGo(x float64) (float64, error) {
	// exp(x) = 1 + x + x^2/2! + x^3/3! + x^4/4!
	result := 1.0
	term := 1.0
	for i := 1; i <= 12; i++ {
		term *= x / float64(i)
		result += term
	}
	return result, nil
}

func logGo(x float64) (float64, error) {
	if x <= 0 {
		return 0, fmt.Errorf("log of non-positive number")
	}
	// ln(x) using identity: ln(x) = 2*atanh((x-1)/(x+1))
	y := (x - 1) / (x + 1)
	y2 := y * y
	// atanh(y) = y + y^3/3 + y^5/5 + ...
	result := y
	term := y
	for i := 3; i <= 21; i += 2 {
		term *= y2
		result += term / float64(i)
	}
	return 2 * result, nil
}

func sqrtGo(x float64) (float64, error) {
	if x < 0 {
		return 0, fmt.Errorf("sqrt of negative number")
	}
	// Newton's method
	if x == 0 {
		return 0, nil
	}
	guess := x
	for i := 0; i < 10; i++ {
		guess = (guess + x/guess) / 2
	}
	return guess, nil
}

func powGo(x, y float64) (float64, error) {
	if y == 0 {
		return 1, nil
	}
	if y == 1 {
		return x, nil
	}
	if y == 2 {
		return x * x, nil
	}
	// x^y = exp(y * ln(x))
	lnx, err := logGo(x)
	if err != nil {
		return 0, err
	}
	return expGo(y * lnx)
}

// Asin computes element-wise arcsine.
func Asin(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, "asin", func(x float32) float32 { return float32(asin(float64(x))) }, nil)
}

// Acos computes element-wise arccosine.
func Acos(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, "acos", func(x float32) float32 { return float32(acos(float64(x))) }, nil)
}

// Atan computes element-wise arctangent.
func Atan(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, "atan", func(x float32) float32 { return float32(atan(float64(x))) }, nil)
}

// Sinh computes element-wise hyperbolic sine.
func Sinh(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, "sinh", func(x float32) float32 { return float32(sinh(float64(x))) }, mathutil.SinhBatch)
}

// Cosh computes element-wise hyperbolic cosine.
func Cosh(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, "cosh", func(x float32) float32 { return float32(cosh(float64(x))) }, mathutil.CoshBatch)
}

// Tanh computes element-wise hyperbolic tangent.
func Tanh(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, "tanh", func(x float32) float32 { return float32(tanh(float64(x))) }, mathutil.TanhBatch)
}

// Erf computes the error function element-wise.
func Erf(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, "erf", func(x float32) float32 { return float32(erfGo(float64(x))) }, nil)
}

func asinGo(x float64) float64 {
	// asin(x) = atan(x / sqrt(1 - x^2))
	sqrtVal, _ := sqrtGo(1 - x*x)
	return atanGo(x / sqrtVal)
}

func acosGo(x float64) float64 {
	// acos(x) = pi/2 - asin(x)
	return 1.5707963267948966 - asinGo(x)
}

func atanGo(x float64) float64 {
	// atan(x) = x - x^3/3 + x^5/5 - x^7/7 + ...
	if x < -1 || x > 1 {
		// atan(x) = pi/2 - atan(1/x) for x > 0
		if x > 0 {
			return 1.5707963267948966 - atanGo(1/x)
		}
		return -1.5707963267948966 - atanGo(1/x)
	}
	x2 := x * x
	result := x
	term := x
	for i := 3; i <= 31; i += 2 {
		term *= -x2
		result += term / float64(i)
	}
	return result
}

func erfGo(x float64) float64 {
	// Abramowitz and Stegun approximation: erf(x) = 1 - poly(t) * exp(-x²)
	// where t = 1/(1+px), poly(t) = t*(a₁ + t*(a₂ + t*(a₃ + t*(a₄ + t*a₅))))
	sign := 1.0
	if x < 0 {
		x = -x
		sign = -1.0
	}
	t := 1.0 / (1.0 + 0.3275911*x)
	poly := t * (0.254829592 +
		t*(-0.284496736+
			t*(1.421413741+
				t*(-1.453152027+
					t*1.061405429))))
	expVal, _ := expGo(-x * x)
	result := 1.0 - poly*expVal
	return sign * result
}
