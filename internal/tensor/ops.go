package tensor

import (
	"fmt"
)

// TensorContract performs a tensor contraction (einsum-style) on two tensors.
// sumLabels are the index names to contract over; outLabels are the output index order.
func TensorContract(a, b *Tensor, sumLabels, outLabels []string) (*Tensor, error) {
	aLabels, bLabels := deduceLabels(a, b, sumLabels, outLabels)

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
	return out, nil
}

func contractGeneric(a, b, out *Tensor, aAxes, bAxes, aFree, bFree []int) error {
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
		adata := a.Float32s()
		bdata := b.Float32s()
		outdata := out.Float32s()
		contractFloat32(adata, a.Shape(), aFree, aAxes, bdata, b.Shape(), bFree, bAxes, outdata)
	default:
		return fmt.Errorf("tensor: contraction not implemented for %s", a.Dtype())
	}
	return nil
}

func contractFloat32(a []float32, aShape Shape, aFree, aAxes []int,
	b []float32, bShape Shape, bFree, bAxes []int,
	out []float32) {

	// Compute strides for each operand
	aStrides := computeStrides(aShape, 4)
	bStrides := computeStrides(bShape, 4)
	outShape := make(Shape, 0, len(aFree)+len(bFree))
	for _, i := range aFree {
		outShape = append(outShape, aShape[i])
	}
	for _, i := range bFree {
		outShape = append(outShape, bShape[i])
	}
	outStrides := computeStrides(outShape, 4)

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
		// Map free indices to aFree position
		for i := 0; i < aFreeCount; i++ {
			aIdx[aFree[i]] = indices[i]
		}
		for i := 0; i < bFreeCount; i++ {
			bIdx[bFree[i]] = indices[aFreeCount+i]
		}

		// Compute output offset
		outOff := offsetFromIndices(outIdx, outStrides, 4)

		// Contract over summed axes
		var sum float32
		// iteration over contract axes
		cIndices := make([]int, len(aAxes))
		totalIter := 1
		for _, ax := range aAxes {
			totalIter *= aShape[ax]
		}
		for ci := 0; ci < totalIter; ci++ {
			// Map cIndices to positions
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

			aOff := offsetFromIndices(aIdx, aStrides, 4)
			bOff := offsetFromIndices(bIdx, bStrides, 4)
			sum += a[aOff/4] * b[bOff/4]
		}

		out[outOff/4] = sum

		// Advance indices
		i := totalFree - 1
		for i >= 0 {
			indices[i]++
			if indices[i] < outShape[i] {
				break
			}
			indices[i] = 0
			i--
		}
		if i < 0 {
			break
		}
	}
}

func offsetFromIndices(indices []int, strides Strides, elemSize int) int {
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
	return elementwiseBinary(a, b, func(x, y float32) float32 { return x + y })
}

// Sub performs element-wise subtraction.
func Sub(a, b *Tensor) (*Tensor, error) {
	return elementwiseBinary(a, b, func(x, y float32) float32 { return x - y })
}

// Mul performs element-wise multiplication.
func Mul(a, b *Tensor) (*Tensor, error) {
	return elementwiseBinary(a, b, func(x, y float32) float32 { return x * y })
}

// Div performs element-wise division.
func Div(a, b *Tensor) (*Tensor, error) {
	return elementwiseBinary(a, b, func(x, y float32) float32 { return x / y })
}

// Neg performs element-wise negation.
func Neg(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, func(x float32) float32 { return -x })
}

// Sin computes element-wise sine.
func Sin(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, func(x float32) float32 { return float32(sin(float64(x))) })
}

// Cos computes element-wise cosine.
func Cos(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, func(x float32) float32 { return float32(cos(float64(x))) })
}

// Tan computes element-wise tangent.
func Tan(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, func(x float32) float32 { return float32(tan(float64(x))) })
}

// Exp computes element-wise exponential.
func Exp(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, func(x float32) float32 { return float32(exp(float64(x))) })
}

// Log computes element-wise natural logarithm.
func Log(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, func(x float32) float32 { return float32(log(float64(x))) })
}

// Sqrt computes element-wise square root.
func Sqrt(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, func(x float32) float32 { return float32(sqrt(float64(x))) })
}

// Pow computes a raised to the power of b element-wise.
func Pow(a, b *Tensor) (*Tensor, error) {
	return elementwiseBinary(a, b, func(x, y float32) float32 { return float32(pow(float64(x), float64(y))) })
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
		adata := a.Float32s()
		outdata := out.Float32s()
		reduceSumFloat32(adata, a.Shape(), axis, outdata)
	default:
		return nil, fmt.Errorf("tensor: ReduceSum not implemented for %s", a.Dtype())
	}
	return out, nil
}

func reduceSumFloat32(data []float32, shape Shape, axis int, out []float32) {
	rank := len(shape)
	strides := computeStrides(shape, 4)
	outShape := make(Shape, 0, rank-1)
	for i, d := range shape {
		if i != axis {
			outShape = append(outShape, d)
		}
	}
	outStrides := computeStrides(outShape, 4)

	indices := make([]int, rank)
	outIdx := make([]int, rank-1)
	total := numElements(shape)
	for i := 0; i < total; i++ {
		rem := i
		for d := rank - 1; d >= 0; d-- {
			indices[d] = rem % shape[d]
			rem /= shape[d]
		}
		// Map to output index (skip axis)
		oi := 0
		for d := 0; d < rank; d++ {
			if d != axis {
				outIdx[oi] = indices[d]
				oi++
			}
		}
		off := offsetFromIndices(indices, strides, 4)
		outOff := offsetFromIndices(outIdx, outStrides, 4)
		out[outOff/4] += data[off/4]
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

func deduceLabels(a, b *Tensor, sumLabels, outLabels []string) ([]string, []string) {
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

func elementwiseUnary(a *Tensor, fn unaryFn) (*Tensor, error) {
	out := New(a.Dtype(), a.Shape())
	switch a.Dtype() {
	case DtypeFloat32:
		adata := a.Float32s()
		outdata := out.Float32s()
		for i, v := range adata {
			outdata[i] = fn(v)
		}
	default:
		return nil, fmt.Errorf("tensor: element-wise unary not implemented for %s", a.Dtype())
	}
	return out, nil
}

func elementwiseBinary(a, b *Tensor, fn binaryFn) (*Tensor, error) {
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
			for i := range outdata {
				outdata[i] = fn(adata[i], bdata[i])
			}
		} else {
			elementwiseBinaryBroadcast(adata, a.Shape(), bdata, b.Shape(), outdata, fn)
		}
	default:
		return nil, fmt.Errorf("tensor: element-wise binary not implemented for %s+%s", a.Dtype(), b.Dtype())
	}
	return out, nil
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
	return elementwiseUnary(a, func(x float32) float32 { return float32(asinGo(float64(x))) })
}

// Acos computes element-wise arccosine.
func Acos(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, func(x float32) float32 { return float32(acosGo(float64(x))) })
}

// Atan computes element-wise arctangent.
func Atan(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, func(x float32) float32 { return float32(atanGo(float64(x))) })
}

// Sinh computes element-wise hyperbolic sine.
func Sinh(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, func(x float32) float32 {
		e, _ := expGo(float64(x))
		ne, _ := expGo(-float64(x))
		return float32((e - ne) / 2)
	})
}

// Cosh computes element-wise hyperbolic cosine.
func Cosh(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, func(x float32) float32 {
		e, _ := expGo(float64(x))
		ne, _ := expGo(-float64(x))
		return float32((e + ne) / 2)
	})
}

// Tanh computes element-wise hyperbolic tangent.
func Tanh(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, func(x float32) float32 {
		e, _ := expGo(float64(x))
		ne, _ := expGo(-float64(x))
		return float32((e - ne) / (e + ne))
	})
}

// Erf computes the error function element-wise.
func Erf(a *Tensor) (*Tensor, error) {
	return elementwiseUnary(a, func(x float32) float32 { return float32(erfGo(float64(x))) })
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
		t * (-0.284496736 +
			t * (1.421413741 +
				t * (-1.453152027 +
					t * 1.061405429))))
	expVal, _ := expGo(-x * x)
	result := 1.0 - poly*expVal
	return sign * result
}
