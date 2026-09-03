package tensor

import (
	"fmt"
	"math"
)

// LeviCivita generates the totally antisymmetric Levi-Civita permutation tensor of rank `dim`.
// For dim=3, it produces the standard 3D permutation tensor epsilon_ijk.
// For dim=4, it produces the 4D relativistic permutation tensor epsilon_mu_nu_rho_sigma.
func LeviCivita(dim int, dt Dtype) (*Tensor, error) {
	if dim < 2 || dim > 8 {
		return nil, fmt.Errorf("tensor: LeviCivita dim %d out of supported range [2, 8]", dim)
	}
	if dt == DtypeInvalid {
		dt = DtypeFloat64
	}

	shape := make(Shape, dim)
	for i := range shape {
		shape[i] = dim
	}
	out := New(dt, shape)

	total := numElements(shape)
	indices := make([]int, dim)

	for i := 0; i < total; i++ {
		rem := i
		for d := dim - 1; d >= 0; d-- {
			indices[d] = rem % dim
			rem /= dim
		}

		// Check for repeated indices (value is 0)
		hasDuplicate := false
		seen := make([]bool, dim)
		for _, idx := range indices {
			if seen[idx] {
				hasDuplicate = true
				break
			}
			seen[idx] = true
		}
		if hasDuplicate {
			continue
		}

		// Count parity of permutation
		inversions := 0
		for j := 0; j < dim; j++ {
			for k := j + 1; k < dim; k++ {
				if indices[j] > indices[k] {
					inversions++
				}
			}
		}

		val := 1.0
		if inversions%2 != 0 {
			val = -1.0
		}

		// Write to tensor
		setScalarFloat(out, indices, val)
	}

	return out, nil
}

// InvertMetric2D computes the matrix inverse of a rank-2 metric tensor g_ab -> g^ab.
func InvertMetric2D(metric *Tensor) (*Tensor, error) {
	if metric.Rank() != 2 || metric.Shape()[0] != metric.Shape()[1] {
		return nil, fmt.Errorf("tensor: metric must be square rank-2 matrix (got shape %v)", metric.Shape())
	}
	n := metric.Shape()[0]
	dt := metric.Dtype()
	if dt != DtypeFloat64 && dt != DtypeFloat32 {
		dt = DtypeFloat64
	}

	// Read into 2D float64 matrix
	mat := make([][]float64, n)
	inv := make([][]float64, n)
	for i := 0; i < n; i++ {
		mat[i] = make([]float64, n)
		inv[i] = make([]float64, n)
		inv[i][i] = 1.0
		for j := 0; j < n; j++ {
			mat[i][j] = getScalarFloat(metric, []int{i, j})
		}
	}

	// Gauss-Jordan elimination with partial pivoting
	for i := 0; i < n; i++ {
		pivot := i
		for j := i + 1; j < n; j++ {
			if math.Abs(mat[j][i]) > math.Abs(mat[pivot][i]) {
				pivot = j
			}
		}
		if math.Abs(mat[pivot][i]) < 1e-15 {
			return nil, fmt.Errorf("tensor: singular metric tensor cannot be inverted")
		}
		mat[i], mat[pivot] = mat[pivot], mat[i]
		inv[i], inv[pivot] = inv[pivot], inv[i]

		pivotVal := mat[i][i]
		for j := 0; j < n; j++ {
			mat[i][j] /= pivotVal
			inv[i][j] /= pivotVal
		}

		for k := 0; k < n; k++ {
			if k != i {
				factor := mat[k][i]
				for j := 0; j < n; j++ {
					mat[k][j] -= factor * mat[i][j]
					inv[k][j] -= factor * inv[i][j]
				}
			}
		}
	}

	out := New(dt, Shape{n, n})
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			setScalarFloat(out, []int{i, j}, inv[i][j])
		}
	}
	return out, nil
}

// MetricRaise raises a covariant index on tensor T at `axis` using inverse metric g^{mu nu}:
// T_{...mu...} g^{mu nu} -> T_{...}^{...nu...}
func MetricRaise(t *Tensor, metricInv *Tensor, axis int) (*Tensor, error) {
	if metricInv.Rank() != 2 || metricInv.Shape()[0] != metricInv.Shape()[1] {
		return nil, fmt.Errorf("tensor: inverse metric must be rank-2 square matrix")
	}
	if axis < 0 || axis >= t.Rank() {
		return nil, fmt.Errorf("tensor: axis %d out of range [0, %d)", axis, t.Rank())
	}
	dim := metricInv.Shape()[0]
	if t.Shape()[axis] != dim {
		return nil, fmt.Errorf("tensor: metric dimension %d does not match tensor axis dimension %d", dim, t.Shape()[axis])
	}

	// Contract axis with metricInv axis 0: sumLabels=["m"], metric has ["m", "n"]
	tLabels := make([]string, t.Rank())
	for i := range tLabels {
		tLabels[i] = fmt.Sprintf("a%d", i)
	}
	contractLabel := tLabels[axis]
	newLabel := "raised_idx"
	mLabels := []string{contractLabel, newLabel}

	outLabels := make([]string, 0, t.Rank())
	for i, lbl := range tLabels {
		if i == axis {
			outLabels = append(outLabels, newLabel)
		} else {
			outLabels = append(outLabels, lbl)
		}
	}

	tCopy := t.Clone()
	tCopy.SetLabels(tLabels)
	mCopy := metricInv.Clone()
	mCopy.SetLabels(mLabels)

	res, err := TensorContract(tCopy, mCopy, []string{contractLabel}, outLabels)
	if err != nil {
		return nil, err
	}

	// If contraction put the new index at the end, transpose back to original position
	if axis != t.Rank()-1 {
		perm := make([]int, res.Rank())
		// Current output order from TensorContract has free(a) then free(b)
		// We want the raised index at position `axis`
		for i := 0; i < axis; i++ {
			perm[i] = i
		}
		perm[axis] = res.Rank() - 1 // the new index was from bFree
		for i := axis + 1; i < res.Rank(); i++ {
			perm[i] = i - 1
		}
		return Transpose(res, perm)
	}
	return res, nil
}

// MetricLower lowers a contravariant index on tensor T at `axis` using metric g_{mu nu}:
// T^{...mu...} g_{mu nu} -> T^{...}_{...nu...}
func MetricLower(t *Tensor, metric *Tensor, axis int) (*Tensor, error) {
	return MetricRaise(t, metric, axis)
}

// ChristoffelSymbols computes Christoffel connection coefficients of the second kind:
// Gamma^sigma_{mu nu} = 0.5 * g^{sigma rho} * (d_mu g_{nu rho} + d_nu g_{mu rho} - d_rho g_{mu nu})
// metric has shape [D, D] (g_ab)
// metricDeriv has shape [D, D, D] representing d_rho g_{mu nu} (axis 0: derivative coord rho, axis 1: mu, axis 2: nu)
func ChristoffelSymbols(metric, metricDeriv *Tensor) (*Tensor, error) {
	if metric.Rank() != 2 || metric.Shape()[0] != metric.Shape()[1] {
		return nil, fmt.Errorf("tensor: metric must be square rank-2 matrix")
	}
	d := metric.Shape()[0]
	if metricDeriv.Rank() != 3 || metricDeriv.Shape()[0] != d || metricDeriv.Shape()[1] != d || metricDeriv.Shape()[2] != d {
		return nil, fmt.Errorf("tensor: metricDeriv must have shape [%d, %d, %d]", d, d, d)
	}

	invMetric, err := InvertMetric2D(metric)
	if err != nil {
		return nil, fmt.Errorf("tensor: failed to invert metric for Christoffel symbols: %w", err)
	}

	dt := metric.Dtype()
	if dt != DtypeFloat32 {
		dt = DtypeFloat64
	}

	// Gamma has indices [sigma, mu, nu]
	gamma := New(dt, Shape{d, d, d})

	for sigma := 0; sigma < d; sigma++ {
		for mu := 0; mu < d; mu++ {
			for nu := 0; nu < d; nu++ {
				var sum float64
				for rho := 0; rho < d; rho++ {
					gInv := getScalarFloat(invMetric, []int{sigma, rho})
					// d_mu g_{nu rho} -> metricDeriv[mu, nu, rho]
					// d_nu g_{mu rho} -> metricDeriv[nu, mu, rho]
					// d_rho g_{mu nu} -> metricDeriv[rho, mu, nu]
					term1 := getScalarFloat(metricDeriv, []int{mu, nu, rho})
					term2 := getScalarFloat(metricDeriv, []int{nu, mu, rho})
					term3 := getScalarFloat(metricDeriv, []int{rho, mu, nu})

					sum += 0.5 * gInv * (term1 + term2 - term3)
				}
				setScalarFloat(gamma, []int{sigma, mu, nu}, sum)
			}
		}
	}

	return gamma, nil
}

// RiemannCurvature computes the Riemann curvature tensor:
// R^rho_{sigma mu nu} = d_mu Gamma^rho_{nu sigma} - d_nu Gamma^rho_{mu sigma}
//                     + Gamma^rho_{mu lambda} Gamma^lambda_{nu sigma} - Gamma^rho_{nu lambda} Gamma^lambda_{mu sigma}
// christoffel has shape [D, D, D] (indices: rho, mu, nu)
// christoffelDeriv has shape [D, D, D, D] (axis 0: derivative coord lambda, axes 1..3: rho, mu, nu)
func RiemannCurvature(christoffel, christoffelDeriv *Tensor) (*Tensor, error) {
	if christoffel.Rank() != 3 {
		return nil, fmt.Errorf("tensor: christoffel must have rank 3 [rho, mu, nu]")
	}
	d := christoffel.Shape()[0]
	if christoffel.Shape()[1] != d || christoffel.Shape()[2] != d {
		return nil, fmt.Errorf("tensor: christoffel dimensions must match [%d, %d, %d]", d, d, d)
	}
	if christoffelDeriv.Rank() != 4 || christoffelDeriv.Shape()[0] != d ||
		christoffelDeriv.Shape()[1] != d || christoffelDeriv.Shape()[2] != d || christoffelDeriv.Shape()[3] != d {
		return nil, fmt.Errorf("tensor: christoffelDeriv must have shape [%d, %d, %d, %d]", d, d, d, d)
	}

	dt := christoffel.Dtype()
	if dt != DtypeFloat32 {
		dt = DtypeFloat64
	}

	// Shape: [rho, sigma, mu, nu]
	riemann := New(dt, Shape{d, d, d, d})

	for rho := 0; rho < d; rho++ {
		for sigma := 0; sigma < d; sigma++ {
			for mu := 0; mu < d; mu++ {
				for nu := 0; nu < d; nu++ {
					// d_mu Gamma^rho_{nu sigma} -> christoffelDeriv[mu, rho, nu, sigma]
					dMuGamma := getScalarFloat(christoffelDeriv, []int{mu, rho, nu, sigma})
					// d_nu Gamma^rho_{mu sigma} -> christoffelDeriv[nu, rho, mu, sigma]
					dNuGamma := getScalarFloat(christoffelDeriv, []int{nu, rho, mu, sigma})

					var quadraticSum float64
					for lambda := 0; lambda < d; lambda++ {
						g1 := getScalarFloat(christoffel, []int{rho, mu, lambda})
						g2 := getScalarFloat(christoffel, []int{lambda, nu, sigma})

						g3 := getScalarFloat(christoffel, []int{rho, nu, lambda})
						g4 := getScalarFloat(christoffel, []int{lambda, mu, sigma})

						quadraticSum += (g1 * g2) - (g3 * g4)
					}

					val := (dMuGamma - dNuGamma) + quadraticSum
					setScalarFloat(riemann, []int{rho, sigma, mu, nu}, val)
				}
			}
		}
	}

	return riemann, nil
}

// RicciTensor contracts the Riemann curvature tensor to obtain the Ricci tensor:
// R_{sigma nu} = R^mu_{sigma mu nu}
func RicciTensor(riemann *Tensor) (*Tensor, error) {
	if riemann.Rank() != 4 {
		return nil, fmt.Errorf("tensor: riemann tensor must have rank 4 (got %d)", riemann.Rank())
	}
	d := riemann.Shape()[0]
	for i := 1; i < 4; i++ {
		if riemann.Shape()[i] != d {
			return nil, fmt.Errorf("tensor: riemann tensor axes must all equal %d", d)
		}
	}

	dt := riemann.Dtype()
	ricci := New(dt, Shape{d, d})

	for sigma := 0; sigma < d; sigma++ {
		for nu := 0; nu < d; nu++ {
			var sum float64
			for mu := 0; mu < d; mu++ {
				sum += getScalarFloat(riemann, []int{mu, sigma, mu, nu})
			}
			setScalarFloat(ricci, []int{sigma, nu}, sum)
		}
	}

	return ricci, nil
}

// RicciScalar contracts the Ricci tensor with the inverse metric to compute the Ricci curvature scalar:
// R = g^{sigma nu} R_{sigma nu}
func RicciScalar(ricci, metricInv *Tensor) (*Tensor, error) {
	if ricci.Rank() != 2 || metricInv.Rank() != 2 {
		return nil, fmt.Errorf("tensor: ricci and metricInv must both be rank 2")
	}
	d := ricci.Shape()[0]
	if ricci.Shape()[1] != d || metricInv.Shape()[0] != d || metricInv.Shape()[1] != d {
		return nil, fmt.Errorf("tensor: ricci and metricInv dimensions must match square %d", d)
	}

	var scalar float64
	for sigma := 0; sigma < d; sigma++ {
		for nu := 0; nu < d; nu++ {
			gInv := getScalarFloat(metricInv, []int{sigma, nu})
			rVal := getScalarFloat(ricci, []int{sigma, nu})
			scalar += gInv * rVal
		}
	}

	out := New(ricci.Dtype(), Shape{1})
	setScalarFloat(out, []int{0}, scalar)
	return out, nil
}

// WedgeProduct computes the exterior differential wedge product of a p-form and a q-form (A ^ B).
// For 1-forms A and B of dimension D, (A ^ B)_ij = A_i B_j - A_j B_i.
// In general for forms of rank p and q, the result has rank p + q and is fully antisymmetrized.
func WedgeProduct(formA, formB *Tensor) (*Tensor, error) {
	p := formA.Rank()
	q := formB.Rank()
	if p == 0 || q == 0 {
		return nil, fmt.Errorf("tensor: wedge product requires non-scalar forms")
	}
	dim := formA.Shape()[0]
	for _, s := range formA.Shape() {
		if s != dim {
			return nil, fmt.Errorf("tensor: formA must have equal axis sizes")
		}
	}
	for _, s := range formB.Shape() {
		if s != dim {
			return nil, fmt.Errorf("tensor: formB must have equal axis sizes")
		}
	}

	totalRank := p + q
	outShape := make(Shape, totalRank)
	for i := range outShape {
		outShape[i] = dim
	}

	dt := Promote(formA.Dtype(), formB.Dtype())
	if dt != DtypeFloat32 {
		dt = DtypeFloat64
	}
	out := New(dt, outShape)

	totalOut := numElements(outShape)
	indices := make([]int, totalRank)

	// Pre-generate all permutations of totalRank
	perms, parities := generatePermutations(totalRank)
	normFactor := 1.0 / (factorial(p) * factorial(q))

	for i := 0; i < totalOut; i++ {
		rem := i
		for d := totalRank - 1; d >= 0; d-- {
			indices[d] = rem % dim
			rem /= dim
		}

		var sum float64
		permIdx := make([]int, totalRank)
		for pi, perm := range perms {
			for k, pVal := range perm {
				permIdx[k] = indices[pVal]
			}

			// Sub-indices for A (first p) and B (remaining q)
			aIdx := permIdx[:p]
			bIdx := permIdx[p:]

			aVal := getScalarFloat(formA, aIdx)
			bVal := getScalarFloat(formB, bIdx)

			sign := float64(parities[pi])
			sum += sign * (aVal * bVal)
		}

		setScalarFloat(out, indices, sum*normFactor)
	}

	return out, nil
}

// Helpers for scalar read/write across float dtypes
func getScalarFloat(t *Tensor, indices []int) float64 {
	strides := computeStrides(t.Shape(), t.Dtype().Size())
	off := offsetFromIndices(indices, strides, t.Dtype().Size())
	switch t.Dtype() {
	case DtypeFloat32:
		return float64(t.Float32s()[off/4])
	case DtypeFloat64:
		return t.Float64s()[off/8]
	default:
		return 0
	}
}

func setScalarFloat(t *Tensor, indices []int, val float64) {
	strides := computeStrides(t.Shape(), t.Dtype().Size())
	off := offsetFromIndices(indices, strides, t.Dtype().Size())
	switch t.Dtype() {
	case DtypeFloat32:
		t.Float32s()[off/4] = float32(val)
	case DtypeFloat64:
		t.Float64s()[off/8] = val
	}
}

func factorial(n int) float64 {
	res := 1.0
	for i := 2; i <= n; i++ {
		res *= float64(i)
	}
	return res
}

func generatePermutations(n int) ([][]int, []int) {
	var perms [][]int
	var parities []int

	arr := make([]int, n)
	for i := range arr {
		arr[i] = i
	}

	var permute func([]int, int)
	permute = func(a []int, l int) {
		if l == n {
			p := make([]int, n)
			copy(p, a)
			perms = append(perms, p)

			// Count inversions
			inv := 0
			for i := 0; i < n; i++ {
				for j := i + 1; j < n; j++ {
					if p[i] > p[j] {
						inv++
					}
				}
			}
			if inv%2 == 0 {
				parities = append(parities, 1)
			} else {
				parities = append(parities, -1)
			}
			return
		}
		for i := l; i < n; i++ {
			a[l], a[i] = a[i], a[l]
			permute(a, l+1)
			a[l], a[i] = a[i], a[l]
		}
	}
	permute(arr, 0)
	return perms, parities
}
