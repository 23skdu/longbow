package tensor

import (
	"math"
	"testing"
)

func TestLeviCivita3D(t *testing.T) {
	lc, err := LeviCivita(3, DtypeFloat64)
	if err != nil {
		t.Fatalf("LeviCivita(3) failed: %v", err)
	}
	if lc.Rank() != 3 {
		t.Fatalf("expected rank 3, got %d", lc.Rank())
	}
	for i := 0; i < 3; i++ {
		if lc.Shape()[i] != 3 {
			t.Fatalf("expected dim 3, got %d", lc.Shape()[i])
		}
	}

	// Permutation parity checks: e_012 = 1, e_102 = -1, e_001 = 0
	e012 := getScalarFloat(lc, []int{0, 1, 2})
	e102 := getScalarFloat(lc, []int{1, 0, 2})
	e001 := getScalarFloat(lc, []int{0, 0, 1})
	e201 := getScalarFloat(lc, []int{2, 0, 1})

	if e012 != 1.0 {
		t.Errorf("expected e012 == 1.0, got %f", e012)
	}
	if e102 != -1.0 {
		t.Errorf("expected e102 == -1.0, got %f", e102)
	}
	if e001 != 0.0 {
		t.Errorf("expected e001 == 0.0, got %f", e001)
	}
	if e201 != 1.0 {
		t.Errorf("expected e201 == 1.0, got %f", e201)
	}

	// Verify total non-zero elements equals 3! = 6
	nonZeroCount := 0
	for _, v := range lc.Float64s() {
		if v != 0 {
			nonZeroCount++
		}
	}
	if nonZeroCount != 6 {
		t.Errorf("expected 6 non-zero elements, got %d", nonZeroCount)
	}
}

func TestLeviCivita4D(t *testing.T) {
	lc, err := LeviCivita(4, DtypeFloat64)
	if err != nil {
		t.Fatalf("LeviCivita(4) failed: %v", err)
	}
	if lc.Rank() != 4 {
		t.Fatalf("expected rank 4, got %d", lc.Rank())
	}

	e0123 := getScalarFloat(lc, []int{0, 1, 2, 3})
	e1023 := getScalarFloat(lc, []int{1, 0, 2, 3})
	e0113 := getScalarFloat(lc, []int{0, 1, 1, 3})

	if e0123 != 1.0 {
		t.Errorf("expected e0123 == 1.0, got %f", e0123)
	}
	if e1023 != -1.0 {
		t.Errorf("expected e1023 == -1.0, got %f", e1023)
	}
	if e0113 != 0.0 {
		t.Errorf("expected e0113 == 0.0, got %f", e0113)
	}

	// 4! = 24 non-zero elements
	nonZeroCount := 0
	for _, v := range lc.Float64s() {
		if v != 0 {
			nonZeroCount++
		}
	}
	if nonZeroCount != 24 {
		t.Errorf("expected 24 non-zero elements, got %d", nonZeroCount)
	}
}

func TestMetricRaiseLower(t *testing.T) {
	// Minkowski spacetime metric eta_mu_nu = diag(-1, 1, 1, 1)
	eta := New(DtypeFloat64, Shape{4, 4})
	setScalarFloat(eta, []int{0, 0}, -1.0)
	setScalarFloat(eta, []int{1, 1}, 1.0)
	setScalarFloat(eta, []int{2, 2}, 1.0)
	setScalarFloat(eta, []int{3, 3}, 1.0)

	// Contravariant 4-velocity vector V^mu = (2, 3, 4, 5)
	vUp := New(DtypeFloat64, Shape{4})
	setScalarFloat(vUp, []int{0}, 2.0)
	setScalarFloat(vUp, []int{1}, 3.0)
	setScalarFloat(vUp, []int{2}, 4.0)
	setScalarFloat(vUp, []int{3}, 5.0)

	// Lower index: V_mu = eta_{mu nu} V^nu = (-2, 3, 4, 5)
	vDown, err := MetricLower(vUp, eta, 0)
	if err != nil {
		t.Fatalf("MetricLower failed: %v", err)
	}

	expectedDown := []float64{-2.0, 3.0, 4.0, 5.0}
	for i, exp := range expectedDown {
		got := getScalarFloat(vDown, []int{i})
		if math.Abs(got-exp) > 1e-6 {
			t.Errorf("vDown[%d]: expected %f, got %f", i, exp, got)
		}
	}

	// Invert metric: eta^{mu nu} = diag(-1, 1, 1, 1)
	etaInv, err := InvertMetric2D(eta)
	if err != nil {
		t.Fatalf("InvertMetric2D failed: %v", err)
	}

	// Raise index: V^mu = eta^{mu nu} V_nu = (2, 3, 4, 5)
	vRaised, err := MetricRaise(vDown, etaInv, 0)
	if err != nil {
		t.Fatalf("MetricRaise failed: %v", err)
	}

	expectedRaised := []float64{2.0, 3.0, 4.0, 5.0}
	for i, exp := range expectedRaised {
		got := getScalarFloat(vRaised, []int{i})
		if math.Abs(got-exp) > 1e-6 {
			t.Errorf("vRaised[%d]: expected %f, got %f", i, exp, got)
		}
	}
}

func TestChristoffelAndRiemannFlatSpace(t *testing.T) {
	dim := 4
	// Constant Minkowski metric
	eta := New(DtypeFloat64, Shape{dim, dim})
	setScalarFloat(eta, []int{0, 0}, -1.0)
	for i := 1; i < dim; i++ {
		setScalarFloat(eta, []int{i, i}, 1.0)
	}

	// All derivatives of Minkowski metric vanish
	dEta := New(DtypeFloat64, Shape{dim, dim, dim})

	gamma, err := ChristoffelSymbols(eta, dEta)
	if err != nil {
		t.Fatalf("ChristoffelSymbols failed: %v", err)
	}

	// Verify all Gamma = 0
	for _, v := range gamma.Float64s() {
		if v != 0 {
			t.Fatalf("expected flat space Gamma = 0, got %f", v)
		}
	}

	// Derivatives of Gamma vanish
	dGamma := New(DtypeFloat64, Shape{dim, dim, dim, dim})

	riemann, err := RiemannCurvature(gamma, dGamma)
	if err != nil {
		t.Fatalf("RiemannCurvature failed: %v", err)
	}

	// Verify Riemann tensor = 0
	for _, v := range riemann.Float64s() {
		if v != 0 {
			t.Fatalf("expected flat space Riemann = 0, got %f", v)
		}
	}

	// Verify Ricci tensor and scalar = 0
	ricci, err := RicciTensor(riemann)
	if err != nil {
		t.Fatalf("RicciTensor failed: %v", err)
	}
	for _, v := range ricci.Float64s() {
		if v != 0 {
			t.Fatalf("expected flat space Ricci = 0, got %f", v)
		}
	}

	etaInv, _ := InvertMetric2D(eta)
	scalar, err := RicciScalar(ricci, etaInv)
	if err != nil {
		t.Fatalf("RicciScalar failed: %v", err)
	}
	if getScalarFloat(scalar, []int{0}) != 0 {
		t.Fatalf("expected Ricci scalar = 0, got %f", getScalarFloat(scalar, []int{0}))
	}
}

func TestWedgeProductAntisymmetry(t *testing.T) {
	dim := 3
	// 1-form A = (1, 2, 3)
	formA := New(DtypeFloat64, Shape{dim})
	setScalarFloat(formA, []int{0}, 1.0)
	setScalarFloat(formA, []int{1}, 2.0)
	setScalarFloat(formA, []int{2}, 3.0)

	// 1-form B = (4, 5, 6)
	formB := New(DtypeFloat64, Shape{dim})
	setScalarFloat(formB, []int{0}, 4.0)
	setScalarFloat(formB, []int{1}, 5.0)
	setScalarFloat(formB, []int{2}, 6.0)

	// A ^ B
	wedgeAB, err := WedgeProduct(formA, formB)
	if err != nil {
		t.Fatalf("WedgeProduct(A, B) failed: %v", err)
	}

	// B ^ A
	wedgeBA, err := WedgeProduct(formB, formA)
	if err != nil {
		t.Fatalf("WedgeProduct(B, A) failed: %v", err)
	}

	// Antisymmetry check: (A ^ B)_ij = - (B ^ A)_ij
	for i := 0; i < dim; i++ {
		for j := 0; j < dim; j++ {
			ab := getScalarFloat(wedgeAB, []int{i, j})
			ba := getScalarFloat(wedgeBA, []int{i, j})
			if math.Abs(ab+ba) > 1e-6 {
				t.Errorf("wedge antisymmetry violated at (%d,%d): %f vs %f", i, j, ab, ba)
			}
			if i == j && math.Abs(ab) > 1e-6 {
				t.Errorf("diagonal element non-zero: (%d,%d) = %f", i, j, ab)
			}
		}
	}

	// A ^ A = 0
	wedgeAA, err := WedgeProduct(formA, formA)
	if err != nil {
		t.Fatalf("WedgeProduct(A, A) failed: %v", err)
	}
	for i := 0; i < dim; i++ {
		for j := 0; j < dim; j++ {
			val := getScalarFloat(wedgeAA, []int{i, j})
			if math.Abs(val) > 1e-6 {
				t.Errorf("A ^ A not zero at (%d,%d): %f", i, j, val)
			}
		}
	}
}
