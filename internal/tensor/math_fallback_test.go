package tensor

import (
	"math"
	"testing"
)

func TestSinGo(t *testing.T) {
	v, err := sinGo(0)
	assertNoErr(t, err)
	if v != 0 {
		t.Errorf("sinGo(0) = %v, want 0", v)
	}
	v, err = sinGo(1.57079632679) // pi/2
	assertNoErr(t, err)
	if v < 0.9 || v > 1.1 {
		t.Errorf("sinGo(pi/2) = %v, want ~1", v)
	}
}

func TestCosGo(t *testing.T) {
	v, err := cosGo(0)
	assertNoErr(t, err)
	if v != 1 {
		t.Errorf("cosGo(0) = %v, want 1", v)
	}
	v, err = cosGo(3.14159265359) // pi
	assertNoErr(t, err)
	if v > -0.8 || v < -1.4 {
		t.Errorf("cosGo(pi) = %v, want ~-1 (tol 0.4)", v)
	}
}

func TestLogGo(t *testing.T) {
	v, err := logGo(1)
	assertNoErr(t, err)
	if v != 0 {
		t.Errorf("logGo(1) = %v, want 0", v)
	}
	v, err = logGo(math.E)
	assertNoErr(t, err)
	if v < 0.85 || v > 1.15 {
		t.Errorf("logGo(e) = %v, want ~1 (tol 0.15)", v)
	}
	_, err = logGo(-1)
	if err == nil {
		t.Error("expected error for log of negative")
	}
}

func TestPowGo(t *testing.T) {
	v, err := powGo(2, 0)
	assertNoErr(t, err)
	if v != 1 {
		t.Errorf("powGo(2,0) = %v, want 1", v)
	}
	v, err = powGo(3, 1)
	assertNoErr(t, err)
	if v != 3 {
		t.Errorf("powGo(3,1) = %v, want 3", v)
	}
	v, err = powGo(5, 2)
	assertNoErr(t, err)
	if v != 25 {
		t.Errorf("powGo(5,2) = %v, want 25", v)
	}
	v, err = powGo(2, 3)
	assertNoErr(t, err)
	if v < 7 || v > 9 {
		t.Errorf("powGo(2,3) = %v, want ~8", v)
	}
	_, err = powGo(-1, 0.5)
	if err == nil {
		t.Error("expected error for pow of negative base with non-integer exponent")
	}
}

func TestSincosTaylorRangeReduction(t *testing.T) {
	s, c := sincosTaylor(10) // > pi, forces range reduction
	if s < -1.1 || s > 1.1 {
		t.Errorf("sin(10) out of range: %v", s)
	}
	if c < -1.1 || c > 1.1 {
		t.Errorf("cos(10) out of range: %v", c)
	}
	s2, c2 := sincosTaylor(-5) // < -pi, forces range reduction
	if s2 < -1.1 || s2 > 1.1 {
		t.Errorf("sin(-5) out of range: %v", s2)
	}
	if c2 < -1.1 || c2 > 1.1 {
		t.Errorf("cos(-5) out of range: %v", c2)
	}
}

func TestSqrtGo(t *testing.T) {
	v, err := sqrtGo(0)
	assertNoErr(t, err)
	if v != 0 {
		t.Errorf("sqrtGo(0) = %v, want 0", v)
	}
	v, err = sqrtGo(9)
	assertNoErr(t, err)
	if v < 2.9 || v > 3.1 {
		t.Errorf("sqrtGo(9) = %v, want ~3", v)
	}
	_, err = sqrtGo(-1)
	if err == nil {
		t.Error("expected error for sqrt of negative")
	}
}

func TestExpGoEdgeCase(t *testing.T) {
	v, err := expGo(0)
	assertNoErr(t, err)
	if v != 1 {
		t.Errorf("expGo(0) = %v, want 1", v)
	}
	v, err = expGo(1)
	assertNoErr(t, err)
	if v < 2.7 || v > 2.72 {
		t.Errorf("expGo(1) = %v, want ~2.718", v)
	}
	v, err = expGo(-1)
	assertNoErr(t, err)
	if v < 0.36 || v > 0.37 {
		t.Errorf("expGo(-1) = %v, want ~0.368", v)
	}
}
