//go:build !darwin || !arm64

package amx

import (
	"testing"
)

func TestAMXStub(t *testing.T) {
	a := []float32{1, 2, 3, 4}
	b := []float32{5, 6, 7, 8}

	dot, err := DotAMX(a, b)
	if err != nil || dot != 0 {
		t.Errorf("DotAMX stub failed: got %v, err %v", dot, err)
	}

	l2, err := L2AMX(a, b)
	if err != nil || l2 != 0 {
		t.Errorf("L2AMX stub failed: got %v, err %v", l2, err)
	}
}
