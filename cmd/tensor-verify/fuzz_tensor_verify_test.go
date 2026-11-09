package main

import (
	"testing"

	"github.com/23skdu/longbow/internal/tensor"
)

func FuzzShapeEqual(f *testing.F) {
	f.Add(2, 3, 2, 3)
	f.Add(1, 1, 2, 2)
	f.Fuzz(func(t *testing.T, a0, a1, b0, b1 int) {
		if a0 < 0 || a0 > 8 || a1 < 0 || a1 > 8 || b0 < 0 || b0 > 8 || b1 < 0 || b1 > 8 {
			return
		}
		a := tensor.Shape{a0, a1}
		b := tensor.Shape{b0, b1}
		_ = shapeEqual(a, b)
	})
}
