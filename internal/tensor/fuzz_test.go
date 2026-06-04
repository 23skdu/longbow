package tensor

import (
	"testing"
)

// FuzzParseEinsum fuzzes the einsum expression parser.
// The parser should never panic on any input; it may return errors for invalid input.
func FuzzParseEinsum(f *testing.F) {
	seeds := []string{
		"ij,jk->ik",
		"ij,jk",
		"ii->i",
		"ab,bc,cd->ad",
		"a,b->ab",
		"ij->ji",
		"",
		",",
		",,,,",
		"->",
		"a->",
		"->a",
		"a!b,b!c->a!c",
		"   ij , jk -> ik   ",
		"a_b, b_c -> a_c",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, expr string) {
		_, _ = ParseEinsum(expr)
	})
}

// FuzzBroadcastShapes fuzzes the shape broadcasting logic.
// broadcastShapes should never panic; it may return errors for non-broadcastable shapes.
func FuzzBroadcastShapes(f *testing.F) {
	seeds := []struct {
		a1, a2, b1, b2 int
	}{
		{2, 3, 1, 3},
		{3, 1, 1, 3},
		{5, 1, 1, 1},
		{4, 0, 0, 0},
		{2, 3, 4, 5},
		{1, 1, 1, 1},
	}
	for _, s := range seeds {
		f.Add(s.a1, s.a2, s.b1, s.b2)
	}
	f.Fuzz(func(t *testing.T, a1, a2, b1, b2 int) {
		if a1 < 0 || a1 > 100 || a2 < 0 || a2 > 100 || b1 < 0 || b1 > 100 || b2 < 0 || b2 > 100 {
			return
		}
		var a, b Shape
		if a2 == 0 {
			a = Shape{a1}
		} else {
			a = Shape{a1, a2}
		}
		if b2 == 0 {
			b = Shape{b1}
		} else {
			b = Shape{b1, b2}
		}
		_, _ = broadcastShapes(a, b)
	})
}
