package store

import "testing"

func TestCanScaleDownIngestion(t *testing.T) {
	cases := []struct {
		pending, cur, min int
		want              bool
	}{
		{0, 5, 4, true},
		{1, 5, 4, false},
		{0, 4, 4, false},
		{0, 3, 4, false},
		{5, 10, 4, false},
	}
	for _, c := range cases {
		got := CanScaleDownIngestion(int64(c.pending), c.cur, c.min)
		if got != c.want {
			t.Errorf("pending=%d cur=%d min=%d: got %v want %v", c.pending, c.cur, c.min, got, c.want)
		}
	}
}

func TestCanScaleDownIndexing(t *testing.T) {
	if !CanScaleDownIndexing(0, 5, 4) {
		t.Fatal("expected true")
	}
	if CanScaleDownIndexing(1, 5, 4) {
		t.Fatal("expected false with pending")
	}
}

func FuzzCanScaleDownIngestion(f *testing.F) {
	f.Add(int64(0), 5, 4)
	f.Add(int64(10), 2, 4)
	f.Fuzz(func(t *testing.T, pending int64, cur, min int) {
		if pending < 0 {
			pending = -pending
		}
		if cur < 0 {
			cur = -cur
		}
		if min < 0 {
			min = -min
		}
		got := CanScaleDownIngestion(pending, cur, min)
		// Invariants
		if pending > 0 && got {
			t.Fatal("must not scale down with pending work")
		}
		if cur <= min && got {
			t.Fatal("must not scale below minimum")
		}
	})
}
