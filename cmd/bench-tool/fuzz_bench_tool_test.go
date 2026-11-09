package main

import "testing"

func FuzzGenerateRecord(f *testing.F) {
	f.Add(2, 4, "float32", 4)
	f.Add(1, 1, "bogus", 0)
	f.Fuzz(func(t *testing.T, count, dim int, dtype string, tqBits int) {
		if count < 1 || count > 5 {
			return
		}
		if dim < 1 || dim > 32 {
			return
		}
		if tqBits < 0 || tqBits > 8 {
			return
		}
		rec, _, err := generateRecord(count, dim, dtype, tqBits)
		if err != nil {
			return
		}
		if rec != nil {
			rec.Release()
		}
	})
}
