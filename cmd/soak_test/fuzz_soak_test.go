package main

import "testing"

func FuzzBuildSoakSchema(f *testing.F) {
	f.Add(int32(128))
	f.Add(int32(1))
	f.Fuzz(func(t *testing.T, dim int32) {
		if dim < 1 {
			dim = 1
		}
		if dim > 256 {
			dim = 256
		}
		sch := buildSoakSchema(dim)
		if sch == nil {
			t.Fatal("nil schema")
		}
	})
}

func FuzzGenerateCompoundFilter(f *testing.F) {
	f.Add(0)
	f.Fuzz(func(t *testing.T, _ int) {
		_ = generateCompoundFilter()
		_ = generateDateFilter()
	})
}
