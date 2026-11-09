package safe

import (
	"math"
	"testing"
)

// FuzzUint32ToInt32 checks the overflow boundary of Uint32ToInt32: conversion
// succeeds exactly for values that fit int32 and round-trips through int64,
// and fails with a zero result for anything above math.MaxInt32. The
// unchecked companion Uint32ToInt must agree on in-range values.
func FuzzUint32ToInt32(f *testing.F) {
	// Valid seeds.
	f.Add(uint32(0))
	f.Add(uint32(1))
	f.Add(uint32(math.MaxInt32))
	// Invalid seeds: first overflowing value, 2^31, and the uint32 maximum.
	f.Add(uint32(math.MaxInt32) + 1)
	f.Add(uint32(0x80000000))
	f.Add(uint32(math.MaxUint32))

	f.Fuzz(func(t *testing.T, v uint32) {
		got, err := Uint32ToInt32(v)
		if v <= math.MaxInt32 {
			if err != nil {
				t.Fatalf("Uint32ToInt32(%d) failed: %v", v, err)
			}
			if int64(got) != int64(v) {
				t.Fatalf("Uint32ToInt32(%d) = %d, round trip lost precision", v, got)
			}
			// Uint32ToInt is the unchecked variant and must agree for
			// values that fit int32 on any platform.
			if unchecked := Uint32ToInt(v); int64(unchecked) != int64(v) {
				t.Fatalf("Uint32ToInt(%d) = %d, want %d", v, unchecked, v)
			}
		} else {
			if err == nil {
				t.Fatalf("Uint32ToInt32(%d) = %d, want overflow error", v, got)
			}
			if got != 0 {
				t.Fatalf("Uint32ToInt32(%d) = %d with error, want 0", v, got)
			}
		}
	})
}

// FuzzIntToInt32 checks the int32 range check of IntToInt32 using the actual
// platform int width, so the invariant holds on 32- and 64-bit systems alike.
func FuzzIntToInt32(f *testing.F) {
	// Valid seeds.
	f.Add(int64(0))
	f.Add(int64(math.MaxInt32))
	f.Add(int64(math.MinInt32))
	// Invalid seeds: one past each int32 boundary.
	f.Add(int64(math.MaxInt32) + 1)
	f.Add(int64(math.MinInt32) - 1)

	f.Fuzz(func(t *testing.T, v int64) {
		iv := int(v) // platform int width
		got, err := IntToInt32(iv)
		if iv > math.MaxInt32 || iv < math.MinInt32 {
			if err == nil {
				t.Fatalf("IntToInt32(%d) = %d, want range error", iv, got)
			}
			if got != 0 {
				t.Fatalf("IntToInt32(%d) = %d with error, want 0", iv, got)
			}
			return
		}
		if err != nil {
			t.Fatalf("IntToInt32(%d) failed: %v", iv, err)
		}
		if int64(got) != int64(iv) {
			t.Fatalf("IntToInt32(%d) = %d, round trip lost precision", iv, got)
		}
	})
}

// FuzzInt64ToUint32 checks the uint32 range check of Int64ToUint32: values
// outside [0, math.MaxUint32] must be rejected with a zero result, in-range
// values must round-trip exactly.
func FuzzInt64ToUint32(f *testing.F) {
	// Valid seeds.
	f.Add(int64(0))
	f.Add(int64(math.MaxUint32))
	f.Add(int64(math.MaxInt32))
	// Invalid seeds: negative, one past uint32, and extremes.
	f.Add(int64(-1))
	f.Add(int64(math.MaxUint32) + 1)
	f.Add(int64(math.MaxInt64))
	f.Add(int64(math.MinInt64))

	f.Fuzz(func(t *testing.T, v int64) {
		got, err := Int64ToUint32(v)
		if v < 0 || v > math.MaxUint32 {
			if err == nil {
				t.Fatalf("Int64ToUint32(%d) = %d, want range error", v, got)
			}
			if got != 0 {
				t.Fatalf("Int64ToUint32(%d) = %d with error, want 0", v, got)
			}
			return
		}
		if err != nil {
			t.Fatalf("Int64ToUint32(%d) failed: %v", v, err)
		}
		if uint64(got) != uint64(v) {
			t.Fatalf("Int64ToUint32(%d) = %d, round trip lost precision", v, got)
		}
	})
}
