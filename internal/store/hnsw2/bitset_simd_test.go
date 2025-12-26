package hnsw2

import (
	"testing"
)

func TestBitset_ClearSIMD(t *testing.T) {
	bs := NewBitset(1000)
	
	// Set some bits
	for i := uint32(0); i < 1000; i += 7 {
		bs.Set(i)
	}
	
	// Clear using SIMD
	bs.ClearSIMD()
	
	// All should be unset
	for i := uint32(0); i < 1000; i++ {
		if bs.IsSet(i) {
			t.Errorf("bit %d should be unset after ClearSIMD()", i)
		}
	}
}

func TestBitset_AndNot(t *testing.T) {
	a := NewBitset(100)
	b := NewBitset(100)
	
	// Set bits in a: 0, 10, 20, 30, 40, 50
	for i := uint32(0); i < 100; i += 10 {
		a.Set(i)
	}
	
	// Set bits in b: 10, 30, 50, 70, 90
	for i := uint32(10); i < 100; i += 20 {
		b.Set(i)
	}
	
	// result = a & ~b should have: 0, 20, 40, 60, 80
	result := a.AndNot(b)
	
	expected := map[uint32]bool{0: true, 20: true, 40: true, 60: true, 80: true}
	for i := uint32(0); i < 100; i++ {
		if expected[i] {
			if !result.IsSet(i) {
				t.Errorf("bit %d should be set in AndNot result", i)
			}
		} else {
			if result.IsSet(i) {
				t.Errorf("bit %d should be unset in AndNot result", i)
			}
		}
	}
}

func TestBitset_PopCount(t *testing.T) {
	bs := NewBitset(1000)
	
	// Set every 3rd bit
	expectedCount := 0
	for i := uint32(0); i < 1000; i += 3 {
		bs.Set(i)
		expectedCount++
	}
	
	count := bs.PopCount()
	if count != expectedCount {
		t.Errorf("PopCount() = %d, want %d", count, expectedCount)
	}
}

func TestBitset_PopCount_Empty(t *testing.T) {
	bs := NewBitset(100)
	count := bs.PopCount()
	if count != 0 {
		t.Errorf("PopCount() on empty bitset = %d, want 0", count)
	}
}

func TestBitset_PopCount_Full(t *testing.T) {
	bs := NewBitset(128)
	
	// Set all bits
	for i := uint32(0); i < 128; i++ {
		bs.Set(i)
	}
	
	count := bs.PopCount()
	if count != 128 {
		t.Errorf("PopCount() on full bitset = %d, want 128", count)
	}
}

func TestBitset_AndNot_DifferentSizes(t *testing.T) {
	a := NewBitset(100)
	b := NewBitset(50)
	
	// Set all bits in a
	for i := uint32(0); i < 100; i++ {
		a.Set(i)
	}
	
	// Set all bits in b
	for i := uint32(0); i < 50; i++ {
		b.Set(i)
	}
	
	result := a.AndNot(b)
	
	// Result should have size 50 (min of a and b)
	if result.Size() != 50 {
		t.Errorf("AndNot result size = %d, want 50", result.Size())
	}
	
	// All bits should be unset (a & ~b where both are all 1s)
	for i := uint32(0); i < 50; i++ {
		if result.IsSet(i) {
			t.Errorf("bit %d should be unset", i)
		}
	}
}

// Benchmark SIMD vs scalar Clear
func BenchmarkBitset_Clear(b *testing.B) {
	bs := NewBitset(100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bs.Clear()
	}
}

func BenchmarkBitset_ClearSIMD(b *testing.B) {
	bs := NewBitset(100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bs.ClearSIMD()
	}
}

func BenchmarkBitset_AndNot(b *testing.B) {
	a := NewBitset(100000)
	other := NewBitset(100000)
	
	// Set half the bits
	for i := uint32(0); i < 100000; i += 2 {
		a.Set(i)
		other.Set(i + 1)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = a.AndNot(other)
	}
}

func BenchmarkBitset_PopCount(b *testing.B) {
	bs := NewBitset(100000)
	
	// Set half the bits
	for i := uint32(0); i < 100000; i += 2 {
		bs.Set(i)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bs.PopCount()
	}
}
