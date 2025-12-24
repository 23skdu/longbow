package hnsw2

import "testing"

func TestBitset_SetAndIsSet(t *testing.T) {
	bs := NewBitset(100)
	
	// Initially all bits should be unset
	for i := uint32(0); i < 100; i++ {
		if bs.IsSet(i) {
			t.Errorf("bit %d should be unset initially", i)
		}
	}
	
	// Set some bits
	bs.Set(0)
	bs.Set(5)
	bs.Set(63)
	bs.Set(64)
	bs.Set(99)
	
	// Check set bits
	if !bs.IsSet(0) {
		t.Error("bit 0 should be set")
	}
	if !bs.IsSet(5) {
		t.Error("bit 5 should be set")
	}
	if !bs.IsSet(63) {
		t.Error("bit 63 should be set")
	}
	if !bs.IsSet(64) {
		t.Error("bit 64 should be set")
	}
	if !bs.IsSet(99) {
		t.Error("bit 99 should be set")
	}
	
	// Check unset bits
	if bs.IsSet(1) {
		t.Error("bit 1 should be unset")
	}
	if bs.IsSet(62) {
		t.Error("bit 62 should be unset")
	}
}

func TestBitset_Clear(t *testing.T) {
	bs := NewBitset(100)
	
	// Set some bits
	bs.Set(10)
	bs.Set(20)
	bs.Set(30)
	
	// Clear all
	bs.Clear()
	
	// All should be unset
	for i := uint32(0); i < 100; i++ {
		if bs.IsSet(i) {
			t.Errorf("bit %d should be unset after Clear()", i)
		}
	}
}

func TestBitset_OutOfBounds(t *testing.T) {
	bs := NewBitset(10)
	
	// Should not panic on out of bounds
	bs.Set(100)
	if bs.IsSet(100) {
		t.Error("out of bounds index should return false")
	}
}

func BenchmarkBitset_Set(b *testing.B) {
	bs := NewBitset(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bs.Set(uint32(i % 10000))
	}
}

func BenchmarkBitset_IsSet(b *testing.B) {
	bs := NewBitset(10000)
	for i := 0; i < 5000; i++ {
		bs.Set(uint32(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bs.IsSet(uint32(i % 10000))
	}
}
