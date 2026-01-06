package store


import (
	"sync"
	"testing"
)

// TestResultPoolExpanded_NewSizes tests that expanded pool sizes are available
func TestResultPoolExpanded_NewSizes(t *testing.T) {
	pool := newResultPool()

	tests := []struct {
		name string
		k    int
	}{
		{"k=10 (original)", 10},
		{"k=20 (reranking)", 20},
		{"k=50 (original)", 50},
		{"k=100 (original)", 100},
		{"k=256 (RAG)", 256},
		{"k=1000 (batch)", 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slice := pool.get(tt.k)
			if len(slice) != tt.k {
				t.Errorf("get(%d) returned slice of len %d, want %d", tt.k, len(slice), tt.k)
			}
			pool.put(slice)
		})
	}
}

// TestResultPoolExpanded_PoolReuse tests that expanded pools reuse allocations
func TestResultPoolExpanded_PoolReuse(t *testing.T) {
	pool := newResultPool()

	// Test k=20 (new pool for reranking workloads)
	t.Run("k=20 reuse", func(t *testing.T) {
		slice1 := pool.get(20)
		if cap(slice1) != 20 {
			t.Errorf("k=20 capacity = %d, want 20", cap(slice1))
		}
		pool.put(slice1)

		slice2 := pool.get(20)
		if cap(slice2) != 20 {
			t.Errorf("k=20 reused capacity = %d, want 20", cap(slice2))
		}
		pool.put(slice2)
	})

	// Test k=256 (new pool for RAG workloads)
	t.Run("k=256 reuse", func(t *testing.T) {
		slice1 := pool.get(256)
		if cap(slice1) != 256 {
			t.Errorf("k=256 capacity = %d, want 256", cap(slice1))
		}
		pool.put(slice1)

		slice2 := pool.get(256)
		if cap(slice2) != 256 {
			t.Errorf("k=256 reused capacity = %d, want 256", cap(slice2))
		}
		pool.put(slice2)
	})

	// Test k=1000 (new pool for batch processing)
	t.Run("k=1000 reuse", func(t *testing.T) {
		slice1 := pool.get(1000)
		if cap(slice1) != 1000 {
			t.Errorf("k=1000 capacity = %d, want 1000", cap(slice1))
		}
		pool.put(slice1)

		slice2 := pool.get(1000)
		if cap(slice2) != 1000 {
			t.Errorf("k=1000 reused capacity = %d, want 1000", cap(slice2))
		}
		pool.put(slice2)
	})
}

// TestResultPoolExpanded_ResliceToSmaller tests reslicing to smaller sizes uses appropriate pool
func TestResultPoolExpanded_ResliceToSmaller(t *testing.T) {
	pool := newResultPool()

	tests := []struct {
		name        string
		k           int
		expectedCap int
	}{
		{"k=15 uses pool20", 15, 20},
		{"k=18 uses pool20", 18, 20},
		{"k=150 uses pool256", 150, 256},
		{"k=200 uses pool256", 200, 256},
		{"k=500 uses pool512", 500, 512},
		{"k=750 uses pool1000", 750, 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slice := pool.get(tt.k)
			if len(slice) != tt.k {
				t.Errorf("get(%d) len = %d, want %d", tt.k, len(slice), tt.k)
			}
			if cap(slice) != tt.expectedCap {
				t.Errorf("get(%d) cap = %d, want %d", tt.k, cap(slice), tt.expectedCap)
			}
			pool.put(slice)
		})
	}
}

// TestResultPoolExpanded_LargerThanMax tests k > 1000 allocates new slice
func TestResultPoolExpanded_LargerThanMax(t *testing.T) {
	pool := newResultPool()

	// k > 1000 should allocate new slice, not use pool
	slice := pool.get(2000)
	if len(slice) != 2000 {
		t.Errorf("get(2000) len = %d, want 2000", len(slice))
	}
	if cap(slice) != 2000 {
		t.Errorf("get(2000) cap = %d, want 2000", cap(slice))
	}
	// put should not panic for non-pooled sizes
	pool.put(slice)
}

// TestResultPoolExpanded_ConcurrentAccess tests thread safety of expanded pools
func TestResultPoolExpanded_ConcurrentAccess(t *testing.T) {
	pool := newResultPool()
	var wg sync.WaitGroup

	kValues := []int{10, 20, 50, 100, 256, 1000}

	for _, k := range kValues {
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(size int) {
				defer wg.Done()
				slice := pool.get(size)
				// Simulate work
				for j := range slice {
					slice[j] = VectorID(j)
				}
				pool.put(slice)
			}(k)
		}
	}

	wg.Wait()
}

// TestResultPoolExpanded_ZeroedOnGet tests slices are zeroed on get
func TestResultPoolExpanded_ZeroedOnGet(t *testing.T) {
	pool := newResultPool()

	// Test new pools are zeroed
	for _, k := range []int{20, 256, 1000} {
		t.Run("zeroed k="+string(rune(k)), func(t *testing.T) {
			slice := pool.get(k)
			// Fill with data
			for i := range slice {
				slice[i] = VectorID(i + 1)
			}
			pool.put(slice)

			// Get again - should be zeroed
			slice2 := pool.get(k)
			for i := range slice2 {
				if slice2[i] != 0 {
					t.Errorf("slice[%d] = %d, want 0 (not zeroed)", i, slice2[i])
					break
				}
			}
			pool.put(slice2)
		})
	}
}

// TestResultPoolExpanded_PutNil tests put handles nil safely
func TestResultPoolExpanded_PutNil(t *testing.T) {
	pool := newResultPool()
	// Should not panic
	pool.put(nil)
}

// TestResultPoolExpanded_AllPoolSizesExist verifies struct has all expected pools
func TestResultPoolExpanded_AllPoolSizesExist(t *testing.T) {
	pool := newResultPool()
	if pool == nil {
		t.Fatal("newResultPool returned nil")
	}

	// Verify we can get exact sizes for all pools
	sizes := []int{10, 20, 50, 100, 256, 1000}
	for _, size := range sizes {
		slice := pool.get(size)
		if len(slice) != size || cap(slice) != size {
			t.Errorf("Pool for k=%d: len=%d cap=%d, want both=%d", size, len(slice), cap(slice), size)
		}
		pool.put(slice)
	}
}
