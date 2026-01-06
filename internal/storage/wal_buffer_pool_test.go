package storage

import (
	"bytes"
	"sync"
	"testing"
)

// TestWALBufferPool_BasicGetPut tests basic pool operations
func TestWALBufferPool_BasicGetPut(t *testing.T) {
	pool := newWALBufferPool()

	// Get a buffer
	buf := pool.Get()
	if buf == nil {
		t.Fatal("expected non-nil buffer")
	}

	// Write some data
	buf.WriteString("test data")
	if buf.Len() != 9 {
		t.Errorf("expected buffer len 9, got %d", buf.Len())
	}

	// Put back (should reset)
	pool.Put(buf)

	// Get again - should be reset
	buf2 := pool.Get()
	if buf2.Len() != 0 {
		t.Errorf("expected buffer to be reset, got len %d", buf2.Len())
	}
}

// TestWALBufferPool_Reuse verifies buffer reuse reduces allocations
func TestWALBufferPool_Reuse(t *testing.T) {
	pool := newWALBufferPool()

	// Get and return same buffer multiple times
	for i := 0; i < 100; i++ {
		buf := pool.Get()
		buf.WriteString("some test data that takes up space")
		pool.Put(buf)
	}

	// Final get should have capacity from previous use
	buf := pool.Get()
	if buf.Cap() == 0 {
		t.Log("buffer capacity not preserved (may be new alloc)")
	}
	// Main test is that this doesn't crash and buffers are properly reset
	if buf.Len() != 0 {
		t.Errorf("expected empty buffer, got len %d", buf.Len())
	}
}

// TestWALBufferPool_Concurrent tests thread safety
func TestWALBufferPool_Concurrent(t *testing.T) {
	pool := newWALBufferPool()
	var wg sync.WaitGroup

	workers := 20
	iterations := 1000

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				buf := pool.Get()
				buf.WriteString("concurrent test data")
				// Verify buffer is usable
				if buf.Len() == 0 {
					t.Errorf("worker %d: buffer unexpectedly empty after write", id)
				}
				pool.Put(buf)
			}
		}(w)
	}

	wg.Wait()
}

// TestWALBufferPool_LargeBuffer tests handling of large buffers
func TestWALBufferPool_LargeBuffer(t *testing.T) {
	pool := newWALBufferPool()

	// Get buffer and grow it large
	buf := pool.Get()
	largeData := make([]byte, 1<<20) // 1MB
	buf.Write(largeData)

	if buf.Len() != 1<<20 {
		t.Errorf("expected 1MB buffer, got %d", buf.Len())
	}

	// Return to pool
	pool.Put(buf)

	// Get again - should be reset but may have capacity
	buf2 := pool.Get()
	if buf2.Len() != 0 {
		t.Errorf("expected reset buffer, got len %d", buf2.Len())
	}
}

// BenchmarkWALBufferPool_NoPool baseline without pooling
func BenchmarkWALBufferPool_NoPool(b *testing.B) {
	data := []byte("test record data for WAL entry simulation")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf := new(bytes.Buffer)
		buf.Write(data)
		_ = buf.Bytes()
		// buf goes to GC
	}
}

// BenchmarkWALBufferPool_WithPool measures pooled allocation
func BenchmarkWALBufferPool_WithPool(b *testing.B) {
	pool := newWALBufferPool()
	data := []byte("test record data for WAL entry simulation")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf := pool.Get()
		buf.Write(data)
		_ = buf.Bytes()
		pool.Put(buf)
	}
}

// BenchmarkWALBufferPool_Parallel measures parallel pooled performance
func BenchmarkWALBufferPool_Parallel(b *testing.B) {
	pool := newWALBufferPool()
	data := []byte("test record data for WAL entry simulation")

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.Get()
			buf.Write(data)
			_ = buf.Bytes()
			pool.Put(buf)
		}
	})
}
