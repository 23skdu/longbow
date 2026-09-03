//go:build gpu && linux

package cuda

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestCUDA_PinnedHostPool(t *testing.T) {
	pool := NewPinnedHostPool()
	defer pool.Close()

	if !IsAvailable() {
		t.Skip("CUDA device not available; skipping live hardware allocation test")
	}

	size := int64(4096)
	ptr1, err := pool.Get(size)
	assert.NoError(t, err)
	assert.NotNil(t, ptr1)

	// Write data to pinned buffer
	slice := (*[1024]float32)(ptr1)[:1024:1024]
	for i := range slice {
		slice[i] = float32(i) * 1.5
	}
	assert.Equal(t, float32(0.0), slice[0])
	assert.Equal(t, float32(1.5), slice[1])

	// Return to pool
	pool.Put(ptr1, size)

	// Borrow again - should reuse pooled buffer
	ptr2, err := pool.Get(size)
	assert.NoError(t, err)
	assert.Equal(t, ptr1, ptr2, "Should reuse pooled pinned host buffer")

	pool.Put(ptr2, size)
}

func FuzzCUDA_PinnedBufferPool(f *testing.F) {
	f.Add(int64(64))
	f.Add(int64(1024))
	f.Add(int64(65536))

	f.Fuzz(func(t *testing.T, size int64) {
		pool := NewPinnedHostPool()
		defer pool.Close()

		if size <= 0 {
			_, err := pool.Get(size)
			assert.Error(t, err)
			return
		}

		if !IsAvailable() {
			return
		}

		if size > 10*1024*1024 {
			size = 10 * 1024 * 1024
		}

		ptr, err := pool.Get(size)
		if err != nil {
			return
		}
		assert.NotNil(t, ptr)

		// Test zero-touch
		b := (*[1]byte)(unsafe.Pointer(ptr))
		b[0] = 0x42
		assert.Equal(t, byte(0x42), b[0])

		pool.Put(ptr, size)
	})
}
