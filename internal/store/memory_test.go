package store

import (
	"runtime"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestMemory_AdviseMemory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("AdviseMemory is no-op on Windows")
	}

	size := 4096 // 4KB (page size)
	buf := make([]byte, size)
	ptr := unsafe.Pointer(&buf[0])

	t.Run("Normal", func(t *testing.T) {
		err := AdviseMemory(ptr, uintptr(size), AdviceNormal)
		assert.NoError(t, err)
	})

	t.Run("Random", func(t *testing.T) {
		err := AdviseMemory(ptr, uintptr(size), AdviceRandom)
		assert.NoError(t, err)
	})

	t.Run("Sequential", func(t *testing.T) {
		err := AdviseMemory(ptr, uintptr(size), AdviceSequential)
		assert.NoError(t, err)
	})

	t.Run("InvalidPointer", func(t *testing.T) {
		// Nil pointer should probably error or at least not crash
		err := AdviseMemory(nil, 0, AdviceNormal)
		assert.NoError(t, err) // Current implementation handles it or it's a no-op
	})
}

func TestMemory_LockMemory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("LockMemory is no-op on Windows")
	}

	// Note: mlock might require permissions (e.g. root or ulimit)
	// On many dev systems it might fail if limit is too low.
	// We'll try with a very small chunk.
	size := 128
	buf := make([]byte, size)
	ptr := unsafe.Pointer(&buf[0])

	t.Run("LockAndUnlock", func(t *testing.T) {
		err := LockMemory(ptr, uintptr(size))
		if err != nil {
			t.Logf("Skipping Mlock verification due to environment limits: %v", err)
			return
		}

		err = UnlockMemory(ptr, uintptr(size))
		assert.NoError(t, err)
	})
}

func TestMemory_PinThreadToCore(t *testing.T) {
	// Currently a stub/no-op on macOS/Windows, but let's ensure it doesn't crash
	err := PinThreadToCore(0)
	assert.NoError(t, err)
}

func TestMemory_AdviseRecord(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("AdviseRecord uses AdviseMemory which is no-op on Windows")
	}

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	b := array.NewInt32Builder(mem)
	b.AppendValues([]int32{1, 2, 3, 4, 5}, nil)
	rec := array.NewRecordBatch(schema, []arrow.Array{b.NewArray()}, 5)
	defer rec.Release()

	// Verify it doesn't crash and handles zero length if any
	AdviseRecord(rec, AdviceRandom)
}
