package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

// TestFlight_ZeroCopyAllocator_Baseline establishes a baseline for memory accounting.
// It verifies that standard allocators track usage, which is the prerequisite for
// implementing a custom hook-aware allocator for Zero-Copy.
func TestFlight_ZeroCopyAllocator_Baseline(t *testing.T) {
	// 1. Create a CheckedAllocator to track memory
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	// 2. Allocate a buffer simulating a network read
	sz := 1024
	buf := alloc.Allocate(sz)

	// Verify allocation
	assert.Equal(t, sz, alloc.CurrentAlloc(), "Allocator should track 1024 bytes")

	// 3. Simulate "Zero-Copy" handoff
	// In a real scenario, this buffer would be passed to the storage engine
	// without copying. We simulate retention by NOT releasing it immediately
	// and ensuring the allocator still reports it.

	// 4. Cleanup (Simulate lifecycle end)
	alloc.Free(buf)

	assert.Equal(t, 0, alloc.CurrentAlloc(), "Allocator should track 0 bytes after free")
}

// TestFlight_ZeroCopyAllocator_Integration verifies that ownership of Arrow buffers
// can be transferred from a "Network" layer to a "Store" layer without copying,
// utilizing Arrow's reference counting.
func TestFlight_ZeroCopyAllocator_Integration(t *testing.T) {
	// 1. Setup Allocator (Simulates Flight Server Allocator)
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	// We use a closure or simple logic to ensure clean state
	defer alloc.AssertSize(t, 0)

	// 2. Create a RecordBatch (Simulates incoming Flight Data)
	// We manually build it to control allocation
	bld := array.NewFloat32Builder(alloc)

	bld.AppendValues([]float32{1.1, 2.2, 3.3, 4.4, 5.5}, nil)
	arr := bld.NewArray() // Allocates memory
	bld.Release()         // Release builder resources immediately

	// Verify allocation happened
	initialMem := alloc.CurrentAlloc()
	assert.Greater(t, initialMem, 0, "Builder should have allocated memory")

	// Create RecordBatch
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "data", Type: arrow.PrimitiveTypes.Float32},
	}, nil)
	rec := array.NewRecordBatch(schema, []arrow.Array{arr}, 5)
	arr.Release() // RecordBatch retains the array, so we release our local handle

	// 3. Simulate "Store" Ingestion (Zero-Copy)
	// The store receives the record and Retains it for async processing
	storeRef := rec
	storeRef.Retain()

	// 4. Simulate "Network" Release
	// The request handler finishes and releases its reference
	rec.Release()

	// 5. Verify Memory is STILL held (Zero-Copy success)
	// If it was copied, alloc usage might have dropped (if copy was on different alloc)
	// or doubled (if confident copy). But here we expect exact same usage because
	// we share the underlying buffer.
	assert.Equal(t, initialMem, alloc.CurrentAlloc(), "Memory should still be held by Store reference")

	// 6. Simulate "Store" Release (Async Persistence/Index finished)
	storeRef.Release()

	// 7. Verify Memory Freed
	assert.Equal(t, 0, alloc.CurrentAlloc(), "All memory should be freed")
}
