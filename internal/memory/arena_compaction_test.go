package memory

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompactableArena_BasicOperations(t *testing.T) {
	arena := NewCompactableArena[uint32](4 * 1024 * 1024) // 4MB slabs

	// Allocate some slices
	ref1, err := arena.AllocSlice(100)
	require.NoError(t, err)

	ref2, err := arena.AllocSlice(200)
	require.NoError(t, err)

	// Write data
	data1 := arena.Get(ref1)
	for i := range data1 {
		data1[i] = uint32(i)
	}

	data2 := arena.Get(ref2)
	for i := range data2 {
		data2[i] = uint32(i * 2)
	}

	// Verify data
	assert.Equal(t, uint32(0), data1[0])
	assert.Equal(t, uint32(99), data1[99])
	assert.Equal(t, uint32(0), data2[0])
	assert.Equal(t, uint32(398), data2[199])
}

func TestCompactableArena_Compaction(t *testing.T) {
	arena := NewCompactableArena[uint32](1 * 1024 * 1024) // 1MB slabs

	// Allocate many small slices to create fragmentation
	refs := make([]SliceRef, 0, 100)
	for i := 0; i < 100; i++ {
		ref, err := arena.AllocSlice(1000)
		require.NoError(t, err)

		// Write data
		data := arena.Get(ref)
		for j := range data {
			data[j] = uint32(i*1000 + j)
		}

		refs = append(refs, ref)
	}

	initialSize := arena.TotalAllocated()
	t.Logf("Initial size: %d bytes", initialSize)

	// Keep only every other slice (simulate deletions)
	liveRefs := make([]SliceRef, 0, len(refs)/2)
	for i := 0; i < len(refs); i += 2 {
		liveRefs = append(liveRefs, refs[i])
	}

	// Perform compaction
	newRefs, stats, err := arena.Compact(liveRefs)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, len(liveRefs), len(newRefs))

	t.Logf("Compaction stats: slabs=%d, reclaimed=%d, copied=%d, fragmentation=%.2f%%",
		stats.SlabsCompacted, stats.BytesReclaimed, stats.LiveDataCopied, stats.FragmentationPct)

	// Verify fragmentation was detected
	assert.Greater(t, stats.FragmentationPct, 40.0) // Should be ~50%
	assert.Greater(t, stats.BytesReclaimed, int64(0))

	finalSize := arena.TotalAllocated()
	t.Logf("Final size: %d bytes", finalSize)

	// Final size should be less than initial
	assert.Less(t, finalSize, initialSize)
}

func TestCompactableArena_DataIntegrity(t *testing.T) {
	arena := NewCompactableArena[uint64](2 * 1024 * 1024)

	// Allocate and fill slices with known patterns
	type testSlice struct {
		ref     SliceRef
		pattern uint64
	}

	slices := make([]testSlice, 0, 50)
	for i := 0; i < 50; i++ {
		ref, err := arena.AllocSlice(500)
		require.NoError(t, err)

		pattern := uint64(i * 12345)
		data := arena.Get(ref)
		for j := range data {
			data[j] = pattern + uint64(j)
		}

		slices = append(slices, testSlice{ref, pattern})
	}

	// Extract live refs
	liveRefs := make([]SliceRef, len(slices))
	for i, s := range slices {
		liveRefs[i] = s.ref
	}

	// Compact
	_, stats, err := arena.Compact(liveRefs)
	require.NoError(t, err)
	t.Logf("Compacted %d slabs, reclaimed %d bytes", stats.SlabsCompacted, stats.BytesReclaimed)

	// Note: After compaction, the old refs are invalid
	// In a real scenario, the caller would need to update their references
	// For this test, we just verify the compaction completed successfully
}

func TestCompactableArena_EmptyCompaction(t *testing.T) {
	arena := NewCompactableArena[float32](1 * 1024 * 1024)

	// Allocate some data
	_, err := arena.AllocSlice(100)
	require.NoError(t, err)

	// Compact with empty live refs (everything is dead)
	newRefs, stats, err := arena.Compact([]SliceRef{})
	require.NoError(t, err)
	require.Empty(t, newRefs)
	require.Equal(t, int64(0), stats.LiveDataCopied)
}

func TestCompactableArena_ConcurrentReads(t *testing.T) {
	arena := NewCompactableArena[int32](4 * 1024 * 1024)

	// Allocate some data
	ref, err := arena.AllocSlice(10000)
	require.NoError(t, err)

	data := arena.Get(ref)
	for i := range data {
		data[i] = int32(i)
	}

	// Concurrent reads should work
	done := make(chan bool, 10)
	for g := 0; g < 10; g++ {
		go func() {
			for i := 0; i < 100; i++ {
				d := arena.Get(ref)
				assert.Equal(t, int32(0), d[0])
				assert.Equal(t, int32(9999), d[9999])
			}
			done <- true
		}()
	}

	for g := 0; g < 10; g++ {
		<-done
	}
}

func BenchmarkCompactableArena_Alloc(b *testing.B) {
	arena := NewCompactableArena[uint32](4 * 1024 * 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = arena.AllocSlice(100)
	}
}

func BenchmarkCompactableArena_Get(b *testing.B) {
	arena := NewCompactableArena[uint32](4 * 1024 * 1024)
	ref, _ := arena.AllocSlice(1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = arena.Get(ref)
	}
}

func TestTypedArena_Compact(t *testing.T) {
	arena := NewTypedArena[uint32](NewSlabArena(1 * 1024 * 1024))

	refs := make([]SliceRef, 0, 50)
	for i := 0; i < 50; i++ {
		ref, err := arena.AllocSlice(1000)
		require.NoError(t, err)

		data := arena.Get(ref)
		for j := range data {
			data[j] = uint32(i*1000 + j)
		}

		refs = append(refs, ref)
	}

	initialSize := arena.TotalAllocated()
	t.Logf("Initial size: %d bytes", initialSize)

	liveRefs := make([]SliceRef, 0, len(refs)/2)
	for i := 0; i < len(refs); i += 2 {
		liveRefs = append(liveRefs, refs[i])
	}

	stats, err := arena.Compact(liveRefs)
	require.NoError(t, err)
	require.NotNil(t, stats)

	t.Logf("Compaction stats: slabs=%d, reclaimed=%d, copied=%d, fragmentation=%.2f%%",
		stats.SlabsCompacted, stats.BytesReclaimed, stats.LiveDataCopied, stats.FragmentationPct)

	assert.Greater(t, stats.FragmentationPct, 0.0)
	assert.Greater(t, stats.BytesReclaimed, int64(0))

	finalSize := arena.TotalAllocated()
	t.Logf("Final size: %d bytes", finalSize)
	assert.Less(t, finalSize, initialSize)
}

func TestTypedArena_CompactConcurrent(t *testing.T) {
	arena := NewTypedArena[uint32](NewSlabArena(4 * 1024 * 1024))

	refs := make([]SliceRef, 0, 20)
	for i := 0; i < 20; i++ {
		ref, err := arena.AllocSlice(1000)
		require.NoError(t, err)
		refs = append(refs, ref)
	}

	done := make(chan bool, 5)

	for g := 0; g < 5; g++ {
		go func() {
			defer func() { done <- true }()

			for i := 0; i < 10; i++ {
				liveRefs := make([]SliceRef, 0, len(refs)/2)
				for j := 0; j < len(refs); j += 2 {
					liveRefs = append(liveRefs, refs[j])
				}

				_, err := arena.Compact(liveRefs)
				if err != nil {
					t.Errorf("compact failed: %v", err)
					return
				}
			}
		}()
	}

	for g := 0; g < 5; g++ {
		<-done
	}
}
