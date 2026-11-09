package memory

import (
	"encoding/binary"
	"testing"
)

// FuzzSlabAllocatorAlloc verifies the slab allocator's allocation math:
// every Alloc returns a chunk of exactly the requested size, total accounted
// bytes equal the sum of requests, distinct allocations never overlap
// (writing one chunk cannot corrupt another), and Reset releases the tally.
func FuzzSlabAllocatorAlloc(f *testing.F) {
	f.Add([]byte{10, 0, 0, 0, 20, 0, 0, 0, 30, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0xff, 0xff, 0x0f, 0x00, 1, 0, 0, 0}) // 1048575 % 200001, then 1
	f.Add(make([]byte, 24))                           // three zero-size allocations

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}

		// Derive at most 6 sizes of at most 200001 bytes each:
		// total allocation stays at ~1.2MB, well below runaway territory.
		const maxSize = 200_001
		sizes := make([]int, 0, 6)
		for i := 0; i+4 <= len(data) && len(sizes) < 6; i += 4 {
			n := int(binary.LittleEndian.Uint32(data[i:i+4]) % maxSize)
			sizes = append(sizes, n)
		}

		alloc := NewSlabAllocator()

		chunks := make([][]byte, 0, len(sizes))
		var wantTotal int64
		for i, n := range sizes {
			p := alloc.Alloc(n)
			if len(p) != n {
				t.Fatalf("Alloc(%d) returned %d bytes", n, len(p))
			}
			wantTotal += int64(n)
			if n > 0 {
				// Tag the chunk with a per-index byte; overlap would corrupt it.
				for j := range p {
					p[j] = byte(i + 1)
				}
			}
			chunks = append(chunks, p)
		}

		if got := alloc.TotalAllocated(); got != wantTotal {
			t.Fatalf("TotalAllocated() = %d, want %d", got, wantTotal)
		}

		// No overlap: every chunk must still hold its own tag.
		for i, p := range chunks {
			for _, v := range p {
				if v != byte(i+1) {
					t.Fatalf("chunk %d was corrupted (value %d): overlapping allocations", i, v)
				}
			}
		}

		alloc.Reset()
		if got := alloc.TotalAllocated(); got != 0 {
			t.Fatalf("TotalAllocated() after Reset = %d, want 0", got)
		}
	})
}
