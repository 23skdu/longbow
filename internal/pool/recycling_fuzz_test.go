package pool

import (
	"encoding/binary"
	"testing"
)

// FuzzBytePoolRecycle verifies the recycling contract of BytePool: every Get
// returns an empty buffer, and contents written by one owner never leak to the
// next owner after Put/Get cycles.
func FuzzBytePoolRecycle(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte("wal-record-payload"))
	f.Add([]byte{0x00, 0xff, 0xaa, 0x55})
	f.Add([]byte("AAAAAAAAAA"))
	f.Add(make([]byte, 4096))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}

		pool := NewBytePool()

		buf := pool.Get()
		if buf.Len() != 0 {
			t.Fatalf("fresh pool buffer must be empty, got %d bytes", buf.Len())
		}
		if _, err := buf.Write(data); err != nil {
			t.Fatalf("write to pooled buffer: %v", err)
		}
		if got := buf.Len(); got != len(data) {
			t.Fatalf("buffer length after write = %d, want %d", got, len(data))
		}

		pool.Put(buf)

		recycled := pool.Get()
		if recycled.Len() != 0 {
			t.Fatalf("recycled buffer leaked %d bytes from previous owner", recycled.Len())
		}

		// Second cycle must behave identically (deterministic reset behavior).
		if _, err := recycled.Write(data); err != nil {
			t.Fatalf("write to recycled buffer: %v", err)
		}
		pool.Put(recycled)

		again := pool.Get()
		if again.Len() != 0 {
			t.Fatalf("buffer not reset on Put: %d bytes remain", again.Len())
		}
	})
}

// FuzzBitmapPoolRecycleClearsContents verifies that the global bitmap pool
// never leaks bitmap contents across Get/Put cycles and that putting a nil
// bitmap is safe.
func FuzzBitmapPoolRecycleClearsContents(f *testing.F) {
	f.Add([]byte{1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	f.Add(make([]byte, 64))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}

		bm := GetBitmap()
		if !bm.IsEmpty() {
			t.Fatalf("GetBitmap returned a non-empty bitmap with %d values", bm.GetCardinality())
		}

		seen := make(map[uint32]struct{}, len(data)/4+1)
		for i := 0; i+4 <= len(data); i += 4 {
			v := binary.LittleEndian.Uint32(data[i : i+4])
			seen[v] = struct{}{}
			bm.Add(v)
		}

		if got, want := bm.GetCardinality(), uint64(len(seen)); got != want {
			t.Fatalf("bitmap cardinality = %d, want %d distinct values", got, want)
		}

		PutBitmap(bm)

		recycled := GetBitmap()
		if !recycled.IsEmpty() {
			t.Fatalf("recycled bitmap leaked %d values from previous owner", recycled.GetCardinality())
		}

		// Putting nil must be a no-op, never a panic.
		PutBitmap(nil)
	})
}
