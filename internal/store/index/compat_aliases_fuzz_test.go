package index

import (
	"encoding/binary"
	"math"
	"testing"
)

// FuzzCompatDispatchVectorAny keeps a fuzz target in the compatibility facade
// after the implementation moved to hnsw. It verifies that the exported
// generic wrapper still dispatches typed vectors without changing values and
// ignores vectors of the wrong element type.
func FuzzCompatDispatchVectorAny(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0, 0, 0x80, 0x3f})
	f.Add([]byte{0, 0, 0xc0, 0x7f, 0, 0, 0x80, 0xbf})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 256 {
			data = data[:256]
		}
		data = data[:len(data)-len(data)%4]
		if len(data) == 0 {
			return
		}

		want := make([]float32, len(data)/4)
		for i := range want {
			want[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[i*4:]))
		}

		called := false
		err := DispatchVectorAny[float32](want, func(got []float32) error {
			called = true
			if len(got) != len(want) {
				t.Fatalf("dispatch length = %d, want %d", len(got), len(want))
			}
			for i := range got {
				if math.Float32bits(got[i]) != math.Float32bits(want[i]) {
					t.Fatalf("value %d changed: %x -> %x", i, math.Float32bits(want[i]), math.Float32bits(got[i]))
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("DispatchVectorAny returned error: %v", err)
		}
		if !called {
			t.Fatal("DispatchVectorAny did not call the typed callback")
		}

		called = false
		if err := DispatchVectorAny[float32]([]float64{1}, func([]float32) error {
			called = true
			return nil
		}); err != nil || called {
			t.Fatalf("wrong element type must be ignored: err=%v called=%v", err, called)
		}
	})
}
