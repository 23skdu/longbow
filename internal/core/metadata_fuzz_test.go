package core

import (
	"bytes"
	"math"
	"reflect"
	"testing"
)

// buildFuzzMetadata deterministically turns fuzz bytes into a metadata map
// containing only types EncodeMetadata/DecodeMetadata round-trip exactly.
// Keys are <= 15 bytes (uint16 length field) and entries are bounded to 16.
func buildFuzzMetadata(data []byte) map[string]interface{} {
	md := make(map[string]interface{})
	rest := data
	need := func(n int) []byte {
		if len(rest) < n {
			b := rest
			rest = nil
			return b
		}
		b := rest[:n]
		rest = rest[n:]
		return b
	}
	u64 := func(b []byte) uint64 {
		var v uint64
		for i := 0; i < len(b); i++ {
			v |= uint64(b[i]) << (8 * i)
		}
		return v
	}

	for len(rest) >= 3 && len(md) < 16 {
		ctrl := need(1)[0]
		key := string(need(int(ctrl % 16)))
		switch ctrl >> 4 {
		case 0:
			md[key] = nil
		case 1:
			md[key] = string(need(24))
		case 2:
			md[key] = int64(u64(need(8)))
		case 3:
			v := math.Float64frombits(u64(need(8)))
			if math.IsNaN(v) {
				v = 0
			}
			md[key] = v
		case 4:
			md[key] = ctrl&1 == 1
		default:
			md[key] = append([]byte{}, need(32)...)
		}
	}
	return md
}

// FuzzMetadataRoundTrip asserts that every metadata map built from fuzz
// input survives encode -> decode unchanged.
func FuzzMetadataRoundTrip(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x11, 1, 'a', 5, 'h', 'e', 'l', 'l', 'o'})
	f.Add([]byte{0x21, 2, 'i', 'd', 8, 0, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{0x31, 1, 'f', 0, 0, 0, 0, 0, 0, 0xf8, 0x7f}) // NaN float
	f.Add([]byte{0x41, 1, 'b', 1})                            // bool true
	f.Add([]byte{0x51, 3, 'r', 'a', 'w', 4, 9, 9, 9})         // binary
	f.Add(make([]byte, 100))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}

		md := buildFuzzMetadata(data)
		enc, err := EncodeMetadata(md)
		if err != nil {
			t.Fatalf("EncodeMetadata failed: %v", err)
		}
		dec, err := DecodeMetadata(enc)
		if err != nil {
			t.Fatalf("DecodeMetadata failed: %v", err)
		}
		if !reflect.DeepEqual(dec, md) {
			t.Fatalf("round trip mismatch:\n got: %#v\nwant: %#v", dec, md)
		}

		// A second encode/decode cycle must produce the same map again.
		enc2, err := EncodeMetadata(md)
		if err != nil {
			t.Fatalf("second EncodeMetadata failed: %v", err)
		}
		dec2, err := DecodeMetadata(enc2)
		if err != nil {
			t.Fatalf("second DecodeMetadata failed: %v", err)
		}
		if !reflect.DeepEqual(dec2, md) {
			t.Fatalf("second round trip mismatch:\n got: %#v\nwant: %#v", dec2, md)
		}
	})
}

// metaMapsEqual compares decoded metadata maps with bitwise equality for
// floats (reflect.DeepEqual treats NaN != NaN).
func metaMapsEqual(a, b map[string]interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok {
			return false
		}
		switch x := av.(type) {
		case nil:
			if bv != nil {
				return false
			}
		case string:
			y, ok := bv.(string)
			if !ok || x != y {
				return false
			}
		case int64:
			y, ok := bv.(int64)
			if !ok || x != y {
				return false
			}
		case float64:
			y, ok := bv.(float64)
			if !ok || math.Float64bits(x) != math.Float64bits(y) {
				return false
			}
		case bool:
			y, ok := bv.(bool)
			if !ok || x != y {
				return false
			}
		case []byte:
			y, ok := bv.([]byte)
			if !ok || !bytes.Equal(x, y) {
				return false
			}
		default:
			if !reflect.DeepEqual(av, bv) {
				return false
			}
		}
	}
	return true
}

// FuzzDecodeMetadataIdempotent feeds arbitrary bytes to DecodeMetadata:
// it must never panic, must not leak a partial map on error, and any
// successfully decoded map must be a fixed point of encode -> decode.
func FuzzDecodeMetadataIdempotent(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{1, 0, 0, 0})
	f.Add([]byte{2, 1, 0, 0, 0, 1, 'a', 5, 0, 0, 0, 0, 2, 8, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8})

	valid, err := EncodeMetadata(map[string]interface{}{
		"name":  "longbow",
		"count": int64(7),
		"ratio": 0.5,
		"ok":    true,
		"raw":   []byte{1, 2, 3},
		"none":  nil,
	})
	if err != nil {
		f.Fatalf("seed encode failed: %v", err)
	}
	f.Add(valid)
	truncated := append([]byte{}, valid...)
	f.Add(truncated[:len(truncated)/2])

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}

		m1, err := DecodeMetadata(data)
		if err != nil {
			if m1 != nil {
				t.Fatalf("decode returned a partial map together with error: %#v", m1)
			}
			return
		}

		// Re-encoding a decoded map must yield the same map when decoded
		// again (canonical fixed point).
		enc, err := EncodeMetadata(m1)
		if err != nil {
			t.Fatalf("re-encode failed: %v", err)
		}
		m2, err := DecodeMetadata(enc)
		if err != nil {
			t.Fatalf("re-decode failed: %v", err)
		}
		if !metaMapsEqual(m1, m2) {
			t.Fatalf("decode not idempotent:\n first: %#v\nsecond: %#v", m1, m2)
		}
	})
}
