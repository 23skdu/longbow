package core

import (
	"testing"
)

func TestPackLocation(t *testing.T) {
	tests := []struct {
		name string
		loc  Location
	}{
		{"zero", Location{BatchIdx: 0, RowIdx: 0}},
		{"simple", Location{BatchIdx: 1, RowIdx: 5}},
		{"negative batch", Location{BatchIdx: -1, RowIdx: 0}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			packed := PackLocation(tc.loc)
			unpacked := UnpackLocation(packed)
			if unpacked.BatchIdx != tc.loc.BatchIdx || unpacked.RowIdx != tc.loc.RowIdx {
				t.Errorf("expected %v, got %v", tc.loc, unpacked)
			}
		})
	}
}

func TestUnpackLocation(t *testing.T) {
	// Test roundtrip
	original := Location{BatchIdx: 42, RowIdx: 123}
	packed := PackLocation(original)
	unpacked := UnpackLocation(packed)

	if unpacked.BatchIdx != original.BatchIdx {
		t.Errorf("batch idx mismatch: expected %d, got %d", original.BatchIdx, unpacked.BatchIdx)
	}
	if unpacked.RowIdx != original.RowIdx {
		t.Errorf("row idx mismatch: expected %d, got %d", original.RowIdx, unpacked.RowIdx)
	}
}

func TestPackBytesToFloat32s(t *testing.T) {
	// Test with 4 bytes (1 float)
	input := []byte{0x00, 0x00, 0x80, 0x3f} // 1.0 in little-endian
	result := PackBytesToFloat32s(input)

	if len(result) != 1 {
		t.Errorf("expected 1 float, got %d", len(result))
	}
	if result[0] != 1.0 {
		t.Errorf("expected 1.0, got %f", result[0])
	}
}

func TestPackBytesToFloat32s_Padding(t *testing.T) {
	// Test with 3 bytes (should pad to 4)
	input := []byte{0x00, 0x00, 0x80} // partial
	result := PackBytesToFloat32s(input)

	if len(result) != 1 {
		t.Errorf("expected 1 float, got %d", len(result))
	}
}

func TestPackBytesToFloat32s_Multiple(t *testing.T) {
	// Test with 8 bytes (2 floats)
	input := []byte{0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00, 0x40} // 1.0, 2.0
	result := PackBytesToFloat32s(input)

	if len(result) != 2 {
		t.Errorf("expected 2 floats, got %d", len(result))
	}
	if result[0] != 1.0 {
		t.Errorf("expected 1.0, got %f", result[0])
	}
	if result[1] != 2.0 {
		t.Errorf("expected 2.0, got %f", result[1])
	}
}

func TestVectorID(t *testing.T) {
	var id VectorID = 42
	if uint32(id) != 42 {
		t.Errorf("expected 42, got %d", uint32(id))
	}
}

func TestLocation(t *testing.T) {
	loc := Location{BatchIdx: 1, RowIdx: 2}
	if loc.BatchIdx != 1 {
		t.Errorf("expected BatchIdx 1, got %d", loc.BatchIdx)
	}
	if loc.RowIdx != 2 {
		t.Errorf("expected RowIdx 2, got %d", loc.RowIdx)
	}
}
