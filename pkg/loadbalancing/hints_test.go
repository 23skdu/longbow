package loadbalancing

import (
	"testing"
)

func TestLoadHints_Serialization(t *testing.T) {
	h := LoadHints{
		CPULoad:    45,
		MemLoad:    72,
		QueueDepth: 1234,
		Health:     100,
	}

	buf := make([]byte, LoadHintsSize)
	h.Serialize(buf)

	h2, ok := DeserializeLoadHints(buf)
	if !ok {
		t.Fatal("Failed to deserialize valid hints")
	}

	if h.CPULoad != h2.CPULoad {
		t.Errorf("CPULoad mismatch: %d != %d", h.CPULoad, h2.CPULoad)
	}
	if h.MemLoad != h2.MemLoad {
		t.Errorf("MemLoad mismatch: %d != %d", h.MemLoad, h2.MemLoad)
	}
	if h.QueueDepth != h2.QueueDepth {
		t.Errorf("QueueDepth mismatch: %d != %d", h.QueueDepth, h2.QueueDepth)
	}
	if h.Health != h2.Health {
		t.Errorf("Health mismatch: %d != %d", h.Health, h2.Health)
	}
}

func TestDeserialize_Malformed(t *testing.T) {
	// 1. Too short
	_, ok := DeserializeLoadHints([]byte{0x01, 0x02})
	if ok {
		t.Error("Expected failure for short buffer")
	}

	// 2. Wrong version
	buf := make([]byte, LoadHintsSize)
	buf[0] = 0x02 // Version 2 (not supported)
	_, ok = DeserializeLoadHints(buf)
	if ok {
		t.Error("Expected failure for wrong version")
	}
}

func TestLoadHints_ZeroValue(t *testing.T) {
	h := LoadHints{}
	buf := make([]byte, LoadHintsSize)
	h.Serialize(buf)
	
	h2, ok := DeserializeLoadHints(buf)
	if !ok {
		t.Fatal("Failed to deserialize zero hints")
	}
	if h2.CPULoad != 0 || h2.Health != 0 {
		t.Error("Expected zero values")
	}
}

func TestSerialize_ShortBuffer(t *testing.T) {
	h := LoadHints{CPULoad: 10}
	buf := make([]byte, 5) // Too short
	h.Serialize(buf)
	// Should return early without panic
}

func FuzzDeserialize(f *testing.F) {
	f.Add([]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DeserializeLoadHints(data)
	})
}
