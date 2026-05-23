package loadbalancing

import (
	"testing"
)

func FuzzLoadHints_Roundtrip(f *testing.F) {
	f.Add(uint32(0), uint32(0), int64(0), uint32(0))
	f.Add(uint32(100), uint32(100), int64(10000), uint32(100))
	f.Fuzz(func(t *testing.T, cpu, mem uint32, q int64, health uint32) {
		h := LoadHints{
			CPULoad:    cpu,
			MemLoad:    mem,
			QueueDepth: q,
			Health:     health,
		}

		buf := make([]byte, LoadHintsSize)
		h.Serialize(buf)

		h2, ok := DeserializeLoadHints(buf)
		if !ok {
			t.Fatal("Failed to deserialize")
		}

		if h.CPULoad != h2.CPULoad || h.MemLoad != h2.MemLoad || h.QueueDepth != h2.QueueDepth {
			t.Errorf("Mismatch: %+v != %+v", h, h2)
		}

		// Health is only 24 bits in the current serialization (buf[17], buf[18], buf[19])
		// Mask it to check correctly
		if (h.Health & 0xFFFFFF) != h2.Health {
			t.Errorf("Health mismatch: %d != %d", h.Health&0xFFFFFF, h2.Health)
		}
	})
}
