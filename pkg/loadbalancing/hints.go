package loadbalancing

import (
	"encoding/binary"
)

// LoadHints represents health and capacity information for client-side load balancing.
type LoadHints struct {
	CPULoad    uint32
	MemLoad    uint32
	QueueDepth int64
	Health     uint32
}

const LoadHintsSize = 21 // Increased to 21 to accommodate version + full uint32/int64 fields

// Serialize encodes LoadHints into a byte slice.
func (h *LoadHints) Serialize(buf []byte) {
	if len(buf) < LoadHintsSize {
		return
	}

	buf[0] = 0x01 // Version

	binary.LittleEndian.PutUint32(buf[1:5], h.CPULoad)
	binary.LittleEndian.PutUint32(buf[5:9], h.MemLoad)
	binary.LittleEndian.PutUint64(buf[9:17], uint64(h.QueueDepth)) // #nosec G115
	binary.LittleEndian.PutUint32(buf[17:21], h.Health)
}

// Deserialize decodes LoadHints from a byte slice.
func DeserializeLoadHints(buf []byte) (LoadHints, bool) {
	if len(buf) < LoadHintsSize || buf[0] != 0x01 {
		return LoadHints{}, false
	}

	return LoadHints{
		CPULoad:    binary.LittleEndian.Uint32(buf[1:5]),
		MemLoad:    binary.LittleEndian.Uint32(buf[5:9]),
		QueueDepth: int64(binary.LittleEndian.Uint64(buf[9:17])), // #nosec G115
		Health:     binary.LittleEndian.Uint32(buf[17:21]),
	}, true
}
