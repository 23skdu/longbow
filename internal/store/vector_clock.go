package store

import (
	"bytes"
	"encoding/binary"
	"sort"
	"sync"

	"github.com/23skdu/longbow/internal/metrics"
)

// ClockComparison represents the result of comparing two vector clocks
type ClockComparison int

const (
	ClockEqual      ClockComparison = iota // Clocks are identical
	ClockBefore                            // First clock happened before second
	ClockAfter                             // First clock happened after second
	ClockConcurrent                        // Clocks are concurrent (conflict)
)

// VectorClock implements a vector clock for causal ordering in distributed systems
type VectorClock struct {
	mu     sync.RWMutex
	nodeID string
	clocks map[string]uint64
}

// NewVectorClock creates a new vector clock for the given node
func NewVectorClock(nodeID string) *VectorClock {
	return &VectorClock{
		nodeID: nodeID,
		clocks: make(map[string]uint64),
	}
}

// NodeID returns the node identifier for this clock
func (vc *VectorClock) NodeID() string {
	return vc.nodeID
}

// Get returns the clock value for a specific node
func (vc *VectorClock) Get(nodeID string) uint64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	return vc.clocks[nodeID]
}

// Set sets the clock value for a specific node
func (vc *VectorClock) Set(nodeID string, value uint64) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.clocks[nodeID] = value
}

// Increment increments this node's clock value
func (vc *VectorClock) Increment() {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.clocks[vc.nodeID]++
}

// Merge combines another vector clock into this one, taking max values
func (vc *VectorClock) Merge(other *VectorClock) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	for nodeID, otherVal := range other.clocks {
		if otherVal > vc.clocks[nodeID] {
			vc.clocks[nodeID] = otherVal
		}
	}

	metrics.VectorClockMergesTotal.Inc()
}

// Compare compares this vector clock with another
// Returns: ClockEqual, ClockBefore, ClockAfter, or ClockConcurrent
func (vc *VectorClock) Compare(other *VectorClock) ClockComparison {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	// Collect all node IDs
	allNodes := make(map[string]struct{})
	for k := range vc.clocks {
		allNodes[k] = struct{}{}
	}
	for k := range other.clocks {
		allNodes[k] = struct{}{}
	}

	var hasLess, hasGreater bool

	for nodeID := range allNodes {
		v1 := vc.clocks[nodeID]
		v2 := other.clocks[nodeID]

		if v1 < v2 {
			hasLess = true
		} else if v1 > v2 {
			hasGreater = true
		}
	}

	switch {
	case hasLess && hasGreater:
		metrics.VectorClockConflictsTotal.Inc()
		return ClockConcurrent
	case hasLess:
		return ClockBefore
	case hasGreater:
		return ClockAfter
	default:
		return ClockEqual
	}
}

// Copy creates a deep copy of the vector clock
func (vc *VectorClock) Copy() *VectorClock {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	clonedClock := &VectorClock{
		nodeID: vc.nodeID,
		clocks: make(map[string]uint64, len(vc.clocks)),
	}
	for k, v := range vc.clocks {
		clonedClock.clocks[k] = v
	}
	return clonedClock
}

// Serialize converts the vector clock to bytes for network transfer
func (vc *VectorClock) Serialize() []byte {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	var buf bytes.Buffer

	// Sort keys for deterministic output
	keys := make([]string, 0, len(vc.clocks))
	for k := range vc.clocks {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Write the number of entries
	_ = binary.Write(&buf, binary.LittleEndian, uint32(len(keys))) //nolint:gosec // G115 - length fits in uint32

	for _, k := range keys {
		// Write key length + key
		_ = binary.Write(&buf, binary.LittleEndian, uint32(len(k))) //nolint:gosec // G115 - key length fits in uint32
		buf.WriteString(k)

		// Write value
		_ = binary.Write(&buf, binary.LittleEndian, vc.clocks[k])
	}

	return buf.Bytes()
}

// Deserialize restores vector clock from bytes
func (vc *VectorClock) Deserialize(data []byte) error {

	buf := bytes.NewReader(data)

	var count uint32
	if err := binary.Read(buf, binary.LittleEndian, &count); err != nil {
		return err
	}

	vc.clocks = make(map[string]uint64, count)

	for i := uint32(0); i < count; i++ {
		var keyLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
			return err
		}

		keyBytes := make([]byte, keyLen)
		if _, err := buf.Read(keyBytes); err != nil {
			return err
		}

		var value uint64
		if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
			return err
		}

		vc.clocks[string(keyBytes)] = value
	}

	return nil
}

// =============================================================================
// VersionedData - Data wrapper with vector clock for causal ordering
// =============================================================================

// VersionedData wraps data with a vector clock for causal consistency
type VersionedData struct {
	Dataset string
	Data    []byte
	Clock   *VectorClock
}

// NewVersionedData creates versioned data with a vector clock
func NewVersionedData(dataset string, data []byte, clock *VectorClock) *VersionedData {
	return &VersionedData{
		Dataset: dataset,
		Data:    data,
		Clock:   clock.Copy(),
	}
}

// Supersedes returns true if this data happened after the other
func (vd *VersionedData) Supersedes(other *VersionedData) bool {
	return vd.Clock.Compare(other.Clock) == ClockAfter
}

// Conflicts returns true if this data is concurrent with the other
func (vd *VersionedData) Conflicts(other *VersionedData) bool {
	return vd.Clock.Compare(other.Clock) == ClockConcurrent
}
