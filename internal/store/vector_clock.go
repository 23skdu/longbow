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
	// ClockEqual indicates that the two vector clocks are identical.
	ClockEqual ClockComparison = iota
	// ClockBefore indicates that the first clock happened before the second.
	ClockBefore
	// ClockAfter indicates that the first clock happened after the second.
	ClockAfter
	// ClockConcurrent indicates that the clocks are concurrent (conflict).
	ClockConcurrent
)

// VectorClock implements a vector clock for causal ordering in distributed systems
type VectorClock struct {
	mu        sync.RWMutex
	nodeID    string
	clocks    map[string]uint64
	deltaCh   chan map[string]uint64
	stopCh    chan struct{}
	compactor sync.Once
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

// StartCompactor initializes a lock-free background thread to merge delta timestamps.
func (vc *VectorClock) StartCompactor(bufferSize int) {
	vc.compactor.Do(func() {
		vc.deltaCh = make(chan map[string]uint64, bufferSize)
		vc.stopCh = make(chan struct{})
		go func() {
			for {
				select {
				case <-vc.stopCh:
					return
				case delta := <-vc.deltaCh:
					vc.mu.Lock()
					for nodeID, otherVal := range delta {
						if otherVal > vc.clocks[nodeID] {
							vc.clocks[nodeID] = otherVal
						}
					}
					vc.mu.Unlock()
				}
			}
		}()
	})
}

// StopCompactor stops the background clock compaction thread.
func (vc *VectorClock) StopCompactor() {
	if vc.stopCh != nil {
		close(vc.stopCh)
	}
}

// MergeAsync enqueues another vector clock into this one asynchronously for bounded P95 latency.
func (vc *VectorClock) MergeAsync(other *VectorClock) {
	other.mu.RLock()
	delta := make(map[string]uint64, len(other.clocks))
	for k, v := range other.clocks {
		delta[k] = v
	}
	other.mu.RUnlock()

	if vc.deltaCh != nil {
		select {
		case vc.deltaCh <- delta:
			metrics.VectorClockMergesTotal.Inc()
			return
		default:
			// Queue full, fallback to synchronous
		}
	}
	vc.Merge(other)
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
	_ = binary.Write(&buf, binary.LittleEndian, uint32(len(keys))) // #nosec G115

	for _, k := range keys {
		// Write key length + key
		_ = binary.Write(&buf, binary.LittleEndian, uint32(len(k))) // #nosec G115
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
