package wasmclient

import (
	"sync"
)

// ConflictStrategy determines how divergent local vs remote mutations resolve during sync.
type ConflictStrategy string

const (
	ConflictLWW         ConflictStrategy = "lww"
	ConflictVectorClock ConflictStrategy = "vector_clock"
)

// PendingMutation represents a local offline-first write queued for server synchronization.
type PendingMutation struct {
	ClientID  string           `json:"client_id"`
	Op        string           `json:"op"` // "upsert" or "delete"
	Doc       EdgeDocument     `json:"doc"`
	Timestamp int64            `json:"timestamp"`
	Clock     map[string]int64 `json:"clock,omitempty"`
}

// SyncEngine manages offline mutation queuing, vector clocks, and conflict resolution.
type SyncEngine struct {
	mu       sync.Mutex
	clientID string
	strategy ConflictStrategy
	queue    []PendingMutation
	clocks   map[string]map[string]int64
}

// NewSyncEngine initializes an offline-first synchronization engine for an edge client.
func NewSyncEngine(clientID string, strategy ConflictStrategy) *SyncEngine {
	if clientID == "" {
		clientID = "edge-default"
	}
	if strategy == "" {
		strategy = ConflictLWW
	}
	return &SyncEngine{
		clientID: clientID,
		strategy: strategy,
		clocks:   make(map[string]map[string]int64),
	}
}

// RecordLocal queues a local mutation and advances the document's vector clock.
func (s *SyncEngine) RecordLocal(op string, doc EdgeDocument, ts int64) PendingMutation {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.clocks[doc.ID] == nil {
		s.clocks[doc.ID] = make(map[string]int64)
	}
	s.clocks[doc.ID][s.clientID]++
	doc.UpdatedAt = ts

	clockCopy := make(map[string]int64, len(s.clocks[doc.ID]))
	for k, v := range s.clocks[doc.ID] {
		clockCopy[k] = v
	}

	mut := PendingMutation{
		ClientID:  s.clientID,
		Op:        op,
		Doc:       doc,
		Timestamp: ts,
		Clock:     clockCopy,
	}
	s.queue = append(s.queue, mut)
	return mut
}

// PendingCount returns queued offline mutations waiting to sync.
func (s *SyncEngine) PendingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

// Drain returns queued mutations atomically when syncing to upstream.
func (s *SyncEngine) Drain() []PendingMutation {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) == 0 {
		return nil
	}
	out := make([]PendingMutation, len(s.queue))
	copy(out, s.queue)
	s.queue = nil
	return out
}

// Requeue restores mutations if upstream flush fails transiently.
func (s *SyncEngine) Requeue(muts []PendingMutation) {
	if len(muts) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queue = append(muts, s.queue...)
}

// ResolveConflict compares a local document state against a remote update and returns
// the winning EdgeDocument and merged vector clock.
func (s *SyncEngine) ResolveConflict(local EdgeDocument, localClock map[string]int64, remote EdgeDocument, remoteClock map[string]int64, remoteNodeID string) (EdgeDocument, map[string]int64) {
	mergedClock := mergeVectorClocks(localClock, remoteClock)

	if s.strategy == ConflictVectorClock && len(localClock) > 0 && len(remoteClock) > 0 {
		cmp := compareVectorClocks(localClock, remoteClock)
		if cmp > 0 {
			return local, mergedClock
		}
		if cmp < 0 {
			return remote, mergedClock
		}
		// Concurrent clocks -> fall back to LWW timestamp + deterministic node ID tie-breaker
	}

	if remote.UpdatedAt > local.UpdatedAt {
		return remote, mergedClock
	}
	if remote.UpdatedAt < local.UpdatedAt {
		return local, mergedClock
	}
	if remoteNodeID > s.clientID {
		return remote, mergedClock
	}
	return local, mergedClock
}

func compareVectorClocks(a, b map[string]int64) int {
	aGreater := false
	bGreater := false
	seen := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		seen[k] = struct{}{}
	}
	for k := range b {
		seen[k] = struct{}{}
	}
	for k := range seen {
		av := a[k]
		bv := b[k]
		if av > bv {
			aGreater = true
		} else if bv > av {
			bGreater = true
		}
	}
	if aGreater && !bGreater {
		return 1
	}
	if bGreater && !aGreater {
		return -1
	}
	return 0
}

func mergeVectorClocks(a, b map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		if v > out[k] {
			out[k] = v
		}
	}
	return out
}
