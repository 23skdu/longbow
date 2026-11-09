package sync

import (
	"testing"

	"github.com/rs/zerolog"
)

// FuzzSyncWorkerPeerRegistry exercises AddPeer with arbitrary addresses: the
// registry must deduplicate entries without clobbering sequence state that a
// peer has already recorded. AddPeer touches no network, disk or goroutine,
// so it is safe to fuzz directly (Start/Stop are deliberately not fuzzed —
// they own a background loop).
func FuzzSyncWorkerPeerRegistry(f *testing.F) {
	f.Add("10.0.0.1:50051", "10.0.0.2:50051", uint64(42))
	f.Add("peer-a", "peer-a", uint64(0))
	f.Add("", "", ^uint64(0))
	f.Add("addr\nwith\nnewlines", "other", uint64(1))
	f.Add("host:1", "host:2", uint64(1<<40))
	f.Add("duplicate.example:443", "duplicate.example:443", uint64(7))

	f.Fuzz(func(t *testing.T, addr1, addr2 string, seq uint64) {
		if len(addr1) > 4096 || len(addr2) > 4096 {
			return
		}

		logger := zerolog.Nop()
		w := NewSyncWorker(nil, &logger)

		w.AddPeer(addr1)
		first, ok := w.peers[addr1]
		if !ok {
			t.Fatalf("AddPeer(%q) did not register the peer", addr1)
		}
		if first.Addr != addr1 {
			t.Errorf("peer addr = %q, want %q", first.Addr, addr1)
		}
		if first.Priority != PriorityNormal {
			t.Errorf("peer priority = %v, want PriorityNormal", first.Priority)
		}
		// Record a seen sequence so duplicate registration can be checked
		// for state clobbering.
		first.LastSeenSeq.Store(seq)

		w.AddPeer(addr2)
		w.AddPeer(addr1) // duplicate must be a no-op

		if addr1 == addr2 {
			if len(w.peers) != 1 {
				t.Errorf("duplicate address grew the registry to %d peers", len(w.peers))
			}
		} else {
			if len(w.peers) != 2 {
				t.Errorf("registry has %d peers, want 2", len(w.peers))
			}
			second, ok := w.peers[addr2]
			if !ok {
				t.Fatalf("AddPeer(%q) did not register the second peer", addr2)
			}
			if second.Addr != addr2 {
				t.Errorf("second peer addr = %q, want %q", second.Addr, addr2)
			}
			if second.Priority != PriorityNormal {
				t.Errorf("second peer priority = %v, want PriorityNormal", second.Priority)
			}
			if got := second.LastSeenSeq.Load(); got != 0 {
				t.Errorf("fresh peer seq = %d, want 0", got)
			}
		}

		again, ok := w.peers[addr1]
		if !ok {
			t.Fatalf("peer %q vanished after a duplicate AddPeer", addr1)
		}
		if got := again.LastSeenSeq.Load(); got != seq {
			t.Errorf("re-adding the peer reset its seq: got %d, want %d", got, seq)
		}
	})
}
