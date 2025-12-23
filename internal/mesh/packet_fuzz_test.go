package mesh

import (
	"bytes"
	"testing"
)

func FuzzDecodePacket(f *testing.F) {
	// Seed corpus matches HeaderSize (6 bytes) + some payload
	f.Add([]byte{1, 0, 0, 0, 0, 0})                 // Min valid packet
	f.Add([]byte{1, 0, 0, 0, 1, 1, 0xff})           // Seq=1, Updates=1, payload=0xff
	f.Add([]byte{2, 0, 0, 0, 10, 5, 1, 2, 3, 4, 5}) // Type=2

	f.Fuzz(func(t *testing.T, data []byte) {
		pkt, err := DecodePacket(data)

		// 1. Basic error checking
		if len(data) < HeaderSize {
			if err == nil {
				t.Errorf("expected error for short data len=%d, got nil", len(data))
			}
			return
		}

		if err != nil {
			// If we provided >= HeaderSize, DecodePacket currently only checks len.
			// So it should largely succeed unless we change validation logic.
			// But if it fails, ensuring it doesn't panic is the main goal.
			return
		}

		// 2. Property testing: Re-encode should match input
		if pkt == nil {
			t.Fatal("expected non-nil packet on success")
		}

		// Round trip
		dst := make([]byte, HeaderSize+len(pkt.Payload))
		n, err := EncodePacket(pkt, dst)
		if err != nil {
			t.Fatalf("EncodePacket failed on valid packet: %v", err)
		}

		if n != len(data) {
			t.Errorf("encoded length mismatch: got %d, want %d", n, len(data))
		}

		if !bytes.Equal(dst, data) {
			t.Errorf("round-trip mismatch:\ninput: %x\noutput:%x", data, dst)
		}
	})
}
