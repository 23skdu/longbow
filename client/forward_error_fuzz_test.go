package client

import (
	"errors"
	"strings"
	"testing"
)

// FuzzIsForwardRequired fuzzes parsing of FORWARD_REQUIRED wire errors.
// Invariants: messages without the marker never parse as forward errors,
// parsing is deterministic, and re-parsing a formatted forward error is
// idempotent (it reproduces the same target/addr fields).
func FuzzIsForwardRequired(f *testing.F) {
	seeds := []string{
		"FORWARD_REQUIRED: target=node2 addr=127.0.0.1:3001",
		"FORWARD_REQUIRED: target=node1 addr=unix:///tmp/lb.sock",
		"rpc error: code = Unavailable desc = connection refused",
		"FORWARD_REQUIRED: malformed",
		"",
		"FORWARD_REQUIRED: target=  spaced   addr=  x  ",
		"FORWARD_REQUIRED: target=a addr=b\nFORWARD_REQUIRED: target=c addr=d",
		"FORWARD_REQUIRED: target=a addr=b addr=c",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, msg string) {
		got := IsForwardRequired(errors.New(msg))

		// A message without the marker can never be a forward error.
		if !strings.Contains(msg, "FORWARD_REQUIRED") && got != nil {
			t.Fatalf("IsForwardRequired(%q) = %+v, want nil for message without marker", msg, got)
		}
		if got == nil {
			return
		}

		// Parsing the same message twice must yield the same fields.
		again := IsForwardRequired(errors.New(msg))
		if again == nil || again.TargetNodeID != got.TargetNodeID || again.TargetAddr != got.TargetAddr {
			t.Fatalf("IsForwardRequired not deterministic for %q: first=%+v second=%+v", msg, got, again)
		}

		// Round-trip idempotence: formatting the parsed error and parsing it
		// again must reproduce the same fields.
		reparsed := IsForwardRequired(errors.New(got.Error()))
		if reparsed == nil {
			t.Fatalf("re-parsing %q (formatted from %+v) returned nil", got.Error(), got)
		}
		if reparsed.TargetNodeID != got.TargetNodeID || reparsed.TargetAddr != got.TargetAddr {
			t.Fatalf("round trip changed fields: before=%+v after=%+v", got, reparsed)
		}
	})
}

// FuzzForwardErrorRoundTrip fuzzes ErrForwardRequired -> Error() -> parse.
// Invariant: for messages inside the wire format's domain (no embedded
// newlines, no " addr=" inside the target field), parsing the formatted
// error recovers both fields modulo the TrimSpace the parser applies.
func FuzzForwardErrorRoundTrip(f *testing.F) {
	seeds := [][2]string{
		{"node2", "127.0.0.1:3001"},
		{"node1", "unix:///tmp/lb.sock"},
		{" spaced ", "  x  "},
		{"", ""},
		{"a=b", "c d"},
		{"node3", "host:1234/path?x=1"},
		{"x addr=y", "z"}, // out of domain: guarded below
	}
	for _, s := range seeds {
		f.Add(s[0], s[1])
	}

	f.Fuzz(func(t *testing.T, nodeID, addr string) {
		if len(nodeID) > 512 || len(addr) > 512 {
			return
		}
		// Out of the format's domain: the regex splits at the first
		// " addr=" inside the target field, and cannot cross newlines.
		if strings.Contains(nodeID, " addr=") || strings.Contains(nodeID, "\n") || strings.Contains(addr, "\n") {
			return
		}

		src := &ErrForwardRequired{TargetNodeID: nodeID, TargetAddr: addr}
		got := IsForwardRequired(errors.New(src.Error()))
		if got == nil {
			t.Fatalf("IsForwardRequired(%q) returned nil, want parsed struct", src.Error())
		}
		if want := strings.TrimSpace(nodeID); got.TargetNodeID != want {
			t.Fatalf("TargetNodeID = %q, want %q (input %q)", got.TargetNodeID, want, nodeID)
		}
		if want := strings.TrimSpace(addr); got.TargetAddr != want {
			t.Fatalf("TargetAddr = %q, want %q (input %q)", got.TargetAddr, want, addr)
		}
	})
}
