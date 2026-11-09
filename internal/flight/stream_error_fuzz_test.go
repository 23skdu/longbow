package flight

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// newFuzzStreamError builds the error for a given mode:
//
//	0: nil
//	1: plain error with a fuzzed message (legacy client style)
//	2: wrapped plain error with a fuzzed message
//	3: wrapped io.EOF sentinel
//	4: exact io.EOF sentinel
func newFuzzStreamError(mode uint8, msg string) (err error, wantEOF bool) {
	switch mode % 5 {
	case 0:
		return nil, false
	case 1:
		return errors.New(msg), strings.TrimSpace(msg) == "EOF"
	case 2:
		// The legacy "EOF" detection only matches the exact rendered
		// message; a wrapping prefix means it is never stream EOF.
		return fmt.Errorf("context: %w", errors.New(msg)), false
	case 3:
		return fmt.Errorf("read failed: %w", io.EOF), true
	default:
		return io.EOF, true
	}
}

// FuzzIsStreamEOF checks the stream-EOF detection contract across the
// sentinel, wrapped-sentinel, legacy string and nil cases.
func FuzzIsStreamEOF(f *testing.F) {
	f.Add(uint8(0), "")
	f.Add(uint8(1), "EOF")
	f.Add(uint8(1), "  EOF  ")
	f.Add(uint8(1), "connection reset")
	f.Add(uint8(2), "EOF")
	f.Add(uint8(3), "anything")
	f.Add(uint8(4), "x")

	f.Fuzz(func(t *testing.T, mode uint8, msg string) {
		if len(msg) > 4096 {
			return
		}

		err, want := newFuzzStreamError(mode, msg)
		if got := IsStreamEOF(err); got != want {
			t.Fatalf("mode %d msg %q: IsStreamEOF = %v, want %v", mode%5, msg, got, want)
		}
		if again := IsStreamEOF(err); again != want {
			t.Fatalf("mode %d msg %q: IsStreamEOF not deterministic", mode%5, msg)
		}
	})
}

// FuzzNormaliseStreamError checks that stream termination errors collapse to
// nil exactly when IsStreamEOF reports them, and that every other error (and
// nil) passes through unchanged.
func FuzzNormaliseStreamError(f *testing.F) {
	f.Add(uint8(0), "", uint8(0), uint8(0))
	f.Add(uint8(1), "EOF", uint8(1), uint8(1))
	f.Add(uint8(1), "boom", uint8(0), uint8(1))
	f.Add(uint8(2), "EOF", uint8(1), uint8(0))
	f.Add(uint8(3), "ignored", uint8(0), uint8(0))

	f.Fuzz(func(t *testing.T, mode uint8, msg string, direction, protocol uint8) {
		if len(msg) > 4096 {
			return
		}

		// Bounded label sets keep Prometheus cardinality in check.
		dirs := [2]string{"client", "server"}
		protos := [2]string{"arrow", "grpc"}
		dir := dirs[direction%2]
		proto := protos[protocol%2]

		err, _ := newFuzzStreamError(mode, msg)

		out := NormaliseStreamError(err, dir, proto)
		isEOF := IsStreamEOF(err)
		switch {
		case err == nil:
			if out != nil {
				t.Fatalf("nil error normalised to %v", out)
			}
		case isEOF:
			if out != nil {
				t.Fatalf("EOF error normalised to %v, want nil", out)
			}
		default:
			if out == nil {
				t.Fatalf("non-EOF error %q normalised to nil", err.Error())
			}
			if out.Error() != err.Error() {
				t.Fatalf("error changed: %q -> %q", err.Error(), out.Error())
			}
		}

		// Determinism: normalising twice yields the same outcome.
		again := NormaliseStreamError(err, dir, proto)
		if (again == nil) != (out == nil) {
			t.Fatalf("non-deterministic normalisation: %v vs %v", out, again)
		}
		if out != nil && again.Error() != out.Error() {
			t.Fatalf("non-deterministic message: %q vs %q", out.Error(), again.Error())
		}
	})
}

// FuzzErrorKind checks the metrics label categoriser: labels are drawn from
// the known set, nil errors map to "nil", and messages are categorised by
// their documented substrings.
func FuzzErrorKind(f *testing.F) {
	f.Add(uint8(0), "")
	f.Add(uint8(1), "operation canceled by user")
	f.Add(uint8(1), "context deadline exceeded")
	f.Add(uint8(1), "transport: connection refused")
	f.Add(uint8(1), "something else entirely")

	f.Fuzz(func(t *testing.T, mode uint8, msg string) {
		if len(msg) > 4096 {
			return
		}

		var err error
		if mode%2 == 1 {
			err = errors.New(msg)
		}

		got := errorKind(err)
		switch got {
		case "nil", "canceled", "deadline_exceeded", "transport", "other":
		default:
			t.Fatalf("errorKind returned unknown label %q", got)
		}
		if (err == nil) != (got == "nil") {
			t.Fatalf("errorKind(nil-parity): err=%v, got %q", err, got)
		}
		if err != nil {
			want := "other"
			switch {
			case strings.Contains(msg, "canceled"):
				want = "canceled"
			case strings.Contains(msg, "deadline"):
				want = "deadline_exceeded"
			case strings.Contains(msg, "transport"):
				want = "transport"
			}
			if got != want {
				t.Fatalf("errorKind(%q) = %q, want %q", msg, got, want)
			}
		}
	})
}
