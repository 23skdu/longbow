package errors

import (
	"errors"
	"strings"
	"testing"
)

// checkStructuredError asserts the formatting and context contract shared by
// New and Wrap: Error() is deterministic, preserves type/operation/message
// (and cause) as substrings, a stack is captured, and WithContext never
// changes the message.
func checkStructuredError(t *testing.T, e *StructuredError, wantType ErrorType, op, msg string, cause error) {
	t.Helper()

	if e == nil {
		t.Fatal("structured error is nil")
	}
	if e.Type != wantType {
		t.Fatalf("Type = %q, want %q", e.Type, wantType)
	}
	if e.Operation != op {
		t.Fatalf("Operation = %q, want %q", e.Operation, op)
	}
	if e.Message != msg {
		t.Fatalf("Message = %q, want %q", e.Message, msg)
	}
	if e.Context == nil {
		t.Fatal("Context map not initialized")
	}
	if len(e.Stack) == 0 {
		t.Fatal("stack not captured")
	}

	first := e.Error()
	second := e.Error()
	if first != second {
		t.Fatalf("Error() not deterministic: %q vs %q", first, second)
	}
	if !strings.Contains(first, string(wantType)) {
		t.Fatalf("Error() %q missing type %q", first, wantType)
	}
	if !strings.Contains(first, op) {
		t.Fatalf("Error() %q missing operation %q", first, op)
	}
	if !strings.Contains(first, msg) {
		t.Fatalf("Error() %q missing message %q", first, msg)
	}

	// WithContext must not alter the rendered message.
	e.WithContext("fuzz.key", msg)
	if got := e.Context["fuzz.key"]; got != msg {
		t.Fatalf("Context[fuzz.key] = %v, want %q", got, msg)
	}
	if after := e.Error(); after != first {
		t.Fatalf("WithContext changed Error(): %q -> %q", first, after)
	}

	if cause == nil {
		if e.Unwrap() != nil {
			t.Fatalf("Unwrap() = %v, want nil", e.Unwrap())
		}
		return
	}
	if e.Unwrap() != cause {
		t.Fatalf("Unwrap() = %v, want %v", e.Unwrap(), cause)
	}
	if !errors.Is(e, cause) {
		t.Fatal("errors.Is(wrapped, cause) = false")
	}
	if !strings.Contains(first, cause.Error()) {
		t.Fatalf("Error() %q missing cause %q", first, cause.Error())
	}
}

// FuzzStructuredErrorFormat builds structured errors from fuzzed type,
// operation, message and cause values and checks their formatting contract
// for the New, Wrap and Wrap(nil) paths.
func FuzzStructuredErrorFormat(f *testing.F) {
	f.Add(uint8(0), "validation", "parse", "bad input", "")
	f.Add(uint8(1), "storage", "save", "failed", "disk full")
	f.Add(uint8(2), "network", "", "", "")
	f.Add(uint8(3), "", "", "", "cause")
	f.Add(uint8(4), "timeout", "wait", "deadline", "context deadline exceeded")
	f.Add(uint8(5), "computation", "dot", "mismatched dims", "boom")

	f.Fuzz(func(t *testing.T, mode uint8, errType, op, msg, cause string) {
		if len(errType)+len(op)+len(msg)+len(cause) > 4096 {
			return
		}

		switch mode % 3 {
		case 0:
			e := New(ErrorType(errType), op, msg)
			checkStructuredError(t, e, ErrorType(errType), op, msg, nil)
		case 1:
			causeErr := errors.New(cause)
			e := Wrap(causeErr, ErrorType(errType), op, msg)
			checkStructuredError(t, e, ErrorType(errType), op, msg, causeErr)
		case 2:
			if got := Wrap(nil, ErrorType(errType), op, msg); got != nil {
				t.Fatalf("Wrap(nil) = %v, want nil", got)
			}
		}
	})
}
