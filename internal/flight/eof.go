package flight

import (
	"errors"
	"io"
	"strings"

	"github.com/23skdu/longbow/internal/metrics"
)

// IsStreamEOF returns true when err signals a normal stream termination.
// It handles three cases:
//
//  1. The canonical Go sentinel: io.EOF
//  2. Wrapped errors where io.EOF appears anywhere in the chain
//     (e.g. fmt.Errorf("context: %w", io.EOF))
//  3. Legacy string-only "EOF" errors produced by some Arrow/gRPC
//     client libraries that do not wrap the sentinel properly.
func IsStreamEOF(err error) bool {
	if err == nil {
		return false
	}
	// Fast path: exact sentinel match.
	if errors.Is(err, io.EOF) {
		return true
	}
	// Slow path: string comparison for legacy "EOF" strings from
	// client libraries that construct errors via errors.New("EOF").
	return strings.TrimSpace(err.Error()) == "EOF"
}

// NormaliseStreamError converts a stream-terminal error to nil so that
// callers can treat it as a clean end-of-stream. Non-terminal errors are
// returned unchanged.
//
// direction should be "client" or "server".
// protocol should be "arrow" or "grpc".
func NormaliseStreamError(err error, direction, protocol string) error {
	if IsStreamEOF(err) {
		metrics.EOFNormalisationTotal.WithLabelValues(direction, protocol).Inc()
		return nil
	}
	if err != nil {
		metrics.StreamTerminationErrors.WithLabelValues(direction, errorKind(err)).Inc()
	}
	return err
}

// errorKind returns a short label categorizing the error type for metrics.
func errorKind(err error) string {
	if err == nil {
		return "nil"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "canceled"):
		return "canceled"
	case strings.Contains(msg, "deadline"):
		return "deadline_exceeded"
	case strings.Contains(msg, "transport"):
		return "transport"
	default:
		return "other"
	}
}
