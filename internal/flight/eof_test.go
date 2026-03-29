package flight

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIsStreamEOF_GoSentinel verifies that the canonical io.EOF sentinel is
// detected correctly.
func TestIsStreamEOF_GoSentinel(t *testing.T) {
	assert.True(t, IsStreamEOF(io.EOF), "io.EOF must be detected as stream end")
}

// TestIsStreamEOF_WrappedEOF verifies that a wrapped io.EOF
// (as produced by fmt.Errorf + %w) is detected correctly.
func TestIsStreamEOF_WrappedEOF(t *testing.T) {
	wrapped := fmt.Errorf("read failed: %w", io.EOF)
	assert.True(t, IsStreamEOF(wrapped), "wrapped io.EOF must be detected as stream end")
}

// TestIsStreamEOF_StringOnlyEOF verifies backward compatibility with legacy
// Arrow/gRPC clients that create errors via errors.New("EOF").
func TestIsStreamEOF_StringOnlyEOF(t *testing.T) {
	legacyEOF := errors.New("EOF")
	// Note: errors.Is will return false here (different pointer), but
	// IsStreamEOF's string-path must catch it.
	require.False(t, errors.Is(legacyEOF, io.EOF), "pre-condition: not a wrapped sentinel")
	assert.True(t, IsStreamEOF(legacyEOF), "string 'EOF' must be detected as stream end")
}

// TestIsStreamEOF_OtherError verifies that unrelated errors are not
// mis-classified as stream termination.
func TestIsStreamEOF_OtherError(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"nil", nil},
		{"context canceled", fmt.Errorf("context canceled")},
		{"transport error", fmt.Errorf("transport: connection reset by peer")},
		{"rpc error", fmt.Errorf("rpc error: code = Unavailable desc = server gone")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.False(t, IsStreamEOF(tc.err), "non-EOF error must not be detected as stream end")
		})
	}
}

// TestNormaliseStreamError_EOFBecomesNil verifies that a stream-terminal error
// is converted to nil by NormaliseStreamError.
func TestNormaliseStreamError_EOFBecomesNil(t *testing.T) {
	assert.Nil(t, NormaliseStreamError(io.EOF, "server", "arrow"))
	assert.Nil(t, NormaliseStreamError(fmt.Errorf("done: %w", io.EOF), "client", "grpc"))
	assert.Nil(t, NormaliseStreamError(errors.New("EOF"), "client", "arrow"))
}

// TestNormaliseStreamError_NonEOFPassThrough verifies that genuine errors are
// returned unchanged by NormaliseStreamError.
func TestNormaliseStreamError_NonEOFPassThrough(t *testing.T) {
	orig := fmt.Errorf("unexpected server error")
	got := NormaliseStreamError(orig, "server", "grpc")
	assert.Equal(t, orig, got, "non-EOF error must be returned unchanged")
}
