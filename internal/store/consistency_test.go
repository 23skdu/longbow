package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConsistencyLevelEventual verifies that "eventual" leaves ExactK and Ef
// unchanged.
func TestConsistencyLevelEventual(t *testing.T) {
	opts := &SearchOptions{Ef: 64, ExactK: false, Consistency: ConsistencyEventual}
	require.NoError(t, ApplyConsistency(opts, 10, "test_ds"))
	assert.False(t, opts.ExactK, "eventual: ExactK must remain false")
	assert.Equal(t, 64, opts.Ef, "eventual: Ef must remain unchanged")
}

// TestConsistencyLevelStrong verifies that "strong" enables ExactK and
// promotes Ef to at least 2*K.
func TestConsistencyLevelStrong(t *testing.T) {
	opts := &SearchOptions{Ef: 10, ExactK: false, Consistency: ConsistencyStrong}
	k := 20
	require.NoError(t, ApplyConsistency(opts, k, "test_ds"))
	assert.True(t, opts.ExactK, "strong: ExactK must be true")
	assert.GreaterOrEqual(t, opts.Ef, 2*k, "strong: Ef must be >= 2*K")
}

// TestConsistencyLevelStrong_AlreadyHighEf verifies that a caller-supplied
// Ef that is already ≥ 2*K is not reduced.
func TestConsistencyLevelStrong_AlreadyHighEf(t *testing.T) {
	opts := &SearchOptions{Ef: 500, Consistency: ConsistencyStrong}
	require.NoError(t, ApplyConsistency(opts, 10, "test_ds"))
	assert.Equal(t, 500, opts.Ef, "strong: high existing Ef must not be reduced")
}

// TestConsistencyLevelDefault verifies that an empty string is treated as
// "eventual".
func TestConsistencyLevelDefault(t *testing.T) {
	opts := &SearchOptions{Ef: 32, ExactK: false, Consistency: ""}
	require.NoError(t, ApplyConsistency(opts, 5, "test_ds"))
	assert.False(t, opts.ExactK, "default: ExactK must remain false")
	assert.Equal(t, 32, opts.Ef, "default: Ef must remain unchanged")
}

// TestConsistencyLevelInvalid verifies that an unrecognised consistency value
// returns a descriptive error.
func TestConsistencyLevelInvalid(t *testing.T) {
	opts := &SearchOptions{Consistency: "linearizable"}
	err := ApplyConsistency(opts, 10, "test_ds")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "linearizable", "error must quote the invalid level")
	assert.Contains(t, err.Error(), ConsistencyEventual, "error must list valid options")
	assert.Contains(t, err.Error(), ConsistencyStrong, "error must list valid options")
}
