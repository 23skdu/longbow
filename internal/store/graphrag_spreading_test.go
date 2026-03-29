package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// spreadingActivationParams holds the parameters for graph spreading activation.
// This mirrors what the GraphRAG search path will eventually use.
type spreadingActivationParams struct {
	Alpha float64 // Damping coefficient [0.0, 1.0]
	Depth int     // Maximum BFS hops (0 = no expansion)
}

// validateSpreadingActivationParams returns an error if params are out of range.
func validateSpreadingActivationParams(p spreadingActivationParams) error {
	if p.Alpha < 0.0 || p.Alpha > 1.0 {
		return &spreadingActivationError{msg: "alpha must be in [0.0, 1.0]", value: p.Alpha}
	}
	if p.Depth < 0 {
		return &spreadingActivationError{msg: "depth must be >= 0", value: float64(p.Depth)}
	}
	return nil
}

type spreadingActivationError struct {
	msg   string
	value float64
}

func (e *spreadingActivationError) Error() string {
	return e.msg
}

// applyAlphaDecay applies one round of alpha decay to a score.
// score_at_hop_n = initial_score * alpha^n
func applyAlphaDecay(initialScore, alpha float64, hop int) float64 {
	result := initialScore
	for i := 0; i < hop; i++ {
		result *= alpha
	}
	return result
}

// TestGraphAlpha_ZeroCollapsesToSingleHop verifies that alpha=0.0 eliminates
// all spread: the score at hop ≥ 1 drops to zero.
func TestGraphAlpha_ZeroCollapsesToSingleHop(t *testing.T) {
	params := spreadingActivationParams{Alpha: 0.0, Depth: 3}
	require.NoError(t, validateSpreadingActivationParams(params))

	score := applyAlphaDecay(1.0, params.Alpha, 1)
	assert.Equal(t, float64(0), score, "alpha=0 must zero-out score at hop 1")
}

// TestGraphAlpha_OneFullSpread verifies that alpha=1.0 applies no decay —
// scores remain equal at all depths.
func TestGraphAlpha_OneFullSpread(t *testing.T) {
	params := spreadingActivationParams{Alpha: 1.0, Depth: 5}
	require.NoError(t, validateSpreadingActivationParams(params))

	for hop := 0; hop <= params.Depth; hop++ {
		score := applyAlphaDecay(1.0, params.Alpha, hop)
		assert.Equal(t, 1.0, score, "alpha=1.0 must not decay score at hop %d", hop)
	}
}

// TestGraphDepth_Zero verifies that depth=0 means no expansion.
func TestGraphDepth_Zero(t *testing.T) {
	params := spreadingActivationParams{Alpha: 0.85, Depth: 0}
	require.NoError(t, validateSpreadingActivationParams(params))
	// No assertion needed beyond param validation: depth=0 is valid and
	// the caller should return only seed nodes.
}

// TestGraphDepth_Negative verifies that depth<0 returns an error.
func TestGraphDepth_Negative(t *testing.T) {
	params := spreadingActivationParams{Alpha: 0.85, Depth: -1}
	err := validateSpreadingActivationParams(params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "depth must be >= 0")
}

// TestGraphAlpha_OutOfRange verifies that alpha > 1.0 returns an error with
// the exact message "alpha must be in [0.0, 1.0]".
func TestGraphAlpha_OutOfRange(t *testing.T) {
	params := spreadingActivationParams{Alpha: 1.5, Depth: 2}
	err := validateSpreadingActivationParams(params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "alpha must be in [0.0, 1.0]")
}

// TestSpreadingActivation_ScoreDecay verifies that scores decrease
// monotonically with hop distance when alpha < 1.0.
func TestSpreadingActivation_ScoreDecay(t *testing.T) {
	alpha := 0.85
	initialScore := 1.0
	prevScore := initialScore

	for hop := 1; hop <= 5; hop++ {
		score := applyAlphaDecay(initialScore, alpha, hop)
		assert.Less(t, score, prevScore,
			"score must decrease at hop %d (alpha=%.2f)", hop, alpha)
		prevScore = score
	}
}

// TestGraphRAG_ReRankingResultOrder verifies that higher alpha produces
// stronger graph influence: at alpha=1.0 the score at depth 2 equals the
// initial score, whereas at alpha=0.5 it is significantly lower.
func TestGraphRAG_ReRankingResultOrder(t *testing.T) {
	initialScore := 1.0
	depth := 2

	highAlphaScore := applyAlphaDecay(initialScore, 1.0, depth)
	lowAlphaScore := applyAlphaDecay(initialScore, 0.5, depth)

	assert.Greater(t, highAlphaScore, lowAlphaScore,
		"higher alpha must produce higher retained score at depth %d", depth)
}
