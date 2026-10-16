package store

// Roadmap Item 34: the gpu_kernel_profile action.
//
// The benchmark client asks for this on every GPU run and the CPU answer is
// just as important as the CUDA one: a report that says "no kernels were
// measured" has to be well-formed, because the alternative - a failed call, or
// an empty table that reads like a zero-time run - is what the item exists to
// prevent.

import (
	"encoding/json"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestDoAction_GPUKernelProfile(t *testing.T) {
	s := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<20, 0, 0)
	defer s.Close()

	stream := &mockDoActionServerCoverage{}
	require.NoError(t, s.DoAction(&flight.Action{Type: "gpu_kernel_profile"}, stream))
	require.Len(t, stream.results, 1, "the action must answer exactly one result")

	var resp struct {
		ProfilingEnabled bool    `json:"profiling_enabled"`
		AttributedPct    float64 `json:"attributed_pct"`
		Profile          struct {
			Ops           []map[string]any `json:"ops"`
			TotalMS       float64          `json:"total_ms"`
			UntimedCalls  int64            `json:"untimed_calls"`
			PoolExhausted int64            `json:"pool_exhausted"`
		} `json:"profile"`
	}
	require.NoError(t, json.Unmarshal(stream.results[0].Body, &resp))

	// No GPU work ran in this test, so the report must say so with numbers
	// rather than an error: the client parses this unconditionally. A CPU
	// build answers with an empty table; a CUDA build answers with the
	// operations it *would* have attributed, every one of them at zero - which
	// is the more useful of the two, because it names what was not seen.
	require.False(t, resp.ProfilingEnabled, "no profiling was requested")
	require.Zero(t, resp.Profile.TotalMS, "nothing ran, so no device time")
	require.Zero(t, resp.Profile.UntimedCalls)
	require.Zero(t, resp.Profile.PoolExhausted)
	require.Zero(t, resp.AttributedPct, "nothing measured means nothing attributed")

	seen := map[string]bool{}
	for _, op := range resp.Profile.Ops {
		name, _ := op["name"].(string)
		require.NotEmpty(t, name, "a reported operation must be named")
		require.False(t, seen[name], "operation %s appears twice", name)
		seen[name] = true
		require.Zero(t, op["calls"], "%s did not run in this test", name)
		require.Zero(t, op["total_ms"], "%s has no device time in this test", name)
	}
}
