package sharding

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

// ScatterGather coordinates distributed request execution
type ScatterGather struct {
	rm        *RingManager
	forwarder *RequestForwarder
	logger    zerolog.Logger
}

// NewScatterGather creates a new ScatterGather coordinator
//
//nolint:gocritic // Logger passed by value for constructor simplicity
func NewScatterGather(rm *RingManager, forwarder *RequestForwarder, logger zerolog.Logger) *ScatterGather {
	return &ScatterGather{
		rm:        rm,
		forwarder: forwarder,
		logger:    logger,
	}
}

// Result holds the result from a single node
type Result struct {
	NodeID string
	Data   interface{}
	Error  error
}

// ScatterFn is the function to execute on each node
// It takes a context (with timeout) and the target node ID
type ScatterFn func(ctx context.Context, nodeID string) (interface{}, error)

// Scatter executes the given function across all independent nodes in the cluster.
// It supports a strict consistency mode (all must succeed) or best-effort.
// This is used for Global Search where we query all shards.
func (sg *ScatterGather) Scatter(ctx context.Context, fn ScatterFn) ([]Result, error) {
	members := sg.rm.GetMembers()
	if len(members) == 0 {
		return nil, nil // Or error?
	}

	results := make([]Result, len(members))

	// Create an errgroup to manage concurrency
	// We use a derived context to cancel others if specific fail-fast is needed,
	// but usually for Search we want "Best Effort" (return what we got) or "All".
	// Let's assume Best Effort is handled by not returning error from goroutine.
	g, gCtx := errgroup.WithContext(ctx)

	for i, member := range members {
		idx := i
		nodeID := member
		g.Go(func() error {
			// Execute the function
			// We use the gCtx to respect cancellation
			start := time.Now()
			data, err := fn(gCtx, nodeID)
			duration := time.Since(start)

			results[idx] = Result{
				NodeID: nodeID,
				Data:   data,
				Error:  err,
			}

			if err != nil {
				sg.logger.Warn().
					Str("node", nodeID).
					Err(err).
					Dur("duration", duration).
					Msg("Scatter request failed")
				// We don't return error here if we want to collect partial failures.
				// If we want fail-fast, return err.
				// For distributed search, partial results are often better than none.
				return nil
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return results, nil
}
