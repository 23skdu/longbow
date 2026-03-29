package store

import (
	"fmt"

	"github.com/23skdu/longbow/internal/metrics"
)

// ConsistencyEventual is the default fast search mode.
// HNSW uses its configured ef and ExactK=false.
const ConsistencyEventual = "eventual"

// ConsistencyStrong requests exhaustive search:
// ExactK is set to true and Ef is promoted to at least 2*K.
const ConsistencyStrong = "strong"

// ApplyConsistency enforces the Consistency field of opts onto the option
// values that actually drive HNSW behaviour (ExactK, Ef). It also emits the
// appropriate Prometheus counter.
//
// k is the number of results requested — used to compute the minimum Ef for
// strong mode.
//
// An empty Consistency value is treated as ConsistencyEventual.
// An unrecognised value returns an error.
func ApplyConsistency(opts *SearchOptions, k int, dataset string) error {
	level := opts.Consistency
	if level == "" {
		level = ConsistencyEventual
	}

	switch level {
	case ConsistencyEventual:
		metrics.SearchConsistencyLevelTotal.WithLabelValues(dataset, "eventual").Inc()
		// No changes to ExactK or Ef — use caller-supplied values.
		return nil

	case ConsistencyStrong:
		metrics.SearchConsistencyLevelTotal.WithLabelValues(dataset, "strong").Inc()
		opts.ExactK = true
		// Promote Ef to at least 2*K to widen the beam search.
		minEf := 2 * k
		if opts.Ef < minEf {
			opts.Ef = minEf
		}
		return nil

	default:
		return fmt.Errorf(
			"search consistency: unknown level %q — must be %q or %q",
			level, ConsistencyEventual, ConsistencyStrong,
		)
	}
}
