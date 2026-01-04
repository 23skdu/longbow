package store

// calculateAdaptiveLimit determines the search depth based on filter selectivity.
// k: desired number of results
// matches: number of items matching the filter
// total: total number of items in the index
func calculateAdaptiveLimit(k int, matches uint64, total int) int {
	if total == 0 || matches == 0 {
		return k
	}

	selectivity := float64(matches) / float64(total)

	// Inverse selectivity: if 10% match, we should theoretically search 10x more nodes to find K valid ones.
	// We add a safety buffer.
	factor := 1.0 / selectivity

	// Clamp factor to avoid excessive searching or under-searching
	// Max factor 50: e.g. 2% selectivity. If lower, BruteForce path usually handles it (< 1000 matches).
	// Min factor 2: Always search at least 2*k to ensure quality.
	if factor > 50.0 {
		factor = 50.0
	}
	if factor < 2.0 {
		factor = 2.0
	}

	limit := int(float64(k) * factor)

	// Sanity checks
	if limit > total {
		limit = total
	}
	if limit < k {
		limit = k
	}

	return limit
}
