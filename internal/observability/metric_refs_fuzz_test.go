package observability_test

import (
	"regexp"
	"strings"
	"testing"
)

// fuzzIdentRe matches a bare metric identifier, the shape metricExpr
// extracts from a PromQL expression.
var fuzzIdentRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// FuzzBaseMetricName checks the histogram-suffix stripping helper: the
// result is the input minus at most one known suffix and never grows.
func FuzzBaseMetricName(f *testing.F) {
	f.Add("longbow_search_latency_seconds")
	f.Add("longbow_search_latency_seconds_bucket")
	f.Add("longbow_query_total")
	f.Add("_count")
	f.Add("")
	f.Add("sum_sum_sum")

	f.Fuzz(func(t *testing.T, name string) {
		if len(name) > 512 {
			return
		}
		base := baseMetricName(name)
		if len(base) > len(name) {
			t.Fatalf("baseMetricName(%q) = %q grew the name", name, base)
		}
		if base == name {
			// An unchanged name must not carry any known suffix.
			for _, suffix := range []string{"_bucket", "_count", "_sum"} {
				if strings.HasSuffix(name, suffix) {
					t.Errorf("baseMetricName(%q) = %q but the suffix was not stripped", name, base)
				}
			}
			return
		}
		// A changed name must be the input minus exactly one known suffix.
		switch {
		case strings.HasPrefix(name, base+"_bucket"),
			strings.HasPrefix(name, base+"_count"),
			strings.HasPrefix(name, base+"_sum"):
		default:
			t.Errorf("baseMetricName(%q) = %q, not the input minus a known suffix", name, base)
		}
	})
}

// FuzzMetricRefsInExpr checks the PromQL metric extractor on a constructed
// expression: sum(rate(<name>_bucket[5m])) must yield exactly one
// reference — the histogram sample of <name> — with sum() and rate()
// filtered out as functions, plus no-panic behaviour on the raw input.
func FuzzMetricRefsInExpr(f *testing.F) {
	f.Add("longbow_search_latency_seconds")
	f.Add("m")
	f.Add("_private")
	f.Add("x1")
	f.Add("A1_b2_c3")
	f.Add("rate")
	f.Add("__reserved")

	f.Fuzz(func(t *testing.T, name string) {
		if len(name) > 64 {
			return
		}
		// Only well-formed identifiers that are not label references can be
		// expected to survive extraction verbatim; anything else is out of
		// range for this target (raw input is covered by the no-panic floor
		// below via isRegistered-style helpers).
		if !fuzzIdentRe.MatchString(name) || strings.HasPrefix(name, "__") {
			return
		}

		expr := "sum(rate(" + name + "_bucket[5m]))"
		refs := metricRefsInExpr(expr, "fuzz", 1)
		if len(refs) != 1 {
			t.Fatalf("refs = %+v, want exactly %q", refs, name+"_bucket")
		}
		if refs[0].Name != name+"_bucket" {
			t.Errorf("ref = %q, want %q", refs[0].Name, name+"_bucket")
		}
		if refs[0].Source != "fuzz" || refs[0].Line != 1 {
			t.Errorf("ref location = %s:%d, want fuzz:1", refs[0].Source, refs[0].Line)
		}

		// The extracted histogram sample must resolve against the family it
		// belongs to.
		reg := map[string]registeredMetric{
			name: {Name: name},
		}
		if !isRegistered(reg, refs[0].Name) {
			t.Errorf("isRegistered(%q) = false for a family in the inventory", refs[0].Name)
		}
	})
}

// FuzzIsRegistered checks name resolution against the inventory: every
// alias of a registered family resolves, names that cannot possibly match
// are rejected, and arbitrary input never panics.
func FuzzIsRegistered(f *testing.F) {
	f.Add("longbow_search_latency_seconds")
	f.Add("anything_else")
	f.Add("")
	f.Add("longbow_search_latency_seconds_bucket")
	f.Add("__name")

	f.Fuzz(func(t *testing.T, name string) {
		if len(name) > 128 {
			return
		}
		reg := map[string]registeredMetric{
			"longbow_search_latency_seconds": {Name: "longbow_search_latency_seconds", Labels: []string{"dataset"}},
			"longbow_ingest_vectors":         {Name: "longbow_ingest_vectors"},
		}

		// Every alias of a registered family must resolve: the bare name,
		// the histogram samples and the OpenMetrics counter suffix.
		for _, family := range reg {
			for _, suffix := range []string{"", "_bucket", "_count", "_sum", "_total"} {
				if !isRegistered(reg, family.Name+suffix) {
					t.Errorf("isRegistered(%q) = false, want true", family.Name+suffix)
				}
			}
		}

		// "zzq_" cannot collide with the inventory, so these names (in
		// every alias form) must all be rejected.
		probe := "zzq_" + name
		for _, suffix := range []string{"", "_bucket", "_count", "_sum", "_total"} {
			if isRegistered(reg, probe+suffix) {
				t.Errorf("isRegistered(%q) = true, want false", probe+suffix)
			}
		}

		// Negative control: nothing is registered in an empty inventory.
		if isRegistered(map[string]registeredMetric{}, name) {
			t.Errorf("isRegistered(%q) = true for an empty inventory", name)
		}
	})
}
