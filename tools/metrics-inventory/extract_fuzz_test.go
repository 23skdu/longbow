package main

import (
	"go/parser"
	"go/token"
	"reflect"
	"testing"
)

// FuzzExtractMetricRegistrations fuzzes the AST-based metric extractor with
// arbitrary Go source. Invariants: parsing and extraction are deterministic
// (same source always yields the same registrations), every registration is
// attributed to the scanned file with a valid line number, and the number of
// registrations is bounded by the input size. Invalid source must be
// rejected by the parser deterministically rather than panicking.
func FuzzExtractMetricRegistrations(f *testing.F) {
	// Valid seeds.
	f.Add([]byte(`package test
import "github.com/prometheus/client_golang/prometheus/promauto"
import "github.com/prometheus/client_golang/prometheus"
var c = promauto.NewCounter(prometheus.CounterOpts{
	Name: "longbow_test_total",
})
`))
	f.Add([]byte(`package test
var x = mustRegister("longbow_helper_total", []string{"a", "b"})
`))
	f.Add([]byte(`package test
var x = newMetric("longbow_gauge_one", []string{"l1", "l2"})
`))
	f.Add([]byte(`package test
import "github.com/prometheus/client_golang/prometheus/promauto"
import "github.com/prometheus/client_golang/prometheus"
var c = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "longbow",
	Name:      "sub_total",
})
`))
	// Invalid seeds: syntax error, binary junk, empty source.
	f.Add([]byte("package {{{ not valid"))
	f.Add([]byte{0xff, 0xfe, 0x00, 0x01})
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, src []byte) {
		// Bound: keeps parsing fast and the extracted output proportional
		// to the input size.
		if len(src) > 4096 {
			return
		}

		fset1 := token.NewFileSet()
		f1, err1 := parser.ParseFile(fset1, "fuzz.go", src, 0)
		fset2 := token.NewFileSet()
		f2, err2 := parser.ParseFile(fset2, "fuzz.go", src, 0)

		if (err1 == nil) != (err2 == nil) {
			t.Fatalf("parser determinism violated: err1=%v, err2=%v", err1, err2)
		}
		if err1 != nil {
			// Invalid source must be rejected deterministically (the
			// invariant for invalid input: no panic, stable error).
			if err1.Error() != err2.Error() {
				t.Fatalf("parser error not deterministic: %q vs %q", err1, err2)
			}
			return
		}

		regs1 := extract(fset1, "fuzz.go", f1)
		regs2 := extract(fset2, "fuzz.go", f2)
		if !reflect.DeepEqual(regs1, regs2) {
			t.Fatalf("extract not deterministic:\n%+v\n%+v", regs1, regs2)
		}
		if len(regs1) > len(src) {
			t.Fatalf("extracted %d registrations from %d-byte source", len(regs1), len(src))
		}
		for _, r := range regs1 {
			if r.file != "fuzz.go" {
				t.Fatalf("registration file = %q, want fuzz.go", r.file)
			}
			if r.line < 1 {
				t.Fatalf("registration for %q has line %d, want >= 1", r.name, r.line)
			}
		}
	})
}
