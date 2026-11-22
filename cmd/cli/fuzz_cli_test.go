package main

import "testing"

// FuzzParseFloats runs the fuzz parse floats operation.
func FuzzParseFloats(f *testing.F) {
	f.Add("1.0, 2.0, 3.5")
	f.Add("")
	f.Add("a,b,c")
	f.Fuzz(func(t *testing.T, s string) {
		_ = parseFloats(s)
	})
}

// FuzzIsValidFilterExpr runs the fuzz is valid filter expr operation.
func FuzzIsValidFilterExpr(f *testing.F) {
	f.Add(`{"status": "active"}`)
	f.Add("")
	f.Add("invalid{{")
	f.Fuzz(func(t *testing.T, s string) {
		_ = isValidFilterExpr(s)
	})
}
