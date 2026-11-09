package main

import "testing"

func FuzzParseFloats(f *testing.F) {
	f.Add("1.0, 2.0, 3.5")
	f.Add("")
	f.Add("a,b,c")
	f.Fuzz(func(t *testing.T, s string) {
		_ = parseFloats(s)
	})
}

func FuzzIsValidFilterExpr(f *testing.F) {
	f.Add(`{"status": "active"}`)
	f.Add("")
	f.Add("invalid{{")
	f.Fuzz(func(t *testing.T, s string) {
		_ = isValidFilterExpr(s)
	})
}
