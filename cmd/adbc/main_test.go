package main

import (
	"testing"
)

func TestMainSmoke(t *testing.T) {
	main()
}

func TestIsSupportedADBCVersion(t *testing.T) {
	tests := []struct {
		version  int
		expected bool
	}{
		{0, true},
		{1000000, true},
		{1001000, true},
		{1001999, true},
		{999, false},
		{1002000, false},
		{2000000, false},
		{-1, false},
	}

	for _, tt := range tests {
		got := isSupportedADBCVersion(tt.version)
		if got != tt.expected {
			t.Errorf("isSupportedADBCVersion(%d) = %v; want %v", tt.version, got, tt.expected)
		}
	}
}
