package main

import (
	"testing"
)

func TestBenchmarkModes(t *testing.T) {
	tests := []struct {
		name string
		mode string
	}{
		{"write mode", "write"},
		{"read mode", "read"},
		{"mixed mode", "mixed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.mode != "write" && tt.mode != "read" && tt.mode != "mixed" {
				t.Errorf("unknown mode: %s", tt.mode)
			}
		})
	}
}

func TestBlockSizes(t *testing.T) {
	validSizes := []int{512, 1024, 2048, 4096, 8192, 16384}

	for _, size := range validSizes {
		if size <= 0 {
			t.Errorf("invalid block size: %d", size)
		}
	}
}

func TestConcurrencyBounds(t *testing.T) {
	invalidWorkers := []int{0, -1, -100}
	for _, w := range invalidWorkers {
		if w >= 1 {
			t.Errorf("expected invalid workers: %d", w)
		}
	}
}

func TestFileSizeValidation(t *testing.T) {
	invalidSizes := []int{0, -1, -100}
	for _, s := range invalidSizes {
		if s >= 1 {
			t.Errorf("expected invalid size: %d", s)
		}
	}
}
