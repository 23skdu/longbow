package types

import "runtime"

// ParallelSearchConfig controls parallel search behavior
type ParallelSearchConfig struct {
	Enabled      bool
	Workers      int
	Threshold    int
	MinChunkSize int
	MaxChunkSize int
}

// DefaultParallelSearchConfig returns sensible defaults
func DefaultParallelSearchConfig() ParallelSearchConfig {
	return ParallelSearchConfig{
		Enabled:      true,
		Workers:      runtime.NumCPU(),
		Threshold:    100,
		MinChunkSize: 32,
		MaxChunkSize: 500,
	}
}
