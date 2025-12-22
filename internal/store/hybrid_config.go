package store

import (
	"errors"
)

// HybridSearchConfig configures hybrid search behavior combining BM25 and vector search
type HybridSearchConfig struct {
	// Alpha controls the weighting between vector and keyword search
	// Alpha = 1.0 means pure vector search (dense)
	// Alpha = 0.0 means pure keyword search (sparse/BM25)
	// Alpha = 0.5 means balanced hybrid
	Alpha float32

	// TextColumns specifies which columns should be indexed for BM25 search
	TextColumns []string

	// RRFk is the k parameter for Reciprocal Rank Fusion
	// Higher values give more weight to lower-ranked results
	// Typical values: 60 (default), range 1-1000
	RRFk int

	// BM25 contains the BM25 algorithm parameters
	BM25 BM25Config

	// Enabled determines if hybrid search is active
	Enabled bool
}

// DefaultHybridSearchConfig returns a HybridSearchConfig with sensible defaults
func DefaultHybridSearchConfig() HybridSearchConfig {
	return HybridSearchConfig{
		Alpha:       0.5, // Balanced hybrid
		TextColumns: nil, // Must be set explicitly
		RRFk:        60,  // Standard RRF parameter
		BM25:        DefaultBM25Config(),
		Enabled:     false, // Opt-in feature
	}
}

// Validate checks if the configuration is valid
func (c *HybridSearchConfig) Validate() error {
	// Disabled config is always valid
	if !c.Enabled {
		return nil
	}

	// Alpha must be in [0.0, 1.0]
	if c.Alpha < 0.0 || c.Alpha > 1.0 {
		return errors.New("HybridSearchConfig: Alpha must be between 0.0 and 1.0")
	}

	// RRFk must be positive
	if c.RRFk <= 0 {
		return errors.New("HybridSearchConfig: RRFk must be positive")
	}

	// TextColumns must be specified when enabled
	if len(c.TextColumns) == 0 {
		return errors.New("HybridSearchConfig: TextColumns must be specified when enabled")
	}

	// Validate embedded BM25Config
	if err := c.BM25.Validate(); err != nil {
		return err
	}

	return nil
}
