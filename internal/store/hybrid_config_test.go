package store


import (
	"testing"
)

// =============================================================================
// Subtask 3: HybridSearchConfig Tests (TDD)
// =============================================================================

func TestDefaultHybridSearchConfig(t *testing.T) {
	cfg := DefaultHybridSearchConfig()

	// Alpha should default to 0.5 (balanced)
	if cfg.Alpha != 0.5 {
		t.Errorf("expected Alpha=0.5, got %f", cfg.Alpha)
	}

	// RRF K should have sensible default
	if cfg.RRFk != 60 {
		t.Errorf("expected RRFk=60, got %d", cfg.RRFk)
	}

	// Should not be enabled by default (opt-in)
	if cfg.Enabled {
		t.Error("expected Enabled=false by default")
	}

	// TextColumns should be empty by default
	if len(cfg.TextColumns) != 0 {
		t.Errorf("expected empty TextColumns, got %v", cfg.TextColumns)
	}

	// BM25Config should have valid defaults
	if cfg.BM25.K1 != 1.2 {
		t.Errorf("expected BM25.K1=1.2, got %f", cfg.BM25.K1)
	}
	if cfg.BM25.B != 0.75 {
		t.Errorf("expected BM25.B=0.75, got %f", cfg.BM25.B)
	}
}

func TestHybridSearchConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*HybridSearchConfig)
		wantErr bool
	}{
		{
			name:    "valid default config",
			modify:  func(c *HybridSearchConfig) { c.Enabled = true; c.TextColumns = []string{"text"} },
			wantErr: false,
		},
		{
			name:    "disabled config always valid",
			modify:  func(c *HybridSearchConfig) { c.Enabled = false },
			wantErr: false,
		},
		{
			name:    "alpha below 0",
			modify:  func(c *HybridSearchConfig) { c.Enabled = true; c.TextColumns = []string{"text"}; c.Alpha = -0.1 },
			wantErr: true,
		},
		{
			name:    "alpha above 1",
			modify:  func(c *HybridSearchConfig) { c.Enabled = true; c.TextColumns = []string{"text"}; c.Alpha = 1.1 },
			wantErr: true,
		},
		{
			name:    "alpha exactly 0 valid",
			modify:  func(c *HybridSearchConfig) { c.Enabled = true; c.TextColumns = []string{"text"}; c.Alpha = 0.0 },
			wantErr: false,
		},
		{
			name:    "alpha exactly 1 valid",
			modify:  func(c *HybridSearchConfig) { c.Enabled = true; c.TextColumns = []string{"text"}; c.Alpha = 1.0 },
			wantErr: false,
		},
		{
			name:    "RRFk zero invalid",
			modify:  func(c *HybridSearchConfig) { c.Enabled = true; c.TextColumns = []string{"text"}; c.RRFk = 0 },
			wantErr: true,
		},
		{
			name:    "RRFk negative invalid",
			modify:  func(c *HybridSearchConfig) { c.Enabled = true; c.TextColumns = []string{"text"}; c.RRFk = -10 },
			wantErr: true,
		},
		{
			name:    "enabled without text columns invalid",
			modify:  func(c *HybridSearchConfig) { c.Enabled = true; c.TextColumns = nil },
			wantErr: true,
		},
		{
			name:    "enabled with empty text columns invalid",
			modify:  func(c *HybridSearchConfig) { c.Enabled = true; c.TextColumns = []string{} },
			wantErr: true,
		},
		{
			name:    "BM25 K1 zero is valid",
			modify:  func(c *HybridSearchConfig) { c.Enabled = true; c.TextColumns = []string{"text"}; c.BM25.K1 = 0 },
			wantErr: false,
		},
		{
			name:    "BM25 K1 negative invalid",
			modify:  func(c *HybridSearchConfig) { c.Enabled = true; c.TextColumns = []string{"text"}; c.BM25.K1 = -1.0 },
			wantErr: true,
		},
		{
			name:    "BM25 B negative invalid",
			modify:  func(c *HybridSearchConfig) { c.Enabled = true; c.TextColumns = []string{"text"}; c.BM25.B = -0.1 },
			wantErr: true,
		},
		{
			name:    "BM25 B above 1 invalid",
			modify:  func(c *HybridSearchConfig) { c.Enabled = true; c.TextColumns = []string{"text"}; c.BM25.B = 1.1 },
			wantErr: true,
		},
		{
			name:    "multiple text columns valid",
			modify:  func(c *HybridSearchConfig) { c.Enabled = true; c.TextColumns = []string{"title", "body", "tags"} },
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultHybridSearchConfig()
			tt.modify(&cfg)
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHybridSearchConfigWithBM25Config(t *testing.T) {
	// Test that HybridSearchConfig embeds BM25Config properly
	cfg := HybridSearchConfig{
		Alpha:       0.7,
		TextColumns: []string{"content"},
		RRFk:        100,
		BM25:        BM25Config{K1: 1.5, B: 0.8},
		Enabled:     true,
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("expected valid config, got error: %v", err)
	}

	// Verify all fields set correctly
	if cfg.Alpha != 0.7 {
		t.Errorf("expected Alpha=0.7, got %f", cfg.Alpha)
	}
	if cfg.BM25.K1 != 1.5 {
		t.Errorf("expected BM25.K1=1.5, got %f", cfg.BM25.K1)
	}
	if cfg.BM25.B != 0.8 {
		t.Errorf("expected BM25.B=0.8, got %f", cfg.BM25.B)
	}
	if cfg.RRFk != 100 {
		t.Errorf("expected RRFk=100, got %d", cfg.RRFk)
	}
}

func TestHybridSearchConfigAlphaSemantics(t *testing.T) {
	// Alpha = 1.0 means pure vector search
	// Alpha = 0.0 means pure keyword search
	// Alpha = 0.5 means balanced hybrid

	tests := []struct {
		alpha       float32
		pureVector  bool
		pureKeyword bool
	}{
		{alpha: 1.0, pureVector: true, pureKeyword: false},
		{alpha: 0.0, pureVector: false, pureKeyword: true},
		{alpha: 0.5, pureVector: false, pureKeyword: false},
		{alpha: 0.7, pureVector: false, pureKeyword: false},
	}

	for _, tt := range tests {
		cfg := DefaultHybridSearchConfig()
		cfg.Alpha = tt.alpha
		cfg.Enabled = true
		cfg.TextColumns = []string{"text"}

		if cfg.IsPureVector() != tt.pureVector {
			t.Errorf("Alpha=%f: IsPureVector()=%v, want %v", tt.alpha, cfg.IsPureVector(), tt.pureVector)
		}
		if cfg.IsPureKeyword() != tt.pureKeyword {
			t.Errorf("Alpha=%f: IsPureKeyword()=%v, want %v", tt.alpha, cfg.IsPureKeyword(), tt.pureKeyword)
		}
	}
}
