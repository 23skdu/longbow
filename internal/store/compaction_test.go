package store

import (
	"testing"
	"time"
)

// TestDefaultCompactionConfig verifies sensible defaults
func TestDefaultCompactionConfig(t *testing.T) {
	cfg := DefaultCompactionConfig()

	if cfg.TargetBatchSize != 10000 {
		t.Errorf("expected TargetBatchSize=10000, got %d", cfg.TargetBatchSize)
	}
	if cfg.MinBatchesToCompact != 10 {
		t.Errorf("expected MinBatchesToCompact=10, got %d", cfg.MinBatchesToCompact)
	}
	if cfg.CompactionInterval != 30*time.Second {
		t.Errorf("expected CompactionInterval=30s, got %v", cfg.CompactionInterval)
	}
	if !cfg.Enabled {
		t.Error("expected Enabled=true by default")
	}
}

// TestCompactionConfigValidation tests config validation
func TestCompactionConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     CompactionConfig
		wantErr bool
	}{
		{
			name:    "valid default config",
			cfg:     DefaultCompactionConfig(),
			wantErr: false,
		},
		{
			name:    "zero target batch size",
			cfg:     CompactionConfig{Enabled: true, TargetBatchSize: 0, MinBatchesToCompact: 10, CompactionInterval: time.Second},
			wantErr: true,
		},
		{
			name:    "negative target batch size",
			cfg:     CompactionConfig{Enabled: true, TargetBatchSize: -100, MinBatchesToCompact: 10, CompactionInterval: time.Second},
			wantErr: true,
		},
		{
			name:    "zero min batches",
			cfg:     CompactionConfig{Enabled: true, TargetBatchSize: 10000, MinBatchesToCompact: 0, CompactionInterval: time.Second},
			wantErr: true,
		},
		{
			name:    "min batches = 1 is valid",
			cfg:     CompactionConfig{Enabled: true, TargetBatchSize: 10000, MinBatchesToCompact: 1, CompactionInterval: time.Second},
			wantErr: false,
		},
		{
			name:    "zero interval",
			cfg:     CompactionConfig{Enabled: true, TargetBatchSize: 10000, MinBatchesToCompact: 10, CompactionInterval: 0},
			wantErr: true,
		},
		{
			name:    "disabled config skips validation",
			cfg:     CompactionConfig{Enabled: false, TargetBatchSize: 0},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
