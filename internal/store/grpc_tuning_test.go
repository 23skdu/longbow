package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGRPCConfig_FlowControlPolicy(t *testing.T) {
	tests := []struct {
		name         string
		policy       FlowControlPolicy
		expectedSize int32
	}{
		{
			name:         "Default Policy (4MB)",
			policy:       PolicyDefault,
			expectedSize: 4 << 20,
		},
		{
			name:         "High Bandwidth Policy (16MB)",
			policy:       PolicyHighBandwidth,
			expectedSize: 16 << 20,
		},
		{
			name:         "Auto Policy (defaults to 4MB)",
			policy:       PolicyAuto,
			expectedSize: 4 << 20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultGRPCConfig()
			cfg.Policy = tt.policy
			cfg.ApplyPolicy()

			assert.Equal(t, tt.expectedSize, cfg.InitialWindowSize)
			assert.Equal(t, tt.expectedSize, cfg.InitialConnWindowSize)
		})
	}
}

func TestGRPCConfig_ValidationWithPolicy(t *testing.T) {
	tests := []struct {
		name    string
		policy  FlowControlPolicy
		wantErr bool
	}{
		{"Valid Default", PolicyDefault, false},
		{"Valid High BW", PolicyHighBandwidth, false},
		{"Valid Auto", PolicyAuto, false},
		{"Invalid Policy Low", -1, true},
		{"Invalid Policy High", 99, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultGRPCConfig()
			cfg.Policy = tt.policy
			err := cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func FuzzGRPCConfig_PolicySelection(f *testing.F) {
	f.Add(int(PolicyDefault))
	f.Add(int(PolicyHighBandwidth))
	f.Add(int(PolicyAuto))
	f.Add(-1)
	f.Add(100)

	f.Fuzz(func(t *testing.T, policyInt int) {
		policy := FlowControlPolicy(policyInt)
		cfg := DefaultGRPCConfig()
		cfg.Policy = policy

		err := cfg.Validate()
		if err == nil {
			cfg.ApplyPolicy()
			// If valid, window sizes must be one of the known values
			validWindows := map[int32]bool{
				4 << 20:  true,
				16 << 20: true,
			}
			assert.True(t, validWindows[cfg.InitialWindowSize], "Window size should be one of the expected values for valid policy")
		}
	})
}

func TestGRPCConfig_ManualOverride(t *testing.T) {
	// If a user manually sets window sizes and then calls ApplyPolicy,
	// the policy SHOULD override them (this is specified behavior for "ApplyPolicy").
	cfg := DefaultGRPCConfig()
	cfg.InitialWindowSize = 100
	cfg.Policy = PolicyHighBandwidth
	cfg.ApplyPolicy()

	assert.Equal(t, int32(16<<20), cfg.InitialWindowSize)
}
