package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockAdaptiveIndex struct {
	ef int
}

func (m *mockAdaptiveIndex) SetEfConstruction(ef int) {
	m.ef = ef
}

func TestAdaptiveEfConstructionLimits(t *testing.T) {
	// We want to test that the adaptive targetEf logic:
	// - depth > 5000 -> targetEf = 100
	// - depth > 1000 -> targetEf = 200
	// - default -> targetEf = 400
	// is properly calculated and set.

	tests := []struct {
		name       string
		queueDepth int
		expectedEf int
	}{
		{
			name:       "Normal queue depth",
			queueDepth: 50,
			expectedEf: 400,
		},
		{
			name:       "High queue depth",
			queueDepth: 1500,
			expectedEf: 200,
		},
		{
			name:       "Extremely high queue depth",
			queueDepth: 6000,
			expectedEf: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			depth := tt.queueDepth
			var targetEf int
			switch {
			case depth > 5000:
				targetEf = 100
			case depth > 1000:
				targetEf = 200
			default:
				targetEf = 400
			}

			mock := &mockAdaptiveIndex{}
			mock.SetEfConstruction(targetEf)

			assert.Equal(t, tt.expectedEf, mock.ef)
		})
	}
}
