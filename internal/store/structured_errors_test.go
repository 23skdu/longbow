package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStructuredErrors(t *testing.T) {
	t.Run("ConfigError", func(t *testing.T) {
		err := NewConfigError("test-component", "field", "bad-value", "field must be positive")
		require.Error(t, err)

		var cfgErr *ConfigError
		require.ErrorAs(t, err, &cfgErr, "Error should be ConfigError")
		assert.Equal(t, "test-component", cfgErr.Component)
		assert.Equal(t, "field", cfgErr.Field)
		assert.Equal(t, "bad-value", cfgErr.Value)
		assert.Equal(t, "field must be positive", cfgErr.Message)
		assert.Contains(t, cfgErr.Error(), "config error in test-component")
		assert.Contains(t, cfgErr.Error(), "field=bad-value")
	})

	t.Run("ErrVectorDimensionMismatch", func(t *testing.T) {
		err := &ErrVectorDimensionMismatch{ID: 5, Expected: 128, Actual: 256}
		require.Error(t, err)
		assert.Equal(t, 5, err.ID)
		assert.Equal(t, 128, err.Expected)
		assert.Equal(t, 256, err.Actual)
		assert.Contains(t, err.Error(), "dimension mismatch")
		assert.Contains(t, err.Error(), "vector 5")
		assert.Contains(t, err.Error(), "expected 128")
		assert.Contains(t, err.Error(), "got 256")
	})

	t.Run("ErrVectorDimensionMismatchAs", func(t *testing.T) {
		err := NewConfigError("comp", "f", "v", "m")
		var cfg *ConfigError
		require.True(t, errors.As(err, &cfg))
	})

	t.Run("ErrNeighborSelectionLengthMismatch", func(t *testing.T) {
		err := &ErrNeighborSelectionLengthMismatch{DistancesLen: 10, IDsLen: 7}
		require.Error(t, err)
		assert.Equal(t, 10, err.DistancesLen)
		assert.Equal(t, 7, err.IDsLen)
		assert.Contains(t, err.Error(), "length mismatch")
	})
}
