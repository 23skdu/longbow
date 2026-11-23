package storeerrors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewConfigError(t *testing.T) {
	err := NewConfigError("FlightDataQueueConfig", "QueueSize", "0", "must be > 0")
	require.Error(t, err)

	var configErr *ConfigError
	require.True(t, errors.As(err, &configErr))
	require.Equal(t, "FlightDataQueueConfig", configErr.Component)
	require.Equal(t, "QueueSize", configErr.Field)
	require.Equal(t, "0", configErr.Value)
	require.Equal(t, "must be > 0", configErr.Message)
	require.False(t, configErr.Timestamp.IsZero())
	require.Equal(t, "config error in FlightDataQueueConfig: QueueSize=0: must be > 0", err.Error())
}

func FuzzConfigErrorFormatting(f *testing.F) {
	f.Add("component", "field", "value", "message")
	f.Add("", "", "", "")
	f.Add("queue", "size", "-1", "must be positive")

	f.Fuzz(func(t *testing.T, component, field, value, message string) {
		err := NewConfigError(component, field, value, message)
		var configErr *ConfigError
		if !errors.As(err, &configErr) {
			t.Fatal("NewConfigError must return *ConfigError")
		}
		if configErr.Timestamp.IsZero() {
			t.Fatal("ConfigError timestamp must be initialized")
		}
		want := "config error in " + component + ": " + field + "=" + value + ": " + message
		if err.Error() != want {
			t.Fatalf("Error() = %q, want %q", err.Error(), want)
		}
	})
}
