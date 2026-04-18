package store

import (
	"fmt"
	"time"
)

// ConfigError represents an error in configuration.
type ConfigError struct {
	Component string
	Field     string
	Value     string
	Message   string
	Timestamp time.Time
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("config error in %s: %s=%s: %s", e.Component, e.Field, e.Value, e.Message)
}

// NewConfigError creates a new ConfigError.
func NewConfigError(component, field, value, message string) error {
	return &ConfigError{
		Component: component,
		Field:     field,
		Value:     value,
		Message:   message,
		Timestamp: time.Now(),
	}
}
