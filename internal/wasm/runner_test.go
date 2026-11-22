package wasm

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNewRunnerInvalidPath runs the test new runner invalid path operation.
func TestNewRunnerInvalidPath(t *testing.T) {
	runner, err := NewRunner(context.Background(), "/nonexistent/model.wasm")
	assert.Error(t, err)
	assert.Nil(t, runner)
}

// TestRunnerCloseNil runs the test runner close nil operation.
func TestRunnerCloseNil(t *testing.T) {
	var r *Runner
	assert.NotPanics(t, func() {
		if r != nil {
			_ = r.Close(context.Background())
		}
	})
}
