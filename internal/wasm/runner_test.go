package wasm

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRunnerInvalidPath(t *testing.T) {
	runner, err := NewRunner(context.Background(), "/nonexistent/model.wasm")
	assert.Error(t, err)
	assert.Nil(t, runner)
}

func TestRunnerCloseNil(t *testing.T) {
	var r *Runner
	assert.NotPanics(t, func() {
		if r != nil {
			r.Close(context.Background())
		}
	})
}
