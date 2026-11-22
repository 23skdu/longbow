package version

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestVersionInfo runs the test version info operation.
func TestVersionInfo(t *testing.T) {
	info := Info()
	assert.True(t, strings.HasPrefix(info, "Longbow version "))
	assert.Contains(t, info, "Commit:")
	assert.Contains(t, info, "BuildDate:")
	assert.Contains(t, info, "GoVersion:")
	assert.Contains(t, info, "OS/Arch:")
}

// TestVersionVariables runs the test version variables operation.
func TestVersionVariables(t *testing.T) {
	assert.NotEmpty(t, Version)
	assert.NotEmpty(t, Commit)
	assert.NotEmpty(t, BuildDate)
}

// TestVersionPrint runs the test version print operation.
func TestVersionPrint(t *testing.T) {
	assert.NotPanics(t, Print)
}
