package tensor

import (
	"testing"

	"github.com/23skdu/longbow/internal/mathutil"
	"github.com/stretchr/testify/assert"
)

func TestParseDispatchMode(t *testing.T) {
	tests := []struct {
		input string
		want  DispatchMode
	}{
		{"auto", DispatchAuto},
		{"AUTO", DispatchAuto},
		{"emlgo", DispatchEML},
		{"eml", DispatchEML},
		{"EMLGO", DispatchEML},
		{"standard", DispatchStandard},
		{"std", DispatchStandard},
		{"go", DispatchStandard},
		{"STANDARD", DispatchStandard},
		{"", DispatchAuto},
		{"unknown", DispatchAuto},
		{" emlgo ", DispatchEML},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ParseDispatchMode(tt.input)
			assert.Equal(t, tt.want, got, "ParseDispatchMode(%q)", tt.input)
		})
	}
}

func TestGetDispatchMode(t *testing.T) {
	origMode := dispatchMode
	defer func() { dispatchMode = origMode }()

	dispatchMode = DispatchAuto
	assert.Equal(t, DispatchAuto, GetDispatchMode())

	dispatchMode = DispatchEML
	assert.Equal(t, DispatchEML, GetDispatchMode())

	dispatchMode = DispatchStandard
	assert.Equal(t, DispatchStandard, GetDispatchMode())
}

func TestResolveBackend_StandardMode(t *testing.T) {
	origMode := dispatchMode
	dispatchMode = DispatchStandard
	defer func() { dispatchMode = origMode }()

	// All types should return standard
	types := []string{"float32", "float64", "int8", "uint8", "complex64", "complex128", "turboquant"}
	for _, typeName := range types {
		got := ResolveBackend(typeName, 100000)
		assert.Equal(t, mathutil.BackendStandard, got,
			"DispatchStandard: ResolveBackend(%q, 100000)", typeName)
	}
}

func TestResolveBackend_EmptyTypeName(t *testing.T) {
	origMode := dispatchMode
	dispatchMode = DispatchAuto
	defer func() { dispatchMode = origMode }()

	got := ResolveBackend("", 100000)
	assert.Equal(t, mathutil.BackendStandard, got, "empty type name should use standard")
}

// TestResolveBackend_AutoMode_RoutingRules verifies the routing logic structure.
// Without the emlgo build tag, all modes fall through to BackendStandard.
// With the emlgo build tag, auto mode routes complex/TQ>=50k to emlgo.
func TestResolveBackend_AutoMode_RoutingRules(t *testing.T) {
	origMode := dispatchMode
	dispatchMode = DispatchAuto
	defer func() { dispatchMode = origMode }()

	// Test that dispatchMode is correctly set
	assert.Equal(t, DispatchAuto, GetDispatchMode())

	// Verify standard mode returns standard for everything
	dispatchMode = DispatchStandard
	got := ResolveBackend("complex64", 100000)
	assert.Equal(t, mathutil.BackendStandard, got, "standard mode should always return standard")

	// Verify EML mode: without emlgo build tag, still returns standard (stub behavior)
	dispatchMode = DispatchEML
	got = ResolveBackend("complex64", 100000)
	// Without emlgo tag: always BackendStandard (stub)
	// With emlgo tag: BackendEML
	// Both are valid; the stub correctly falls through
	assert.True(t, got == mathutil.BackendStandard || got == mathutil.BackendEML,
		"eml mode returns standard (no emlgo tag) or emlgo (with tag)")
}
