//go:build !emlgo

package tensor

import (
	"os"
	"strings"

	"github.com/23skdu/longbow/internal/mathutil"
)

// DispatchMode controls which math backend is used.
type DispatchMode int

const (
	// DispatchAuto uses empirical routing rules based on data type and scale.
	DispatchAuto DispatchMode = iota
	// DispatchEML forces emlgo for all operations.
	DispatchEML
	// DispatchStandard forces standard math for all operations.
	DispatchStandard
)

var dispatchMode DispatchMode = DispatchAuto

// GetDispatchMode returns the current dispatch mode.
func GetDispatchMode() DispatchMode {
	return dispatchMode
}

// ParseDispatchMode parses a LONGBOW_MATH_DISPATCH env var value.
func ParseDispatchMode(val string) DispatchMode {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "emlgo", "eml":
		return DispatchEML
	case "standard", "std", "go":
		return DispatchStandard
	default:
		return DispatchAuto
	}
}

// ResolveBackend selects the math backend for a given data type and vector count.
// Without the emlgo build tag, all modes fall through to BackendStandard.
func ResolveBackend(typeName string, vectorCount int) mathutil.Backend {
	return mathutil.BackendStandard
}

// ApplyDispatchConfig reads LONGBOW_MATH_DISPATCH and configures the math backend.
// Without the emlgo build tag, this only sets the dispatch mode variable.
func ApplyDispatchConfig() {
	if val, ok := os.LookupEnv("LONGBOW_MATH_DISPATCH"); ok {
		dispatchMode = ParseDispatchMode(val)
	}
}

func init() {
	ApplyDispatchConfig()
}
