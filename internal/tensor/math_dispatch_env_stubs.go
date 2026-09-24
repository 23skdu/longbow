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

// MinEMLVectorCount mirrors the emlgo build threshold (kept in sync for tests).
const MinEMLVectorCount = 50000

var dispatchMode DispatchMode = DispatchAuto

// GetDispatchMode returns the current dispatch mode.
func GetDispatchMode() DispatchMode {
	return dispatchMode
}

// SetDispatchMode sets the dispatch mode. Without the emlgo build tag this
// only records the mode; the math backend is always standard.
func SetDispatchMode(m DispatchMode) {
	dispatchMode = m
	if m == DispatchStandard || m == DispatchEML || m == DispatchAuto {
		mathutil.SetBackend(mathutil.BackendStandard)
		SetMathImpl(MathSIMD)
	}
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
		SetDispatchMode(ParseDispatchMode(val))
	}
}

func init() {
	ApplyDispatchConfig()
}
