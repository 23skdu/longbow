//go:build emlgo

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

// MinEMLVectorCount is the minimum vector count before auto mode routes any
// type to emlgo. Below this, emlgo's channel-based batch workers and per-call
// IsEML checks cost more than they save (see docs/emlgo.md §11).
const MinEMLVectorCount = 50000

// complex64EMLMaxCount is the largest count where auto mode still routes
// complex64 to emlgo. At 500k, emlgo complex64 dense regressed -38%
// (docs/emlgo.md, nextsteps P0 #1); standard wins above this threshold.
const complex64EMLMaxCount = 250000

// GetDispatchMode returns the current dispatch mode.
func GetDispatchMode() DispatchMode {
	return dispatchMode
}

// SetDispatchMode sets the dispatch mode and applies it to the math backend.
func SetDispatchMode(m DispatchMode) {
	dispatchMode = m
	applyModeToBackend(m)
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
// Routing rules (empirical, docs/emlgo.md):
//   - Below MinEMLVectorCount: always standard (emlgo dispatch overhead dominates)
//   - complex64: emlgo only up to complex64EMLMaxCount (500k dense: -38%)
//   - complex128: emlgo for dense/hybrid wins (+159% at 100k); standard at 500k+
//     where sparse regresses -37% and dense P99 spikes (nextsteps P0 #2)
//   - turboquant: emlgo above MinEMLVectorCount (TQ kernels benefit)
//   - float64/int*/uint*/float16/binary: always standard (float64 emlgo
//     adds +47% memory at 500k; ints/float16 regress dense at 100k)
func ResolveBackend(typeName string, vectorCount int) mathutil.Backend {
	switch dispatchMode {
	case DispatchEML:
		return mathutil.BackendEML
	case DispatchStandard:
		return mathutil.BackendStandard
	}

	// Auto mode
	if vectorCount < MinEMLVectorCount {
		return mathutil.BackendStandard
	}

	switch typeName {
	case "complex64":
		if vectorCount <= complex64EMLMaxCount {
			return mathutil.BackendEML
		}
		return mathutil.BackendStandard
	case "complex128":
		if vectorCount >= 500000 {
			return mathutil.BackendStandard
		}
		return mathutil.BackendEML
	case "turboquant", "turboquant2", "turboquant4", "turboquant8":
		return mathutil.BackendEML
	default:
		// float32, float64, int8..int64, uint8..uint64, float16, binary
		return mathutil.BackendStandard
	}
}

// ApplyDispatchConfig reads LONGBOW_MATH_DISPATCH and configures the math backend.
// Call once at startup after CPU feature detection.
func ApplyDispatchConfig() {
	if val, ok := os.LookupEnv("LONGBOW_MATH_DISPATCH"); ok {
		SetDispatchMode(ParseDispatchMode(val))
		return
	}
	// No env var: keep package default (DispatchAuto) but ensure backend matches.
	applyModeToBackend(dispatchMode)
}

// applyModeToBackend maps a dispatch mode onto the global mathutil backend and
// the tensor math implementation function pointers.
func applyModeToBackend(m DispatchMode) {
	switch m {
	case DispatchEML:
		mathutil.SetBackend(mathutil.BackendEML)
		SetMathImpl(MathEML)
	case DispatchStandard:
		mathutil.SetBackend(mathutil.BackendStandard)
		SetMathImpl(MathSIMD)
	default:
		// Auto: leave InitMathDispatch's choice (MathEML for this build);
		// per-dtype routing happens via ResolveBackend at call sites.
	}
}

func init() {
	ApplyDispatchConfig()
}
