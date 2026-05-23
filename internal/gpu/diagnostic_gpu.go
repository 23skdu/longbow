//go:build gpu

package gpu

import (
	"github.com/rs/zerolog"
)

// CheckBinaryDiagnostic verifies if the binary is built with GPU support.
// Since this file is built with the 'gpu' tag, it just returns true.
func CheckBinaryDiagnostic(logger *zerolog.Logger) {
	logger.Debug().Msg("Binary is built with GPU support (gpu build tag present).")
}
