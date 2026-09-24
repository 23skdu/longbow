//go:build !darwin || !arm64 || !gpu

package metal

import "testing"

func TestMetalNonDarwin(t *testing.T) {
	// Verifies package loads on non-Darwin platforms where Metal is not supported.
}
