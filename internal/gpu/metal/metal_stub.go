//go:build !gpu || !darwin || !arm64 || !cgo

package metal

import (
	"errors"

	"github.com/23skdu/longbow/internal/gpu/types"
)

// MetalIndexOptimized is a stub when Metal or CGO is unavailable.
type MetalIndexOptimized struct{}

func (m *MetalIndexOptimized) AddTurboQuant(ids []int64, tqData []byte, bits int) error {
	return errors.New("metal: not supported without cgo on darwin/arm64")
}

// NewMetalIndexOptimized is a stub when Metal or CGO is unavailable.
func NewMetalIndexOptimized(cfg types.GPUConfig) (types.Index, error) {
	return nil, errors.New("metal: not supported without cgo on darwin/arm64")
}

// NewMetalHybridIndex is a stub when Metal or CGO is unavailable.
func NewMetalHybridIndex(cfg types.GPUConfig) (types.Index, error) {
	return nil, errors.New("metal: not supported without cgo on darwin/arm64")
}
