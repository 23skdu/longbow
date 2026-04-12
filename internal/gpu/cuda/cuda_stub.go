//go:build !gpu || !linux

package cuda

import (
	"fmt"
	"github.com/23skdu/longbow/internal/gpu/types"
)

func NewCUDAIndexImpl(cfg types.GPUConfig) (types.Index, error) {
	return nil, fmt.Errorf("CUDA index not supported on this platform: build with -tags gpu,linux")
}
