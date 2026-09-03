//go:build !gpu || !linux

package cuda

import (
	"fmt"
	"github.com/23skdu/longbow/internal/gpu/types"
)

func NewCUDAIndexImpl(cfg types.GPUConfig) (types.Index, error) {
	return nil, fmt.Errorf("CUDA index not supported on this platform: build with -tags gpu,linux")
}

// MemcpyKind defines direction of CUDA memory copy
type MemcpyKind int

const (
	MemcpyHostToHost     MemcpyKind = 0
	MemcpyHostToDevice   MemcpyKind = 1
	MemcpyDeviceToHost   MemcpyKind = 2
	MemcpyDeviceToDevice MemcpyKind = 3
)

func HostAlloc(size int64) (any, error) {
	return nil, fmt.Errorf("CUDA not supported on this platform")
}

func FreeHost(ptr any) error {
	return nil
}

func MemcpyAsync(dst, src any, size int64, kind MemcpyKind, stream any) error {
	return fmt.Errorf("CUDA not supported on this platform")
}

func StreamSynchronize(stream any) error {
	return fmt.Errorf("CUDA not supported on this platform")
}

type PinnedHostPool struct{}

func NewPinnedHostPool() *PinnedHostPool {
	return &PinnedHostPool{}
}

func (p *PinnedHostPool) Get(size int64) (any, error) {
	return nil, fmt.Errorf("CUDA not supported on this platform")
}

func (p *PinnedHostPool) Put(ptr any, size int64) {}

func (p *PinnedHostPool) Close() error {
	return nil
}

