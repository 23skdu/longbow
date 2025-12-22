//go:build !gpu

package gpu

import "errors"

// StubIndex is a no-op implementation for builds without GPU support.
type StubIndex struct{}

var ErrGPUNotAvailable = errors.New("GPU support not enabled in this build")

func NewIndex() (Index, error) {
	return nil, ErrGPUNotAvailable
}

func NewIndexWithConfig(cfg GPUConfig) (Index, error) {
	return nil, ErrGPUNotAvailable
}
