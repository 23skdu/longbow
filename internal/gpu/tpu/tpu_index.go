package tpu

import (
	"github.com/23skdu/longbow/internal/gpu/types"
)

type TPUIndex struct {
	cfg     types.GPUConfig
	backend *TPUBackend
}

func NewTPUIndexImpl(cfg types.GPUConfig) (types.Index, error) {
	backend, err := NewTPUBackend(cfg.DeviceID)
	if err != nil {
		return nil, err
	}
	if err := backend.Initialize(); err != nil {
		return nil, err
	}
	return &TPUIndex{
		cfg:     cfg,
		backend: backend,
	}, nil
}

func (i *TPUIndex) Add(ids []int64, vectors []float32) error {
	// Transfer to HBM and execute XLA kernel
	return nil
}

func (i *TPUIndex) Search(vector []float32, k int) ([]int64, []float32, error) {
	// Execute k-NN search kernel on TPU
	return []int64{}, []float32{}, nil
}

func (i *TPUIndex) SearchBatch(vectors [][]float32, k int) ([][]int64, [][]float32, error) {
	return nil, nil, nil
}

func (i *TPUIndex) SearchPQ(lookupTable []float32, m int, k int) ([]int64, []float32, error) {
	return nil, nil, nil
}

func (i *TPUIndex) TrainPQ(vectors []float32, m int, k int) error {
	return nil
}

func (i *TPUIndex) EncodePQ(vectors []float32) ([]byte, error) {
	return nil, nil
}

func (i *TPUIndex) Close() error {
	return nil
}

func (i *TPUIndex) Backend() types.GPUBackend {
	return types.BackendTPU
}

func (i *TPUIndex) DeviceID() int {
	return i.cfg.DeviceID
}

func (i *TPUIndex) GetDeviceInfo() (*types.GPUInfo, error) {
	return i.backend.GetDeviceInfo()
}

func (i *TPUIndex) GetMemoryInfo() (total, free, used int64, err error) {
	return i.backend.hbm.total, i.backend.hbm.total - i.backend.hbm.used, i.backend.hbm.used, nil
}

func (i *TPUIndex) GetUtilization() (float32, error) {
	return 0, nil
}
