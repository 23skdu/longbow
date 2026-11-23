package index

import (
	"github.com/23skdu/longbow/internal/store/index/hnsw"
	"github.com/23skdu/longbow/internal/store/index/ivf"
	"github.com/23skdu/longbow/internal/store/types"
)

// hnswFactory adapts the extracted HNSW constructor to the interface that the
// IVF quantizers use for building coarse quantizers.
type hnswFactory struct{}

func (hnswFactory) CreateHNSW(cfg types.IndexConfig) (types.PluggableVectorIndex, error) {
	return hnsw.CreateHNSWIndex(cfg)
}

func init() {
	ivf.SetHNSWFactory(hnswFactory{})
}
