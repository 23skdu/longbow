package ivf

import (
	"github.com/23skdu/longbow/internal/store/index/hnsw"
)

// HNSWFactory is the construction seam between the IVF variants and the HNSW
// core. IVF code never builds a concrete HNSW index directly; the root index
// package supplies (or overrides) the factory at wiring time. Keeping the
// boundary as an interface also lets tests inject lightweight fakes.
type HNSWFactory interface {
	CreateHNSW(cfg IndexConfig) (PluggableVectorIndex, error)
}

type hnswFactoryFunc func(cfg IndexConfig) (PluggableVectorIndex, error)

// CreateHNSW implements HNSWFactory.
func (f hnswFactoryFunc) CreateHNSW(cfg IndexConfig) (PluggableVectorIndex, error) { return f(cfg) }

var activeHNSWFactory HNSWFactory = hnswFactoryFunc(func(cfg IndexConfig) (PluggableVectorIndex, error) {
	config := cfg
	if config.Dimension <= 0 {
		config.Dimension = 1
	}
	return hnsw.NewHNSWPluggableAdapter(config.Dimension, hnswConfigOf(config)), nil
})

// SetHNSWFactory installs the factory used by all IVF HNSW-backed quantizers.
// It is called once by the root index package during wiring.
func SetHNSWFactory(f HNSWFactory) {
	if f != nil {
		activeHNSWFactory = f
	}
}

// HNSWFactory returns the currently installed HNSW construction factory.
func CurrentHNSWFactory() HNSWFactory { return activeHNSWFactory }

func createHNSW(cfg IndexConfig) (PluggableVectorIndex, error) {
	return activeHNSWFactory.CreateHNSW(cfg)
}

func hnswConfigOf(cfg IndexConfig) ArrowHNSWConfig {
	var c ArrowHNSWConfig
	if cfg.HNSWConfig != nil {
		c = *cfg.HNSWConfig
	}
	return c
}
