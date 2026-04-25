package types

import (
	"os"
	"time"

	"github.com/23skdu/longbow/internal/core"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/prometheus/client_golang/prometheus"
)

func runtimeNumCPU() int {
	return 4 // Simple fallback, usually overridden
}

// IndexJob represents a job for the indexing worker
type IndexJob struct {
	DatasetName string
	Record      arrow.RecordBatch
	BatchIdx    int
	CreatedAt   time.Time
}

// RowLocation represents a physical location of a row
type RowLocation struct {
	BatchIdx int
	RowIdx   int
}

// IndexJobQueueStats tracks queue statistics.
type IndexJobQueueStats struct {
	TotalSent     uint64 // Total jobs sent
	DirectSent    uint64 // Jobs sent directly to main channel
	OverflowCount uint64 // Jobs sent to overflow buffer
	DrainedCount  uint64 // Jobs drained from overflow to main
	DroppedCount  uint64 // Jobs dropped when both buffers full
}

// IndexJobQueueConfig holds settings for the indexing job queue
type IndexJobQueueConfig struct {
	MainChannelSize    int           // Primary channel buffer size
	OverflowBufferSize int           // Secondary overflow buffer size
	DropOnOverflow     bool          // If true, drop jobs when both buffers full
	DrainInterval      time.Duration // How often to drain overflow to main channel
}

// DefaultIndexJobQueueConfig returns sensible defaults for production.
func DefaultIndexJobQueueConfig() IndexJobQueueConfig {
	return IndexJobQueueConfig{
		MainChannelSize:    10000,
		OverflowBufferSize: 50000,
		DropOnOverflow:     true,
		DrainInterval:      1 * time.Millisecond,
	}
}

// ParallelSearchConfig holds settings for parallelized vector search
type ParallelSearchConfig struct {
	Enabled      bool
	Workers      int
	Threshold    int
	MinChunkSize int
	MaxChunkSize int
}

// DefaultParallelSearchConfig returns sensible defaults for parallel search
func DefaultParallelSearchConfig() ParallelSearchConfig {
	return ParallelSearchConfig{
		Enabled:      true,
		Workers:      runtimeNumCPU(),
		Threshold:    512,
		MinChunkSize: 256,
		MaxChunkSize: 1024,
	}
}

// ArrowHNSWConfig holds configuration for ArrowHNSW index
type ArrowHNSWConfig struct {
	M              int
	MMax           int
	MMax0          int
	EfConstruction int32
	EfSearch       int32

	AdaptiveEf          bool
	AdaptiveEfMin       int
	AdaptiveEfThreshold int
	InitialCapacity     int

	Workers      int
	Quantization bool
	SQ8Enabled   bool

	UseDisk  bool
	DiskPath string

	Metric   core.DistanceMetric
	Logger   any
	DataType VectorDataType

	BQEnabled bool
	PQEnabled bool
	PQM       int
	PQK       int

	Dims                    int
	SelectionHeuristicLimit int

	ParallelSearch ParallelSearchConfig

	AdaptiveMEnabled   bool
	AdaptiveMThreshold int

	Float16Enabled         bool
	SQ8TrainingThreshold   int
	PackedAdjacencyEnabled bool
	SearchLayerSampleRate  float64

	Registerer        prometheus.Registerer
	TurboQuantEnabled bool
	TurboQuantBits    int
	LockFreeThreshold int // Layer threshold for CAS-based lock-free updates (e.g. 2)
	NUMANode          int // Target NUMA node for memory pinning (-1 for default)
}

// DefaultArrowHNSWConfig returns a configuration with sensible defaults
func DefaultArrowHNSWConfig() ArrowHNSWConfig {
	config := ArrowHNSWConfig{
		M:                       32,
		MMax:                    64,
		MMax0:                   64,
		EfConstruction:          400,
		EfSearch:                50,
		AdaptiveEf:              false,
		AdaptiveEfMin:           50,
		AdaptiveEfThreshold:     0,
		InitialCapacity:         50000,
		Workers:                 4,
		Quantization:            false,
		SQ8Enabled:              false,
		UseDisk:                 false,
		DiskPath:                "./data",
		DataType:                VectorTypeFloat32,
		SQ8TrainingThreshold:    5000,
		SearchLayerSampleRate:   0.24,
		TurboQuantEnabled:       false,
		TurboQuantBits:          8,
		SelectionHeuristicLimit: 400,
		Metric:                  core.MetricEuclidean,
		ParallelSearch:          DefaultParallelSearchConfig(),
		NUMANode:                -1,
	}

	if os.Getenv("LONGBOW_LOW_MEM") == "1" || os.Getenv("LONGBOW_LOW_MEM") == "true" {
		config.InitialCapacity = 5000
		config.M = 16
		config.MMax = 32
		config.MMax0 = 32
	}

	if os.Getenv("LONGBOW_BQ_ENABLED") == "1" || os.Getenv("LONGBOW_BQ_ENABLED") == "true" {
		config.BQEnabled = true
	}

	return config
}
