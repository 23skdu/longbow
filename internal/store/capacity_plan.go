package store

import (
	"fmt"
	"time"
)

type CapacityPlan struct {
	CurrentVectors   int64     `json:"current_vectors"`
	MaxVectors       int64     `json:"max_vectors"`
	StorageBytes     int64     `json:"storage_bytes"`
	SearchQPS        float64   `json:"search_qps"`
	IngestQPS        float64   `json:"ingest_qps"`
	AvgLatencyMs     float64   `json:"avg_latency_ms"`
	RecommendedScale string    `json:"recommended_scale"`
	Timestamp        time.Time `json:"timestamp"`
}

type AutoScaleConfig struct {
	Enabled            bool    `json:"enabled"`
	MinWorkers         int     `json:"min_workers"`
	MaxWorkers         int     `json:"max_workers"`
	TargetQPSPerWorker float64 `json:"target_qps_per_worker"`
	ScaleUpThreshold   float64 `json:"scale_up_threshold"`
	ScaleDownThreshold float64 `json:"scale_down_threshold"`
}

func (vs *VectorStore) GetCapacityPlan() (CapacityPlan, error) {
	plan := CapacityPlan{
		Timestamp: time.Now(),
	}

	dsCount := 0
	totalVectors := int64(0)
	vs.IterateDatasets(func(name string, ds *Dataset) {
		dsCount++
		ds.dataMu.RLock()
		for _, rec := range ds.Records {
			totalVectors += rec.NumRows()
		}
		ds.dataMu.RUnlock()
	})

	plan.CurrentVectors = totalVectors
	plan.MaxVectors = int64(dsCount * 1000000)

	plan.SearchQPS = vs.getSearchQPS()
	plan.IngestQPS = vs.getIngestQPS()
	plan.AvgLatencyMs = vs.getAvgLatencyMs()

	if plan.SearchQPS > 1000 || plan.AvgLatencyMs > 100 {
		plan.RecommendedScale = "scale_up"
	} else if plan.SearchQPS < 100 && plan.AvgLatencyMs < 10 {
		plan.RecommendedScale = "scale_down"
	} else {
		plan.RecommendedScale = "maintain"
	}

	return plan, nil
}

func (vs *VectorStore) GetAutoScaleConfig() AutoScaleConfig {
	vs.configMu.RLock()
	defer vs.configMu.RUnlock()

	return AutoScaleConfig{
		Enabled:            vs.autoScaleEnabled,
		MinWorkers:         vs.autoScaleMinWorkers,
		MaxWorkers:         vs.autoScaleMaxWorkers,
		TargetQPSPerWorker: vs.autoScaleTargetQPS,
		ScaleUpThreshold:   vs.autoScaleUpThreshold,
		ScaleDownThreshold: vs.autoScaleDownThreshold,
	}
}

func (vs *VectorStore) SetAutoScaleConfig(config AutoScaleConfig) error {
	if config.MinWorkers > config.MaxWorkers {
		return fmt.Errorf("min_workers cannot exceed max_workers")
	}

	vs.configMu.Lock()
	defer vs.configMu.Unlock()

	vs.autoScaleEnabled = config.Enabled
	vs.autoScaleMinWorkers = config.MinWorkers
	vs.autoScaleMaxWorkers = config.MaxWorkers
	vs.autoScaleTargetQPS = config.TargetQPSPerWorker
	vs.autoScaleUpThreshold = config.ScaleUpThreshold
	vs.autoScaleDownThreshold = config.ScaleDownThreshold

	return nil
}

func (vs *VectorStore) getSearchQPS() float64 {
	return 0.0
}

func (vs *VectorStore) getIngestQPS() float64 {
	return 0.0
}

func (vs *VectorStore) getAvgLatencyMs() float64 {
	return 0.0
}
