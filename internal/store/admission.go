package store

import (
	"context"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/23skdu/longbow/internal/autoscale"
	lbmem "github.com/23skdu/longbow/internal/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AdmissionController struct {
	maxMemory     *atomic.Int64
	currentMemory *atomic.Int64
	scaler        *autoscale.AutoScaler
	migratingCount atomic.Int32
	logger         zerolog.Logger
	
	// Migration thresholds
	maxSearchLatency  time.Duration
	maxIngestThroughput float64
}

// NewAdmissionController creates a new admission controller.
func NewAdmissionController(maxMemory, currentMemory *atomic.Int64, scaler *autoscale.AutoScaler, logger zerolog.Logger) *AdmissionController {
	return &AdmissionController{
		maxMemory:     maxMemory,
		currentMemory: currentMemory,
		scaler:        scaler,
		logger:        logger,
		maxSearchLatency:  500 * time.Millisecond,
		maxIngestThroughput: 150000, // Updated for 1M scale target
	}
}

func (ac *AdmissionController) MigrationStarted() {
	ac.migratingCount.Add(1)
}

func (ac *AdmissionController) MigrationFinished() {
	ac.migratingCount.Add(-1)
}

// AdmitMigration checks if a background migration/rebalancing task should proceed.
func (ac *AdmissionController) AdmitMigration(ctx context.Context) error {
	if ac.scaler == nil {
		return nil
	}
	
	snapshot := ac.scaler.GetLoadSnapshot()
	
	// 80% Rule for Search Latency
	if snapshot.SearchLatency > time.Duration(float64(ac.maxSearchLatency)*0.8) {
		return status.Errorf(codes.ResourceExhausted, "migration throttled: search latency (%.1fms) exceeds 80%% capacity", float64(snapshot.SearchLatency.Milliseconds()))
	}
	
	// 80% Rule for Ingestion Pressure
	if snapshot.IngestThroughput > ac.maxIngestThroughput*0.8 {
		return status.Errorf(codes.ResourceExhausted, "migration throttled: ingestion throughput (%.1f vectors/s) exceeds 80%% capacity", snapshot.IngestThroughput)
	}

	// 80% Rule for Memory Pressure specifically for migration
	maxMem := ac.maxMemory.Load()
	if maxMem > 0 {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		offHeapMem := int64(0)
		for _, a := range lbmem.GetGlobalArenas() {
			offHeapMem += a.UsedBytes.Load()
		}
		usage := (float64(m.HeapAlloc) + float64(offHeapMem)) / float64(maxMem)
		if usage > 0.80 {
			return status.Errorf(codes.ResourceExhausted, "migration throttled: memory usage (%.1f%%) exceeds 80%% background threshold", usage*100)
		}
	}

	return nil
}

// Admit checks if a request of the given type should be admitted.
func (ac *AdmissionController) Admit(ctx context.Context, opType string) error {
	maxMem := ac.maxMemory.Load()
	if maxMem <= 0 {
		return nil // No limit enforced
	}

	// 1. Get Manual Estimate
	currMem := ac.currentMemory.Load()
	
	// 2. Get Actual Heap Usage
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapMem := int64(m.HeapAlloc) // #nosec G115
	
	// 3. Get Off-Heap Arena Usage
	var offHeapMem int64
	for _, a := range lbmem.GetGlobalArenas() {
		offHeapMem += a.UsedBytes.Load()
	}
	
	// Use the maximum of manual tracking and actual physical usage (Heap + Off-Heap)
	effectiveMem := currMem
	physicalMem := heapMem + offHeapMem
	if physicalMem > effectiveMem {
		effectiveMem = physicalMem
	}

	memoryUsage := float64(effectiveMem) / float64(maxMem)

	// Migration-aware thresholds: Apply tighter limits if any index is currently migrating
	// as migration is a high-memory, non-interruptible background process.
	hardLimit := 0.94
	ingestLimit := 0.90
	if ac.migratingCount.Load() > 0 {
		hardLimit = 0.88   // Tighter limit during migration (88%)
		ingestLimit = 0.85 // Tighter ingest limit during migration (85%)
	}

	// Hard Limit
	if memoryUsage > hardLimit {
		// Allow deletions and maintenance to proceed even under pressure, as they often free resources
		if opType != "maintenance" && opType != "delete" && opType != "drop" {
			ac.logger.Warn().
				Float64("usage_ratio", memoryUsage).
				Int64("effective_bytes", effectiveMem).
				Int64("max_bytes", maxMem).
				Str("op_type", opType).
				Msg("Request rejected: critical memory pressure")
			return status.Errorf(codes.ResourceExhausted, "critical memory pressure (%.1f%% usage): request rejected", memoryUsage*100)
		}
	}

	// Soft Limit for Ingestion
	if opType == "ingest" && memoryUsage > ingestLimit {
		ac.logger.Warn().
			Float64("usage_ratio", memoryUsage).
			Int64("effective_bytes", effectiveMem).
			Int64("max_bytes", maxMem).
			Str("op_type", opType).
			Msg("Ingestion throttled: high memory pressure")
		return status.Errorf(codes.ResourceExhausted, "high memory pressure (%.1f%% usage): ingestion throttled", memoryUsage*100)
	}

	// AutoScaler Health Signals
	if ac.scaler != nil {
		snapshot := ac.scaler.GetLoadSnapshot()
		if snapshot.Health == autoscale.HealthCritical {
			// In critical health, reject non-essential requests
			if opType != "maintenance" {
				ac.logger.Warn().
					Float64("search_qps", snapshot.SearchQPS).
					Str("op_type", opType).
					Msg("Request rejected: system at critical capacity")
				return status.Errorf(codes.ResourceExhausted, "system is at critical capacity (QPS: %.1f): request rejected", snapshot.SearchQPS)
			}
		}
	}

	return nil
}
