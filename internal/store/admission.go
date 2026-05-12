package store

import (
	"context"
	"runtime"
	"sync/atomic"

	"github.com/rs/zerolog"
	"github.com/23skdu/longbow/internal/autoscale"
	lbmem "github.com/23skdu/longbow/internal/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AdmissionController protects the system from OOM and overload.
type AdmissionController struct {
	maxMemory     *atomic.Int64
	currentMemory *atomic.Int64
	scaler        *autoscale.AutoScaler
	logger        zerolog.Logger
}

// NewAdmissionController creates a new admission controller.
func NewAdmissionController(maxMemory, currentMemory *atomic.Int64, scaler *autoscale.AutoScaler, logger zerolog.Logger) *AdmissionController {
	return &AdmissionController{
		maxMemory:     maxMemory,
		currentMemory: currentMemory,
		scaler:        scaler,
		logger:        logger,
	}
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

	// Hard Limit: 92% Memory Usage
	if memoryUsage > 0.92 {
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

	// Soft Limit: 88% Memory Usage for Ingestion (Reduced from 90%)
	if opType == "ingest" && memoryUsage > 0.88 {
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
