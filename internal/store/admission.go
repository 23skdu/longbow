package store

import (
	"context"
	"runtime"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/autoscale"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AdmissionController protects the system from OOM and overload.
type AdmissionController struct {
	maxMemory     *atomic.Int64
	currentMemory *atomic.Int64
	scaler        *autoscale.AutoScaler
}

// NewAdmissionController creates a new admission controller.
func NewAdmissionController(maxMemory, currentMemory *atomic.Int64, scaler *autoscale.AutoScaler) *AdmissionController {
	return &AdmissionController{
		maxMemory:     maxMemory,
		currentMemory: currentMemory,
		scaler:        scaler,
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
	
	// 2. Get Actual Heap Usage (more accurate for OOM protection)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapMem := int64(m.HeapAlloc) // #nosec G115
	
	// Use the maximum of manual tracking and actual heap usage
	effectiveMem := currMem
	if heapMem > effectiveMem {
		effectiveMem = heapMem
	}

	memoryUsage := float64(effectiveMem) / float64(maxMem)

	// Hard Limit: 94% Memory Usage (Reduced from 96% for safety margin)
	if memoryUsage > 0.94 {
		// Allow deletions and maintenance to proceed even under pressure, as they often free resources
		if opType != "maintenance" && opType != "delete" && opType != "drop" {
			return status.Errorf(codes.ResourceExhausted, "critical memory pressure (%.1f%% usage): request rejected", memoryUsage*100)
		}
	}

	// Soft Limit: 90% Memory Usage for Ingestion (Reduced from 92%)
	if opType == "ingest" && memoryUsage > 0.90 {
		return status.Errorf(codes.ResourceExhausted, "high memory pressure (%.1f%% usage): ingestion throttled", memoryUsage*100)
	}

	// AutoScaler Health Signals
	if ac.scaler != nil {
		snapshot := ac.scaler.GetLoadSnapshot()
		if snapshot.Health == autoscale.HealthCritical {
			// In critical health, reject non-essential requests
			if opType != "maintenance" {
				return status.Errorf(codes.ResourceExhausted, "system is at critical capacity (QPS: %.1f): request rejected", snapshot.SearchQPS)
			}
		}
	}

	return nil
}
