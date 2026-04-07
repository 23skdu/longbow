package store

import (
	"context"
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

	currMem := ac.currentMemory.Load()
	memoryUsage := float64(currMem) / float64(maxMem)

	// Hard Limit: 95% Memory Usage
	if memoryUsage > 0.95 {
		return status.Errorf(codes.ResourceExhausted, "critical memory pressure (%.1f%% usage): request rejected", memoryUsage*100)
	}

	// Soft Limit: 90% Memory Usage for Ingestion
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
