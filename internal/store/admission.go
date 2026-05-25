package store

import (
	"context"
	"flag"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/autoscale"
	lbmem "github.com/23skdu/longbow/internal/memory"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AdmissionController manages request admission and resource throttling.
type AdmissionController struct {
	Bypass         bool
	maxMemory      *atomic.Int64
	currentMemory  *atomic.Int64
	hardMemory     int64 // Absolute hard limit (LONGBOW_MAX_MEMORY_HARD)
	scaler         *autoscale.AutoScaler
	migratingCount atomic.Int32
	logger         zerolog.Logger
	tuner          *lbmem.GCTuner

	activeQueries atomic.Int32
	walReplaying  atomic.Bool
	querySem      chan struct{}

	// Migration thresholds
	maxSearchLatency    time.Duration
	maxIngestThroughput float64
}

// NewAdmissionController creates a new admission controller.
func NewAdmissionController(maxMemory, currentMemory *atomic.Int64, scaler *autoscale.AutoScaler, logger zerolog.Logger) *AdmissionController {
	hardMem := int64(0)
	if v := os.Getenv("LONGBOW_MAX_MEMORY_HARD"); v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil && parsed > 0 {
			hardMem = parsed
		}
	}
	return &AdmissionController{
		Bypass:              isTestMode(),
		maxMemory:           maxMemory,
		currentMemory:       currentMemory,
		hardMemory:          hardMem,
		scaler:              scaler,
		logger:              logger,
		maxSearchLatency:    500 * time.Millisecond,
		maxIngestThroughput: 150000,                 // Updated for 1M scale target
		querySem:            make(chan struct{}, 2), // Cap query concurrency to 2 during sharding/WAL
	}
}

// SetTuner associates a GCTuner with the admission controller.
func (ac *AdmissionController) SetTuner(tuner *lbmem.GCTuner) {
	ac.tuner = tuner
}

// MigrationStarted increments the migrating count.
func (ac *AdmissionController) MigrationStarted() {
	ac.migratingCount.Add(1)
}

// MigrationFinished decrements the migrating count.
func (ac *AdmissionController) MigrationFinished() {
	ac.migratingCount.Add(-1)
}

// SetWALReplay flags whether WAL replay is in progress.
func (ac *AdmissionController) SetWALReplay(active bool) {
	ac.walReplaying.Store(active)
}

// IsWALReplay returns true if WAL replay is active.
func (ac *AdmissionController) IsWALReplay() bool {
	return ac.walReplaying.Load()
}

// Release releases slots and updates request counters when operations finish.
func (ac *AdmissionController) Release(opType string) {
	if opType == "search" || opType == "query" {
		ac.activeQueries.Add(-1)
		select {
		case <-ac.querySem:
		default:
		}
	}
}

// AdmitMigration checks if a background migration/rebalancing task should proceed.
func (ac *AdmissionController) AdmitMigration(ctx context.Context) error {
	if ac.Bypass {
		return nil
	}
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

	// 95% Rule for Memory Pressure specifically for migration to prevent sharding deadlocks
	maxMem := ac.maxMemory.Load()
	if maxMem > 0 {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		heapMem := int64(m.HeapAlloc) // #nosec G115
		offHeapMem := lbmem.GetGlobalOffHeapAllocated()
		physicalMem := heapMem + offHeapMem
		usage := float64(physicalMem) / float64(maxMem)
		if usage > 0.95 {
			return status.Errorf(codes.ResourceExhausted, "migration throttled: memory usage (%.1f%%) exceeds 95%% background threshold", usage*100)
		}
	}

	return nil
}

// Admit checks if a request of the given type should be admitted.
func (ac *AdmissionController) Admit(ctx context.Context, opType string) error {
	if ac.Bypass {
		return nil
	}
	if opType == "search" || opType == "query" {
		if ac.walReplaying.Load() || ac.migratingCount.Load() > 0 {
			// Search throttling active!
			// Try to acquire slot from querySem
			select {
			case ac.querySem <- struct{}{}:
				// Slot acquired!
				ac.activeQueries.Add(1)
			default:
				// No slot available immediately. Let's wait up to 50ms
				select {
				case ac.querySem <- struct{}{}:
					ac.activeQueries.Add(1)
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(50 * time.Millisecond):
					return status.Errorf(codes.ResourceExhausted, "search throttled during hot WAL replay / sharding phase to prioritize ingestion")
				}
			}
		} else {
			// Normal operation
			ac.activeQueries.Add(1)
		}
	}

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

	// 3. Get Off-Heap Arena Usage (using TotalCapacity for actual footprint)
	offHeapMem := lbmem.GetGlobalOffHeapAllocated()

	// Use the maximum of manual tracking and actual physical usage (Heap + Off-Heap)
	effectiveMem := currMem
	physicalMem := heapMem + offHeapMem
	if physicalMem > effectiveMem {
		effectiveMem = physicalMem
	}

	memoryUsage := float64(effectiveMem) / float64(maxMem)
	if ac.tuner != nil {
		ratio := ac.tuner.GetUtilizationRatio()
		if ratio > memoryUsage {
			memoryUsage = ratio
		}
	}

	// Check against absolute hard memory limit (LONGBOW_MAX_MEMORY_HARD)
	if ac.hardMemory > 0 && effectiveMem > ac.hardMemory {
		// Hard absolute limit breached - reject immediately
		if opType != "maintenance" && opType != "delete" && opType != "drop" {
			ac.logger.Warn().
				Int64("effective_bytes", effectiveMem).
				Int64("hard_limit_bytes", ac.hardMemory).
				Str("op_type", opType).
				Msg("Request rejected: hard memory limit breached")
			return status.Errorf(codes.ResourceExhausted, "hard memory limit breached (%d bytes): request rejected", effectiveMem)
		}
	}

	// Migration-aware thresholds: Apply tighter limits if any index is currently migrating
	// as migration is a high-memory, non-interruptible background process.
	hardLimit := 0.94
	ingestLimit := 0.90
	if ac.migratingCount.Load() > 0 {
		hardLimit = 0.88   // Tighter limit during migration (88%)
		ingestLimit = 0.85 // Tighter ingest limit during migration (85%)
	}

	// Adaptive Memory Backpressure: as memory usage approaches the hard limit
	// (between 80% and 95%), inject exponentially scaling sleep delays
	// (5ms to 100ms) on ingestion threads to allow eviction and compaction
	// workers to free memory.
	if opType == "ingest" && memoryUsage > 0.80 && memoryUsage < hardLimit {
		p := (memoryUsage - 0.80) / (0.95 - 0.80)
		if p > 1.0 {
			p = 1.0
		}
		// Exponential scaling: p^2 * 95ms + 5ms base = 5ms at 80%, 100ms at 95%
		delay := time.Duration(p*p*float64(95*time.Millisecond) + float64(5*time.Millisecond))
		time.Sleep(delay)
	}

	// Hard Limit
	if memoryUsage > hardLimit {
		// Proactively run GC and release unused OS memory to see if we can bring memory down
		runtime.GC()
		debug.FreeOSMemory()

		// Recalculate physical and effective memory usage
		currMem = ac.currentMemory.Load()
		runtime.ReadMemStats(&m)
		heapMem = int64(m.HeapAlloc) // #nosec G115
		offHeapMemRecalc := offHeapMem
		physicalMem = heapMem + offHeapMemRecalc
		if physicalMem > currMem {
			effectiveMem = physicalMem
		} else {
			effectiveMem = currMem
		}
		memoryUsage = float64(effectiveMem) / float64(maxMem)
		if ac.tuner != nil {
			ratio := ac.tuner.GetUtilizationRatio()
			if ratio > memoryUsage {
				memoryUsage = ratio
			}
		}

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

func isTestMode() bool {
	if flag.Lookup("test.v") != nil {
		return true
	}
	if os.Getenv("LONGBOW_IN_TEST") == "true" || os.Getenv("LONGBOW_TEST") == "true" {
		return true
	}
	for _, arg := range os.Args {
		if strings.HasPrefix(arg, "-test.") {
			return true
		}
	}
	return strings.HasSuffix(os.Args[0], ".test") || strings.Contains(os.Args[0], "/_test/") || strings.Contains(os.Args[0], "/Temp/go-build")
}
