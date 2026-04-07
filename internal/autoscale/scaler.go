package autoscale

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// ClusterHealth represents the node's operational health level.
type ClusterHealth int

const (
	HealthHealthy  ClusterHealth = iota // Normal operations
	HealthDegraded                      // High load, throttling ingestion
	HealthCritical                      // Near capacity, rejecting all load
)

func (ch ClusterHealth) String() string {
	switch ch {
	case HealthHealthy:
		return "HEALTHY"
	case HealthDegraded:
		return "DEGRADED"
	case HealthCritical:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

// LoadSnapshot captures the current system load metrics.
type LoadSnapshot struct {
	SearchQPS        float64
	SearchLatency    time.Duration
	IngestThroughput float64
	Health           ClusterHealth
	Timestamp        time.Time
}

// ScalingConfig defines the parameters for dynamic resource adjustment.
type ScalingConfig struct {
	MinIndexingWorkers int
	MaxIndexingWorkers int
	MinIngestionWorkers int
	MaxIngestionWorkers int

	TargetQPSPerWorker float64
	ScaleUpThreshold     float64 // e.g. 0.8 (80% load)
	ScaleDownThreshold   float64 // e.g. 0.3 (30% load)
}

// Reconciler is an interface for components that can resize their worker pools.
type Reconciler interface {
	AdjustWorkerCounts(indexing, ingestion int)
}

// AutoScaler monitors system metrics and provides load signals.
type AutoScaler struct {
	logger zerolog.Logger
	reconciler Reconciler

	// Scaling state
	config ScalingConfig
	lastReconcile time.Time
	cooldown      time.Duration

	// Atomic counters for raw events
	searchCount     atomic.Int64
	ingestCount     atomic.Int64
	totalLatencyNs atomic.Int64

	// Sliding windows for derived metrics
	searchWindow *RollingWindow
	ingestWindow *RollingWindow

	// Configuration
	monitorInterval time.Duration
}

// NewAutoScaler creates a new AutoScaler instance.
func NewAutoScaler(logger zerolog.Logger) *AutoScaler {
	return &AutoScaler{
		logger:          logger.With().Str("component", "auto-scaler").Logger(),
		monitorInterval: 5 * time.Second,
		searchWindow:    NewRollingWindow(time.Second, 60), // 60-second window, 1s buckets
		ingestWindow:    NewRollingWindow(time.Second, 60),
		cooldown:        30 * time.Second,
		config: ScalingConfig{
			MinIndexingWorkers:  1,
			MaxIndexingWorkers:  16, // Default cap
			MinIngestionWorkers: 1,
			MaxIngestionWorkers: 16,
			TargetQPSPerWorker:  50.0,
			ScaleUpThreshold:    0.8,
			ScaleDownThreshold:  0.2,
		},
	}
}

// SetReconciler registers a reconciler for scaling actions.
func (as *AutoScaler) SetReconciler(r Reconciler) {
	as.reconciler = r
}

// RecordSearch registers a search operation.
func (as *AutoScaler) RecordSearch(latency time.Duration) {
	as.searchCount.Add(1)
	as.totalLatencyNs.Add(int64(latency))
}

// RecordIngest registers a batch of ingested vectors.
func (as *AutoScaler) RecordIngest(count int) {
	as.ingestCount.Add(int64(count))
}

// Start runs the monitoring loop.
func (as *AutoScaler) Start(ctx context.Context) {
	ticker := time.NewTicker(as.monitorInterval)
	defer ticker.Stop()

	as.logger.Info().
		Dur("interval", as.monitorInterval).
		Msg("Auto-scaler monitoring started")

	for {
		select {
		case <-ctx.Done():
			as.logger.Info().Msg("Auto-scaler monitoring stopped")
			return
		case <-ticker.C:
			as.sample()
		}
	}
}

// sample periodically updates the sliding windows.
func (as *AutoScaler) sample() {
	// Delta since last sample
	sCount := as.searchCount.Swap(0)
	iCount := as.ingestCount.Swap(0)
	// For latency, we normally want an average over the interval
	// But let's just keep searchWindow for QPS for now.

	as.searchWindow.Add(sCount)
	as.ingestWindow.Add(iCount)

	// Log load if search activity detected
	qps := float64(as.searchWindow.Sum()) / 60.0
	ingestVps := float64(as.ingestWindow.Sum()) / 60.0

	if qps > 0.1 || ingestVps > 0.1 {
		as.logger.Debug().
			Float64("qps", qps).
			Float64("ingest_vps", ingestVps).
			Msg("Current Load Report")
	}

	as.reconcile()
}

// reconcile adjust worker counts based on current load.
func (as *AutoScaler) reconcile() {
	if as.reconciler == nil {
		return
	}

	if time.Since(as.lastReconcile) < as.cooldown {
		return
	}

	snapshot := as.GetLoadSnapshot()
	
	// Indexing logic: Scale based on ingest throughput
	// Simple heuristic: 1 worker per 10,000 vectors/sec?
	// For now, let's just use a basic stair-step logic.
	targetIndexing := as.config.MinIndexingWorkers
	if snapshot.IngestThroughput > 1000 {
		targetIndexing = 4
	}
	if snapshot.IngestThroughput > 5000 {
		targetIndexing = 8
	}

	// Priority override: If Search QPS is very high relative to capacity,
	// scale indexing down to minimum to prioritize search CPU.
	if snapshot.SearchQPS > (as.config.TargetQPSPerWorker * 0.9) {
		targetIndexing = as.config.MinIndexingWorkers
	}

	// Ingestion logic (gRPC parsing/buffering)
	targetIngestion := as.config.MinIngestionWorkers
	if snapshot.IngestThroughput > 2000 {
		targetIngestion = 4
	}

	as.reconciler.AdjustWorkerCounts(targetIndexing, targetIngestion)
	as.lastReconcile = time.Now()
}

// GetLoadSnapshot returns the current load metrics.
func (as *AutoScaler) GetLoadSnapshot() LoadSnapshot {
	qps := float64(as.searchWindow.Sum()) / 60.0
	vps := float64(as.ingestWindow.Sum()) / 60.0

	// Determine health based on metrics
	health := HealthHealthy
	if qps > (as.config.TargetQPSPerWorker * float64(as.config.MaxIndexingWorkers)) {
		health = HealthCritical
	} else if qps > (as.config.TargetQPSPerWorker * 0.8) {
		health = HealthDegraded
	}

	return LoadSnapshot{
		SearchQPS:        qps,
		IngestThroughput: vps,
		Health:           health,
		Timestamp:        time.Now(),
	}
}
