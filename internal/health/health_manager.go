package health

import (
	"context"
	"encoding/json"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// HealthStatus represents the health status of a component
type HealthStatus string

const (
	StatusHealthy   HealthStatus = "healthy"
	StatusDegraded  HealthStatus = "degraded"
	StatusUnhealthy HealthStatus = "unhealthy"
)

// ComponentHealth represents the health of a single component
type ComponentHealth struct {
	Name        string                 `json:"name"`
	Status      HealthStatus           `json:"status"`
	Message     string                 `json:"message,omitempty"`
	LastChecked time.Time              `json:"last_checked"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// SystemHealth represents the overall system health
type SystemHealth struct {
	Status     HealthStatus                `json:"status"`
	Timestamp  time.Time                   `json:"timestamp"`
	Uptime     time.Duration               `json:"uptime"`
	Version    string                      `json:"version"`
	Components map[string]*ComponentHealth `json:"components"`
	System     *SystemInfo                 `json:"system"`
	CheckCount int64                       `json:"check_count"`
}

// SystemInfo provides system-level information
type SystemInfo struct {
	GoVersion     string    `json:"go_version"`
	NumGoroutines int       `json:"num_goroutines"`
	MemoryUsage   MemStats  `json:"memory_usage"`
	LastGC        time.Time `json:"last_gc"`
	NumGC         uint32    `json:"num_gc"`
}

// MemStats represents memory statistics
type MemStats struct {
	Alloc        uint64 `json:"alloc_bytes"`
	TotalAlloc   uint64 `json:"total_alloc_bytes"`
	Sys          uint64 `json:"sys_bytes"`
	NumGC        uint32 `json:"num_gc"`
	HeapAlloc    uint64 `json:"heap_alloc_bytes"`
	HeapSys      uint64 `json:"heap_sys_bytes"`
	HeapIdle     uint64 `json:"heap_idle_bytes"`
	HeapInuse    uint64 `json:"heap_inuse_bytes"`
	HeapReleased uint64 `json:"heap_released_bytes"`
	HeapObjects  uint64 `json:"heap_objects"`
}

// HealthChecker defines the interface for component health checks
type HealthChecker interface {
	Name() string
	Check(ctx context.Context) *ComponentHealth
}

// HealthManager manages health checks for all components
type HealthManager struct {
	startTime    time.Time
	version      string
	checkers     map[string]HealthChecker
	logger       zerolog.Logger
	tracer       trace.Tracer
	checkCounter int64
	registry     *prometheus.Registry
}

// HealthCheckMetrics tracks health check metrics
type HealthCheckMetrics struct {
	checkDuration   *prometheus.HistogramVec
	checkStatus     *prometheus.GaugeVec
	componentStatus *prometheus.GaugeVec
}

var healthMetrics *HealthCheckMetrics

func init() {
	healthMetrics = &HealthCheckMetrics{
		checkDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "health_check_duration_seconds",
				Help:    "Duration of health checks",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"component"},
		),
		checkStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "health_check_status",
				Help: "Health check status (1=healthy, 0.5=degraded, 0=unhealthy)",
			},
			[]string{"component"},
		),
		componentStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "component_health_status",
				Help: "Current component health status (1=healthy, 0.5=degraded, 0=unhealthy)",
			},
			[]string{"component"},
		),
	}
}

// NewHealthManager creates a new health manager
func NewHealthManager(version string, logger zerolog.Logger, tracer trace.Tracer) *HealthManager {
	registry := prometheus.NewRegistry()
	registry.MustRegister(healthMetrics.checkDuration)
	registry.MustRegister(healthMetrics.checkStatus)
	registry.MustRegister(healthMetrics.componentStatus)

	return &HealthManager{
		startTime: time.Now(),
		version:   version,
		checkers:  make(map[string]HealthChecker),
		logger:    logger,
		tracer:    tracer,
		registry:  registry,
	}
}

// RegisterChecker registers a health checker
func (hm *HealthManager) RegisterChecker(checker HealthChecker) {
	hm.checkers[checker.Name()] = checker
	hm.logger.Debug().Str("component", checker.Name()).Msg("Registered health checker")
}

// GetRegistry returns the prometheus registry
func (hm *HealthManager) GetRegistry() *prometheus.Registry {
	return hm.registry
}

// CheckHealth performs health checks on all registered components
func (hm *HealthManager) CheckHealth(ctx context.Context) *SystemHealth {
	ctx, span := hm.tracer.Start(ctx, "HealthManager.CheckHealth")
	defer span.End()

	atomic.AddInt64(&hm.checkCounter, 1)
	checkStart := time.Now()

	span.SetAttributes(
		attribute.String("longbow.version", hm.version),
		attribute.Int64("longbow.health.check_count", hm.checkCounter),
	)

	health := &SystemHealth{
		Status:     StatusHealthy,
		Timestamp:  time.Now(),
		Uptime:     time.Since(hm.startTime),
		Version:    hm.version,
		Components: make(map[string]*ComponentHealth),
		System:     hm.getSystemInfo(),
		CheckCount: hm.checkCounter,
	}

	// Check each component
	for _, checker := range hm.checkers {
		componentCheckStart := time.Now()
		componentHealth := checker.Check(ctx)
		duration := time.Since(componentCheckStart)

		// Record metrics
		healthMetrics.checkDuration.WithLabelValues(checker.Name()).Observe(duration.Seconds())

		var statusValue float64
		switch componentHealth.Status {
		case StatusHealthy:
			statusValue = 1.0
		case StatusDegraded:
			statusValue = 0.5
		case StatusUnhealthy:
			statusValue = 0.0
		}

		healthMetrics.checkStatus.WithLabelValues(checker.Name()).Set(statusValue)
		healthMetrics.componentStatus.WithLabelValues(checker.Name()).Set(statusValue)

		health.Components[checker.Name()] = componentHealth

		// Determine overall status
		if componentHealth.Status == StatusUnhealthy {
			health.Status = StatusUnhealthy
		} else if componentHealth.Status == StatusDegraded && health.Status == StatusHealthy {
			health.Status = StatusDegraded
		}

		span.SetAttributes(
			attribute.String("longbow.health.component."+checker.Name()+".status", string(componentHealth.Status)),
			attribute.Float64("longbow.health.component."+checker.Name()+".duration_seconds", duration.Seconds()),
		)
	}

	span.SetAttributes(
		attribute.String("longbow.health.overall_status", string(health.Status)),
		attribute.Int("longbow.health.components_checked", len(hm.checkers)),
	)

	hm.logger.Info().
		Str("overall_status", string(health.Status)).
		Int("components_checked", len(hm.checkers)).
		Int64("duration_ms", time.Since(checkStart).Milliseconds()).
		Msg("Health check completed")

	return health
}

// getSystemInfo collects system information
func (hm *HealthManager) getSystemInfo() *SystemInfo {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	lastGC := time.Time{}
	if m.LastGC > 0 {
		lastGC = time.Unix(0, int64(m.LastGC))
	}

	return &SystemInfo{
		GoVersion:     runtime.Version(),
		NumGoroutines: runtime.NumGoroutine(),
		MemoryUsage: MemStats{
			Alloc:        m.Alloc,
			TotalAlloc:   m.TotalAlloc,
			Sys:          m.Sys,
			NumGC:        m.NumGC,
			HeapAlloc:    m.HeapAlloc,
			HeapSys:      m.HeapSys,
			HeapIdle:     m.HeapIdle,
			HeapInuse:    m.HeapInuse,
			HeapReleased: m.HeapReleased,
			HeapObjects:  m.HeapObjects,
		},
		LastGC: lastGC,
		NumGC:  m.NumGC,
	}
}

// HTTPHandler returns an http handler for health checks
func (hm *HealthManager) HTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		health := hm.CheckHealth(r.Context())
		w.Header().Set("Content-Type", "application/json")
		if health.Status == StatusUnhealthy {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		if err := json.NewEncoder(w).Encode(health); err != nil {
			http.Error(w, "Failed to encode health response", http.StatusInternalServerError)
		}
	})
}
