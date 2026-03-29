package metrics

import (
	"github.com/23skdu/longbow/internal/gpu/types"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsExporter handles GPU-specific metrics export
type MetricsExporter struct {
	registry   *prometheus.Registry
	gatherer   prometheus.Gatherer
	httpServer *http.Server
	interval   time.Duration
	stopChan   chan struct{}
}

// NewMetricsExporter creates a new GPU metrics exporter
func NewMetricsExporter(interval time.Duration) *MetricsExporter {
	return &MetricsExporter{
		interval: interval,
		stopChan: make(chan struct{}),
	}
}

// StartHTTPServer starts an HTTP server to serve GPU metrics
// This runs on a separate port from the main metrics endpoint
func (e *MetricsExporter) StartHTTPServer(addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/gpu/metrics", promhttp.Handler())
	mux.HandleFunc("/gpu/health", e.healthHandler)

	e.httpServer = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		if err := e.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("GPU metrics server error: %v\n", err)
		}
	}()

	return nil
}

// healthHandler returns GPU health status
func (e *MetricsExporter) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	backend := types.DetectGPUBackend()
	status := "healthy"
	if backend == types.BackendCPU {
		status = "cpu_fallback"
	}

	if _, err := w.Write([]byte(fmt.Sprintf(`{"status": "%s", "backend": "%s"}`, status, backend))); err != nil {
		http.Error(w, "Failed to write response", http.StatusInternalServerError)
	}
}

// Stop stops the metrics HTTP server
func (e *MetricsExporter) Stop() error {
	if e.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return e.httpServer.Shutdown(ctx)
	}
	return nil
}

// UpdateDeviceMetrics updates GPU device-level metrics
// This should be called periodically (e.g., by a background goroutine)
func UpdateDeviceMetrics(deviceID int, backend types.GPUBackend) {
	deviceLabel := fmt.Sprintf("%d", deviceID)

	// Get memory info if available
	if backend == types.BackendCUDA {
		// Note: These would require actual CUDA calls
		// For now, we set placeholders that can be updated with real values
		// when CUDA is available

		// Utilization (0-100%)
		// This would come from nvidia-smi or NVML
		GPUDeviceUtilization.WithLabelValues(deviceLabel).Set(0)

		// Temperature (Celsius)
		GPUDeviceTemperature.WithLabelValues(deviceLabel).Set(0)

		// Power usage (Watts)
		GPUDevicePowerUsage.WithLabelValues(deviceLabel).Set(0)
	}
}

// RecordGPUSearch records metrics for a GPU search operation
func RecordGPUSearch(duration time.Duration, backend string, k int) {
	GPUSearchDurationSeconds.WithLabelValues(backend).Observe(duration.Seconds())
	VectorSearchGPULatencySeconds.WithLabelValues("search").Observe(duration.Seconds())
	VectorSearchGPUOperationsTotal.WithLabelValues("search", "success").Inc()
}

// RecordGPUSearchError records metrics for a failed GPU search
func RecordGPUSearchError(errorType string) {
	VectorSearchGPUOperationsTotal.WithLabelValues("search", "error").Inc()
	GPUFallbackTotal.WithLabelValues(errorType).Inc()
}

// RecordGPUSync records metrics for a GPU sync operation
func RecordGPUSync(duration time.Duration, batchSize int) {
	GPUSyncDurationSeconds.Observe(duration.Seconds())
	GPUOperationsTotal.WithLabelValues("sync", "batch").Inc()
	GPUBatchSize.Set(float64(batchSize))
}

// RecordGPUSyncError records metrics for a failed GPU sync
func RecordGPUSyncError() {
	GPUOperationsTotal.WithLabelValues("sync", "error").Inc()
}

// RecordGPUIndexSize updates the GPU index size metric
func RecordGPUIndexSize(deviceID int, size int64) {
	deviceLabel := fmt.Sprintf("%d", deviceID)
	GPUIndexSize.WithLabelValues(deviceLabel).Set(float64(size))
}

// RecordGPUMemory updates GPU memory metrics
func RecordGPUMemory(deviceID int, total, used, free int64) {
	deviceLabel := fmt.Sprintf("%d", deviceID)
	GPUMemoryBytes.WithLabelValues(deviceLabel, "total").Set(float64(total))
	GPUMemoryBytes.WithLabelValues(deviceLabel, "used").Set(float64(used))
	GPUMemoryBytes.WithLabelValues(deviceLabel, "free").Set(float64(free))
}

// RecordGPUUtilization updates GPU utilization metric
func RecordGPUUtilization(deviceID int, utilization float64) {
	deviceLabel := fmt.Sprintf("%d", deviceID)
	GPUDeviceUtilization.WithLabelValues(deviceLabel).Set(utilization)
}

// RecordGPUTemperature updates GPU temperature metric
func RecordGPUTemperature(deviceID int, temp float64) {
	deviceLabel := fmt.Sprintf("%d", deviceID)
	GPUDeviceTemperature.WithLabelValues(deviceLabel).Set(temp)
}

// RecordGPUPower updates GPU power usage metric
func RecordGPUPower(deviceID int, power float64) {
	deviceLabel := fmt.Sprintf("%d", deviceID)
	GPUDevicePowerUsage.WithLabelValues(deviceLabel).Set(power)
}
