//go:build linux

package iouring

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Latency metrics
	IoUringSubmitLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_iouring_submit_latency_seconds",
			Help:    "Latency of io_uring submission operations",
			Buckets: []float64{0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1},
		},
		[]string{"operation"}, // nop, read, write, fsync, vectored
	)

	IoUringCompleteLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_iouring_complete_latency_seconds",
			Help:    "Latency of io_uring completion operations",
			Buckets: []float64{0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1},
		},
		[]string{"operation"},
	)

	// Queue depth metrics
	IoUringSQDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_iouring_sq_depth",
			Help: "Current submission queue depth (pending entries)",
		},
	)

	IoUringCQDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_iouring_cq_depth",
			Help: "Current completion queue depth (available completions)",
		},
	)

	IoUringSQCapacity = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_iouring_sq_capacity",
			Help: "Total submission queue capacity",
		},
	)

	IoUringCQCapacity = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_iouring_cq_capacity",
			Help: "Total completion queue capacity",
		},
	)

	// Operation counters
	IoUringOpsSubmitted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_iouring_ops_submitted_total",
			Help: "Total number of operations submitted",
		},
		[]string{"operation"}, // read, write, fsync, vectored, nop
	)

	IoUringOpsCompleted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_iouring_ops_completed_total",
			Help: "Total number of operations completed",
		},
		[]string{"operation", "status"}, // status: success, error
	)

	IoUringErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_iouring_errors_total",
			Help: "Total number of io_uring errors",
		},
		[]string{"type"}, // submit_failed, complete_failed, sq_full, cq_overflow
	)

	// Throughput metrics
	IoUringBytesRead = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_iouring_bytes_read_total",
			Help: "Total bytes read via io_uring",
		},
	)

	IoUringBytesWritten = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_iouring_bytes_written_total",
			Help: "Total bytes written via io_uring",
		},
	)

	// Buffer pool metrics
	IoUringBufferPoolTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_iouring_buffer_pool_total",
			Help: "Total number of buffers in pool",
		},
	)

	IoUringBufferPoolAvailable = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_iouring_buffer_pool_available",
			Help: "Number of available buffers in pool",
		},
	)

	IoUringBufferPoolAllocated = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_iouring_buffer_pool_allocated",
			Help: "Number of currently allocated buffers",
		},
	)

	IoUringBufferPoolHits = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_iouring_buffer_pool_hits_total",
			Help: "Total buffer pool hits (buffer reused)",
		},
	)

	IoUringBufferPoolMisses = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_iouring_buffer_pool_misses_total",
			Help: "Total buffer pool misses (allocation required)",
		},
	)

	// Ring state metrics
	IoUringRingActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_iouring_ring_active",
			Help: "Whether the io_uring ring is active (1) or closed (0)",
		},
	)

	// Vectored I/O metrics
	IoUringVectoredChunks = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_iouring_vectored_chunks",
			Help:    "Number of chunks in vectored operations",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128},
		},
	)

	// Arrow-specific metrics
	IoUringArrowRecordsWritten = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_iouring_arrow_records_written_total",
			Help: "Total Arrow records written",
		},
	)

	IoUringArrowRecordsRead = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_iouring_arrow_records_read_total",
			Help: "Total Arrow records read",
		},
	)

	IoUringArrowSerializationLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_iouring_arrow_serialization_latency_seconds",
			Help:    "Arrow IPC serialization latency",
			Buckets: []float64{0.000001, 0.00001, 0.0001, 0.001, 0.01},
		},
	)
)

// RingMetrics wraps a Ring with metrics collection
type RingMetrics struct {
	ring *Ring
}

// NewRingMetrics creates a new RingMetrics wrapper
func NewRingMetrics(ring *Ring) *RingMetrics {
	// Set initial capacity metrics
	IoUringSQCapacity.Set(float64(ring.params.SqEntries))
	IoUringCQCapacity.Set(float64(ring.params.CqEntries))
	IoUringRingActive.Set(1)

	return &RingMetrics{ring: ring}
}

// Update updates queue depth metrics
func (m *RingMetrics) Update() {
	if m.ring == nil {
		return
	}

	IoUringSQDepth.Set(float64(m.ring.SqReady()))
	IoUringCQDepth.Set(float64(m.ring.CqReady()))
}

// Close marks the ring as inactive
func (m *RingMetrics) Close() {
	IoUringRingActive.Set(0)
}

// BufferPoolMetrics wraps a BufferPool with metrics collection
type BufferPoolMetrics struct {
	pool *BufferPool
}

// NewBufferPoolMetrics creates a new BufferPoolMetrics wrapper
func NewBufferPoolMetrics(pool *BufferPool) *BufferPoolMetrics {
	return &BufferPoolMetrics{pool: pool}
}

// Update updates buffer pool metrics
func (m *BufferPoolMetrics) Update() {
	if m.pool == nil {
		return
	}

	available, allocated := m.pool.Stats()
	IoUringBufferPoolAvailable.Set(float64(available))
	IoUringBufferPoolAllocated.Set(float64(allocated))
}

// RecordHit records a buffer pool hit
func RecordBufferPoolHit() {
	IoUringBufferPoolHits.Inc()
}

// RecordMiss records a buffer pool miss
func RecordBufferPoolMiss() {
	IoUringBufferPoolMisses.Inc()
}
