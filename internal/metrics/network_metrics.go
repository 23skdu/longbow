package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// Flight & RPC Metrics
// =============================================================================

var (
	// FlightBytesReadTotal counts total bytes read from Arrow Flight tickets
	FlightBytesReadTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_flight_bytes_read_total",
			Help: "Total bytes read from Flight tickets",
		},
	)

	// FlightBytesWrittenTotal counts total bytes written to Arrow Flight streams
	FlightBytesWrittenTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_flight_bytes_written_total",
			Help: "Total bytes written to Flight streams",
		},
	)

	// FlightOpsTotal counts total Flight operations
	FlightOpsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_flight_ops_total",
			Help: "Total number of Flight operations",
		},
		[]string{"action", "status"},
	)

	// FlightDurationSeconds measures latency of Flight operations
	FlightDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_flight_duration_seconds",
			Help:    "Latency of Flight operations",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10},
		},
		[]string{"action"},
	)

	// FlightActiveTickets tracks currently processing/active tickets
	FlightActiveTickets = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_flight_active_tickets",
			Help: "Number of currently active Flight tickets",
		},
	)

	// FlightStreamPoolSize - Number of recycled stream writers
	FlightStreamPoolSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_flight_stream_pool_size",
			Help: "Size of the Flight stream writer pool",
		},
	)

	// FlightPoolConnectionsActive tracks active connections in the flight client pool
	FlightPoolConnectionsActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_flight_pool_connections_active",
			Help: "Number of active connections in the flight client pool",
		},
	)

	// FlightPoolWaitDuration measures time waiting for a flight client connection
	FlightPoolWaitDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_flight_pool_wait_duration_seconds",
			Help:    "Time spent waiting for a flight client connection from pool",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
	)

	// FlightPoolConnectionsCreated tracks total connections created
	FlightPoolConnectionsCreated = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_flight_pool_connections_created_total",
			Help: "Total number of flight client connections created",
		},
	)

	// FlightPoolConnectionsDestroyed tracks total connections destroyed
	FlightPoolConnectionsDestroyed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_flight_pool_connections_destroyed_total",
			Help: "Total number of flight client connections destroyed",
		},
	)

	// DoExchangeSearchDuration measures latency of DoExchange search operations
	DoExchangeSearchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "longbow_do_exchange_search_duration_seconds",
		Help:    "Latency of DoExchange search operations",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
	})
)

// =============================================================================
// gRPC Metrics
// =============================================================================

var (
	GRPCMaxHeaderListSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_grpc_max_header_list_size",
			Help: "Configured max header list size for gRPC",
		},
	)
	GRPCMaxRecvMsgSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_grpc_max_recv_msg_size",
			Help: "Configured max receive message size for gRPC",
		},
	)
	GRPCMaxSendMsgSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_grpc_max_send_msg_size",
			Help: "Configured max send message size for gRPC",
		},
	)

	GRPCCallDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_grpc_call_duration_seconds",
			Help:    "Duration of gRPC calls",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		},
		[]string{"method", "status"},
	)

	GRPCCallTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_grpc_call_total",
			Help: "Total number of gRPC calls",
		},
		[]string{"method", "status"},
	)

	GRPCMessagesSentTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_grpc_messages_sent_total",
			Help: "Total number of gRPC messages sent",
		},
		[]string{"method"},
	)

	GRPCMessagesReceivedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_grpc_messages_received_total",
			Help: "Total number of gRPC messages received",
		},
		[]string{"method"},
	)
)
