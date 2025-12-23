package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// Gossip & Mesh Metrics
// =============================================================================

var (
	// GossipActiveMembers tracks the number of alive members in the mesh
	GossipActiveMembers = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_gossip_active_members",
			Help: "Current number of alive members in the gossip mesh",
		},
	)

	// GossipPingsTotal counts gossip pings sent and received
	GossipPingsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_gossip_pings_total",
			Help: "Total number of gossip pings",
		},
		[]string{"direction"}, // "sent", "received"
	)

	// MeshSyncDeltasTotal counts record batches replicated via mesh sync
	MeshSyncDeltasTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_mesh_sync_deltas_total",
			Help: "Total number of record batches replicated via mesh sync",
		},
		[]string{"status"}, // "success", "error"
	)

	// MeshSyncBytesTotal counts bytes replicated via mesh sync
	MeshSyncBytesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_mesh_sync_bytes_total",
			Help: "Total bytes replicated via mesh sync",
		},
	)

	// MeshMerkleMatchTotal counts Merkle root comparison results
	MeshMerkleMatchTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_mesh_merkle_match_total",
			Help: "Total Merkle root comparison results",
		},
		[]string{"result"}, // "match", "mismatch"
	)

	// PeerHealthStatus tracks peer health (0=down, 1=up)
	PeerHealthStatus = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_peer_health_status",
			Help: "Peer health status (0=down, 1=up)",
		},
		[]string{"peer"},
	)
)

// =============================================================================
// Replication Metrics
// =============================================================================

var (
	// ReplicationPeersTotal = promauto.NewGauge
	ReplicationPeersTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_replication_peers_total",
			Help: "Total number of replication peers",
		},
	)
	ReplicationFailuresTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_replication_failures_total",
			Help: "Total number of replication failures",
		},
	)
	ReplicationSuccessTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_replication_success_total",
			Help: "Total number of successful replication operations",
		},
	)
	ReplicationRetriesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_replication_retries_total",
			Help: "Total number of replication retries",
		},
	)
	ReplicationQueuedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_replication_queued_total",
			Help: "Total number of operations queued for replication",
		},
	)
	ReplicationQueueDropped = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_replication_queue_dropped_total",
			Help: "Total number of operations dropped from replication queue",
		},
	)
	ReplicationLagSeconds = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_replication_lag_seconds",
			Help: "Replication lag in seconds by peer",
		},
		[]string{"peer"},
	)
)

// =============================================================================
// Split Brain & Quorum Metrics
// =============================================================================

var (
	SplitBrainHeartbeatsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_split_brain_heartbeats_total",
			Help: "Total number of split brain detector heartbeats",
		},
	)
	SplitBrainHealthyPeers = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_split_brain_healthy_peers",
			Help: "Current number of healthy peers seen by detector",
		},
	)
	SplitBrainPartitionsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_split_brain_partitions_total",
			Help: "Total number of partition events detected",
		},
	)
	SplitBrainFenced = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_split_brain_fenced_state",
			Help: "Whether the node is currently fenced (1=fenced, 0=normal)",
		},
	)

	QuorumOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_quorum_operation_duration_seconds",
			Help:    "Duration of quorum operations",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		},
		[]string{"operation", "consistency"},
	)
	QuorumSuccessTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_quorum_success_total",
			Help: "Total number of successful quorum operations",
		},
		[]string{"operation", "consistency"},
	)
	QuorumFailureTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_quorum_failure_total",
			Help: "Total number of failed quorum operations",
		},
		[]string{"operation", "consistency", "reason"},
	)
)

// =============================================================================
// Load Balancer & Exchange Metrics
// =============================================================================

var (
	LoadBalancerReplicasTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_load_balancer_replicas_total",
			Help: "Total number of replicas tracked by load balancer",
		},
	)
	LoadBalancerUnhealthyTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_load_balancer_unhealthy_total",
			Help: "Total number of unhealthy replicas",
		},
	)
	LoadBalancerSelectionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_load_balancer_selections_total",
			Help: "Total number of replica selections for read operations",
		},
		[]string{"strategy"},
	)

	DoExchangeCallsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_do_exchange_calls_total",
			Help: "Total number of DoExchange (gossip) calls",
		},
	)

	DoExchangeErrorsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_do_exchange_errors_total",
			Help: "Total number of failed DoExchange (gossip) calls",
		},
	)

	DoExchangeBatchesReceivedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_do_exchange_batches_received_total",
			Help: "Total number of record batches received via DoExchange",
		},
	)

	DoExchangeBatchesSentTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_do_exchange_batches_sent_total",
			Help: "Total number of record batches sent via DoExchange",
		},
	)

	DoExchangeDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_do_exchange_duration_seconds",
			Help:    "Latency of DoExchange (gossip) operations",
			Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1, 5, 10},
		},
	)

	// ProxyRequestsForwardedTotal counts requests forwarded to other nodes
	ProxyRequestsForwardedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_proxy_requests_forwarded_total",
			Help: "Total number of requests forwarded to other nodes",
		},
		[]string{"method", "status"},
	)

	// ProxyRequestLatencySeconds measures latency of forwarded requests
	ProxyRequestLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_proxy_request_latency_seconds",
			Help:    "Latency of forwarded requests",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)
)

// =============================================================================
// Global Search & Vector Clock
// =============================================================================

var (
	GlobalSearchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "longbow_global_search_duration_seconds",
		Help:    "Latency of global search operations",
		Buckets: prometheus.DefBuckets,
	})
	GlobalSearchFanoutSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "longbow_global_search_fanout_size",
		Help:    "Number of peers queried during global search",
		Buckets: []float64{1, 2, 3, 5, 10, 20, 50, 100},
	})
	GlobalSearchPartialFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_global_search_partial_failures_total",
		Help: "Total number of failed peer queries during global search",
	})
	IDResolutionDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "longbow_id_resolution_duration_seconds",
		Help:    "Latency of resolving internal IDs to user IDs",
		Buckets: prometheus.DefBuckets,
	})

	VectorClockMergesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_vector_clock_merges_total",
			Help: "Total number of vector clock merges",
		},
	)
	VectorClockConflictsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_vector_clock_conflicts_total",
			Help: "Total number of vector clock conflicts detected",
		},
	)

	// NUMACrossNodeAccessTotal counts memory accesses across NUMA nodes
	NUMACrossNodeAccessTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_numa_cross_node_access_total",
			Help: "Total number of memory accesses where worker node != data node",
		},
		[]string{"worker_node", "data_node"},
	)

	NumaWorkerDistribution = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_numa_worker_distribution",
			Help: "Number of workers pinned to each NUMA node",
		},
		[]string{"node"},
	)
)
