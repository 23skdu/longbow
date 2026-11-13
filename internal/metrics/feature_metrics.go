package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// StreamConsumedMessagesTotal counts messages consumed by streaming connectors.
	StreamConsumedMessagesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_stream_consumed_messages_total",
			Help: "Total number of messages consumed by streaming connectors",
		},
		[]string{"connector", "source", "status"},
	)

	// StreamDeduplicatedMessagesTotal counts duplicate messages skipped by sequence/idempotency tracking.
	StreamDeduplicatedMessagesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_stream_deduplicated_messages_total",
			Help: "Total number of duplicate streaming messages skipped for exactly-once delivery",
		},
		[]string{"connector"},
	)

	// StreamConsumerLag tracks current estimated consumer lag per connector and topic.
	StreamConsumerLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_stream_consumer_lag",
			Help: "Estimated consumer offset lag per connector and topic",
		},
		[]string{"connector", "topic"},
	)

	// StreamBackpressurePausesTotal counts backpressure pause events in streaming ingestion.
	StreamBackpressurePausesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_stream_backpressure_pauses_total",
			Help: "Total number of streaming backpressure pause events",
		},
		[]string{"connector"},
	)

	// StreamDLQMessagesTotal counts messages routed to the dead-letter queue.
	StreamDLQMessagesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_stream_dlq_messages_total",
			Help: "Total number of streaming messages sent to DLQ",
		},
		[]string{"connector", "reason"},
	)

	// BillingTenantQueriesTotal counts billable search/read queries per tenant.
	BillingTenantQueriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_billing_tenant_queries_total",
			Help: "Total number of queries executed per tenant",
		},
		[]string{"tenant"},
	)

	// BillingTenantVectorsTotal counts total vectors ingested per tenant.
	BillingTenantVectorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_billing_tenant_vectors_total",
			Help: "Total number of vectors ingested per tenant",
		},
		[]string{"tenant"},
	)

	// BillingTenantStorageBytes gauges current billable storage bytes per tenant.
	BillingTenantStorageBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_billing_tenant_storage_bytes",
			Help: "Current billable storage bytes held per tenant",
		},
		[]string{"tenant"},
	)

	// BillingTenantEgressBytesTotal counts egress payload bytes served per tenant.
	BillingTenantEgressBytesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_billing_tenant_egress_bytes_total",
			Help: "Total egress bytes transferred per tenant",
		},
		[]string{"tenant"},
	)

	// BillingTenantQuotaWarningsTotal counts soft quota threshold warnings triggered per tenant.
	BillingTenantQuotaWarningsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_billing_tenant_quota_warnings_total",
			Help: "Total number of soft quota warnings triggered per tenant",
		},
		[]string{"tenant", "resource"},
	)

	// BillingTenantQuotaRejectionsTotal counts hard quota rejections per tenant.
	BillingTenantQuotaRejectionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_billing_tenant_quota_rejections_total",
			Help: "Total number of hard quota rejections per tenant",
		},
		[]string{"tenant", "resource"},
	)

	// RerankCacheHitsTotal counts rerank cache hits.
	RerankCacheHitsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_rerank_cache_hits_total",
			Help: "Total number of reranking cache hits",
		},
		[]string{"strategy"},
	)

	// RerankCacheMissesTotal counts rerank cache misses.
	RerankCacheMissesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_rerank_cache_misses_total",
			Help: "Total number of reranking cache misses",
		},
		[]string{"strategy"},
	)

	// SchemaValidationRejectionsTotal counts batches rejected by schema constraints.
	SchemaValidationRejectionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_schema_validation_rejections_total",
			Help: "Total number of batches rejected due to schema constraint violations",
		},
		[]string{"dataset", "reason"},
	)

	// MaterializedViewRefreshesTotal counts materialized view refresh executions.
	MaterializedViewRefreshesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_materialized_view_refreshes_total",
			Help: "Total number of materialized view refreshes",
		},
		[]string{"view", "mode"},
	)

	// ShardRebalanceOperationsTotal counts elastic shard split/merge/migrate operations.
	ShardRebalanceOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_shard_rebalance_operations_total",
			Help: "Total number of elastic shard rebalance operations",
		},
		[]string{"operation", "status"},
	)
)
