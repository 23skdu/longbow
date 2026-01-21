package store

// GoroutineAudit documents all background goroutines with their lifecycle ownership
type GoroutineAudit struct {
	// Core store goroutines
	CompactionWorker  bool // Runs compaction tasks
	LifecycleManager  bool // Manages dataset lifecycle
	EvictionTicker    bool // Handles periodic eviction
	RepairWorker      bool // Repairs HNSW consistency
	IndexingWorker    bool // Handles batch indexing
	ReplicationWorker bool // Handles data replication

	// Index-specific goroutines
	HNSWRepairWorker bool // HNSW background repair
	BM25IndexWorker  bool // BM25 background indexing
	BM25EvictWorker  bool // BM25 background eviction

	// Network goroutines
	GossipListener bool // Gossip protocol listener
	GRPCServer     bool // gRPC server main loop
	HTTPServer     bool // HTTP API server
	FlightServer   bool // Arrow Flight server

	// Utility goroutines
	MetricsCollector bool // Prometheus metrics scraping
	PprofServer      bool // pprof HTTP server
}

// AuditGoroutines inspects the current goroutine landscape
func AuditGoroutines() GoroutineAudit {
	// This would require runtime inspection in a real implementation
	// For now, return a static audit based on known goroutines
	return GoroutineAudit{
		CompactionWorker:  true,
		LifecycleManager:  true,
		EvictionTicker:    true,
		RepairWorker:      true,
		IndexingWorker:    true,
		ReplicationWorker: false, // Not always active
		HNSWRepairWorker:  true,
		BM25IndexWorker:   false, // Conditional
		BM25EvictWorker:   false, // Conditional
		GossipListener:    false, // Conditional
		GRPCServer:        true,
		HTTPServer:        true,
		FlightServer:      false, // Conditional
		MetricsCollector:  true,
		PprofServer:       false, // Conditional
	}
}

// ListActiveGoroutines returns a human-readable list of known background goroutines
func ListActiveGoroutines() []string {
	var goroutines []string

	audit := AuditGoroutines()

	if audit.CompactionWorker {
		goroutines = append(goroutines, "compaction_worker")
	}
	if audit.LifecycleManager {
		goroutines = append(goroutines, "lifecycle_manager")
	}
	if audit.EvictionTicker {
		goroutines = append(goroutines, "eviction_ticker")
	}
	if audit.RepairWorker {
		goroutines = append(goroutines, "repair_worker")
	}
	if audit.IndexingWorker {
		goroutines = append(goroutines, "indexing_worker")
	}
	if audit.ReplicationWorker {
		goroutines = append(goroutines, "replication_worker")
	}
	if audit.HNSWRepairWorker {
		goroutines = append(goroutines, "hnsw_repair_worker")
	}
	if audit.BM25IndexWorker {
		goroutines = append(goroutines, "bm25_index_worker")
	}
	if audit.BM25EvictWorker {
		goroutines = append(goroutines, "bm25_evict_worker")
	}
	if audit.GossipListener {
		goroutines = append(goroutines, "gossip_listener")
	}
	if audit.GRPCServer {
		goroutines = append(goroutines, "grpc_server")
	}
	if audit.HTTPServer {
		goroutines = append(goroutines, "http_server")
	}
	if audit.FlightServer {
		goroutines = append(goroutines, "flight_server")
	}
	if audit.MetricsCollector {
		goroutines = append(goroutines, "metrics_collector")
	}
	if audit.PprofServer {
		goroutines = append(goroutines, "pprof_server")
	}

	return goroutines
}
