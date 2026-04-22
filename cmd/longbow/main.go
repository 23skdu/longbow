package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof" // Register pprof handlers manually
	"os"
	"os/signal"
	"strconv" // Added for hostname fallback
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"runtime"
	"runtime/debug"

	"github.com/23skdu/longbow/internal/autoscale"
	lbflight "github.com/23skdu/longbow/internal/flight"
	"github.com/23skdu/longbow/internal/gpu"
	"github.com/23skdu/longbow/internal/limiter"
	"github.com/23skdu/longbow/internal/logging"
	lbmem "github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/mesh"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/middleware"
	"github.com/23skdu/longbow/internal/sharding"
	"github.com/23skdu/longbow/internal/store"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Config holds the application configuration
type Config struct {
	// gRPC KeepAlive Configuration
	KeepAliveTime                time.Duration `envconfig:"GRPC_KEEPALIVE_TIME" default:"2h"`
	KeepAliveTimeout             time.Duration `envconfig:"GRPC_KEEPALIVE_TIMEOUT" default:"20s"`
	KeepAliveMinTime             time.Duration `envconfig:"GRPC_KEEPALIVE_MIN_TIME" default:"5m"`
	KeepAlivePermitWithoutStream bool          `envconfig:"GRPC_KEEPALIVE_PERMIT_WITHOUT_STREAM" default:"false"`

	ListenAddr       string        `envconfig:"LISTEN_ADDR" default:"0.0.0.0:3000"`
	NodeID           string        `envconfig:"NODE_ID" default:""` // Optional override
	MetaAddr         string        `envconfig:"META_ADDR" default:"0.0.0.0:3001"`
	MetricsAddr      string        `envconfig:"METRICS_ADDR" default:"0.0.0.0:9090"`
	MaxMemory        int64         `envconfig:"MAX_MEMORY" default:"1073741824"`
	DataPath         string        `envconfig:"DATA_PATH" default:"./data"`
	TTL              time.Duration `envconfig:"TTL" default:"0s"`
	SnapshotInterval time.Duration `envconfig:"SNAPSHOT_INTERVAL" default:"1h"`
	MaxWALSize       int64         `envconfig:"MAX_WAL_SIZE" default:"104857600"`

	// Logging configuration
	LogFormat string `envconfig:"LOG_FORMAT" default:"json"`
	LogLevel  string `envconfig:"LOG_LEVEL" default:"info"`

	// gRPC Server Options - configurable for large RecordBatch transfers
	// Increased to 512MB to support large dimension vectors (e.g., OpenAI 1536/3072-dim)
	GRPCMaxRecvMsgSize        int    `envconfig:"GRPC_MAX_RECV_MSG_SIZE" default:"536870912"`      // 512MB default (was 64MB)
	GRPCMaxSendMsgSize        int    `envconfig:"GRPC_MAX_SEND_MSG_SIZE" default:"536870912"`      // 512MB default (was 64MB)
	GRPCInitialWindowSize     int32  `envconfig:"GRPC_INITIAL_WINDOW_SIZE" default:"1048576"`      // 1MB default
	GRPCInitialConnWindowSize int32  `envconfig:"GRPC_INITIAL_CONN_WINDOW_SIZE" default:"1048576"` // 1MB default
	GRPCMaxConcurrentStreams  uint32 `envconfig:"GRPC_MAX_CONCURRENT_STREAMS" default:"250"`       // 250 default

	// Gossip Configuration (Production Readiness)
	GossipEnabled       bool          `envconfig:"GOSSIP_ENABLED" default:"false"`
	GossipPort          int           `envconfig:"GOSSIP_PORT" default:"7946"`
	GossipInterval      time.Duration `envconfig:"GOSSIP_INTERVAL" default:"200ms"`
	GossipAdvertiseAddr string        `envconfig:"GOSSIP_ADVERTISE_ADDR" default:"127.0.0.1"`  // Set to Pod IP in K8s
	GossipDiscovery     string        `envconfig:"GOSSIP_DISCOVERY_PROVIDER" default:"static"` // static, k8s, dns
	GossipStaticPeers   string        `envconfig:"GOSSIP_STATIC_PEERS" default:""`             // Comma separated
	GossipDNSRecord     string        `envconfig:"GOSSIP_DNS_RECORD" default:""`

	// Storage Configuration
	StorageAsyncFsync     bool `envconfig:"STORAGE_ASYNC_FSYNC" default:"true"`
	StorageDoPutBatchSize int  `envconfig:"STORAGE_DOPUT_BATCH_SIZE" default:"100"`
	StorageUseIOUring     bool `envconfig:"STORAGE_USE_IOURING" default:"false"`
	StorageUseDirectIO    bool `envconfig:"STORAGE_USE_DIRECT_IO" default:"false"`

	// Memory Management Configuration
	MemoryEvictionHeadroom float64 `envconfig:"MEMORY_EVICTION_HEADROOM" default:"0.10"`
	MemoryEvictionPolicy   string  `envconfig:"MEMORY_EVICTION_POLICY" default:"lru"`
	MemoryRejectWrites     bool    `envconfig:"MEMORY_REJECT_WRITES" default:"true"`

	// Hybrid Search Configuration
	HybridSearchEnabled     bool    `envconfig:"HYBRID_SEARCH_ENABLED" default:"false"`
	HybridSearchTextColumns string  `envconfig:"HYBRID_TEXT_COLUMNS" default:""`
	HybridSearchAlpha       float32 `envconfig:"HYBRID_ALPHA" default:"0.5"`

	// GPU Configuration
	GPUEnabled  bool `envconfig:"GPU_ENABLED" default:"false"`
	GPUDeviceID int  `envconfig:"GPU_DEVICE_ID" default:"0"`

	// Rate Limiting Configuration
	RateLimitRPS   int `envconfig:"RATE_LIMIT_RPS" default:"0"` // 0 = disabled
	RateLimitBurst int `envconfig:"RATE_LIMIT_BURST" default:"0"`

	// Garbage Collection Tuning
	GCBallastG int `envconfig:"GC_BALLAST_G" default:"0"` // Ballast size in GB
	GOGC       int `envconfig:"GOGC" default:"100"`       // Go Garbage Collector percentage

	// HNSW Configuration
	HNSWMaxM           int  `envconfig:"HNSW_M" default:"32"`
	HNSWEfConstruction int  `envconfig:"HNSW_EF_CONSTRUCTION" default:"400"`
	HNSWSQ8Enabled     bool `envconfig:"HNSW_SQ8_ENABLED" default:"false"`

	// Compaction Configuration
	CompactionEnabled         bool          `envconfig:"COMPACTION_ENABLED" default:"true"`
	CompactionInterval        time.Duration `envconfig:"COMPACTION_INTERVAL" default:"30s"`
	CompactionTargetBatchSize int64         `envconfig:"COMPACTION_TARGET_BATCH_SIZE" default:"10000"`
	CompactionMinBatches      int           `envconfig:"COMPACTION_MIN_BATCHES" default:"10"`

	// Auto Sharding Configuration
	AutoShardingEnabled        bool `envconfig:"AUTO_SHARDING_ENABLED" default:"true"`
	AutoShardingThreshold      int  `envconfig:"AUTO_SHARDING_THRESHOLD" default:"10000"`
	AutoShardingSplitThreshold int  `envconfig:"AUTO_SHARDING_SPLIT_THRESHOLD" default:"65536"` // Default chunk size
	RingShardingEnabled        bool `envconfig:"RING_SHARDING_ENABLED" default:"true"`
	IngestionWorkerCount       int  `envconfig:"INGESTION_WORKER_COUNT" default:"0"` // 0 = runtime.NumCPU()

	// CDC Configuration (Part 17.1)
	CDCEnabled        bool `envconfig:"CDC_ENABLED" default:"false"`
	CDCBufferSize     int  `envconfig:"CDC_BUFFER_SIZE" default:"1024"`
	CDCAsyncDispatch  bool `envconfig:"CDC_ASYNC_DISPATCH" default:"true"`
	CDCWorkerPoolSize int  `envconfig:"CDC_WORKER_POOL_SIZE" default:"4"`

	// WebSocket Configuration (Part 17.2)
	WebSocketEnabled bool   `envconfig:"WEBSOCKET_ENABLED" default:"false"`
	WebSocketAddr    string `envconfig:"WEBSOCKET_ADDR" default:"0.0.0.0:3002"`

	// MQ Exporter Configuration (Part 17.3)
	MQEnabled   bool   `envconfig:"MQ_ENABLED" default:"false"`
	MQType      string `envconfig:"MQ_TYPE" default:"kafka"` // kafka or pulsar
	MQBrokers   string `envconfig:"MQ_BROKERS" default:""`
	MQTopic     string `envconfig:"MQ_TOPIC" default:""`
	MQBatchSize int    `envconfig:"MQ_BATCH_SIZE" default:"100"`

	// Learned Index Configuration (Part 16)
	LearnedIndexEnabled          bool          `envconfig:"LEARNED_INDEX_ENABLED" default:"false"`
	LearnedIndexMinSamples       int           `envconfig:"LEARNED_INDEX_MIN_SAMPLES" default:"100"`
	LearnedIndexConfidenceThresh float64       `envconfig:"LEARNED_INDEX_CONFIDENCE_THRESH" default:"0.7"`
	LearnedIndexUpdateInterval   time.Duration `envconfig:"LEARNED_INDEX_UPDATE_INTERVAL" default:"1h"`

	// Ollama Configuration (Part 4.1)
	OllamaEnabled  bool   `envconfig:"OLLAMA_ENABLED" default:"false"`
	OllamaEndpoint string `envconfig:"OLLAMA_ENDPOINT" default:"http://localhost:11434"`
	OllamaModel    string `envconfig:"OLLAMA_MODEL" default:""`
	OllamaTimeout  int    `envconfig:"OLLAMA_TIMEOUT" default:"30"`

	// Temporal Query Configuration (Part 22)
	TemporalEnabled            bool          `envconfig:"TEMPORAL_ENABLED" default:"false"`
	TemporalVersionHistory     bool          `envconfig:"TEMPORAL_VERSION_HISTORY" default:"false"`
	TemporalMaxVersions        int           `envconfig:"TEMPORAL_MAX_VERSIONS" default:"10"`
	TemporalRetentionPeriod    time.Duration `envconfig:"TEMPORAL_RETENTION_PERIOD" default:"168h"` // 7 days
	TemporalTTLEnabled         bool          `envconfig:"TEMPORAL_TTL_ENABLED" default:"false"`
	TemporalDefaultTTL         time.Duration `envconfig:"TEMPORAL_DEFAULT_TTL" default:"720h"` // 30 days
	TemporalCleanupInterval    time.Duration `envconfig:"TEMPORAL_CLEANUP_INTERVAL" default:"1h"`
	TemporalAggregationEnabled bool          `envconfig:"TEMPORAL_AGGREGATION_ENABLED" default:"false"`
	TemporalMaxBuckets         int           `envconfig:"TEMPORAL_MAX_BUCKETS" default:"1000"`
	
	// RDMA Configuration
	RDMAEnabled   bool   `envconfig:"RDMA_ENABLED" default:"false"`
	RDMAInterface string `envconfig:"RDMA_INTERFACE" default:"eth0"`
	RDMAPort      int    `envconfig:"RDMA_PORT" default:"3002"`
}

// Global config instance for hook functions
var globalCfg Config
var globalIsReady atomic.Bool

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	// Load .env file if it exists (do this before logger init to read LOG_* vars)
	_ = godotenv.Load()

	// Handle signals for graceful shutdown and hot reload
	// Use NotifyContext to cancel context on SIGINT/SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := envconfig.Process("LONGBOW", &globalCfg); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to process config: %v\n", err)
		return err
	}
	cfg := globalCfg

	// Initialize zerolog logger
	logger, err := logging.NewLogger(logging.Config{
		Format: cfg.LogFormat,
		Level:  cfg.LogLevel,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	if err := ValidateConfig(&cfg); err != nil {
		logger.Error().Err(err).Msg("Invalid configuration")
		fmt.Fprintf(os.Stderr, "Invalid configuration: %v\n", err)
		os.Exit(1)
	}

	logger.Info().
		Str("listen_addr", cfg.ListenAddr).
		Str("meta_addr", cfg.MetaAddr).
		Str("metrics_addr", cfg.MetricsAddr).
		Int64("max_memory", cfg.MaxMemory).
		Str("data_path", cfg.DataPath).
		Dur("snapshot_interval", cfg.SnapshotInterval).
		Int64("max_wal_size", cfg.MaxWALSize).
		Dur("ttl", cfg.TTL).
		Int("gc_ballast_g", cfg.GCBallastG).
		Int("gogc", cfg.GOGC).
		Msg(">>---> Starting Longbow")

	// Enable profiling
	runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)

	// Apply GC Ballast if configured
	var ballast []byte
	if cfg.GCBallastG > 0 {
		ballast = make([]byte, uint64(cfg.GCBallastG)<<30)
		logger.Info().Int("size_gb", cfg.GCBallastG).Msg("GC Ballast initialized")
	}

	// Dynamic GOGC Tuning
	var tuner *lbmem.GCTuner
	if cfg.MaxMemory > 0 {
		tuner = lbmem.NewGCTuner(cfg.MaxMemory, cfg.GOGC, 10, &logger)
		tuner.IsAggressive = true
		// Run in background, tied to ctx (stops on signal)
		go tuner.Start(ctx, 500*time.Millisecond)
		logger.Info().
			Int64("limit_bytes", cfg.MaxMemory).
			Int("high_gogc", cfg.GOGC).
			Msg("Aggressive GOGC Tuner started (500ms interval, arena-aware)")
	} else if cfg.GOGC != 100 {
		debug.SetGCPercent(cfg.GOGC)
		logger.Info().Int("value", cfg.GOGC).Msg("GOGC tuned (static)")
	}

	// Create memory allocator
	mem := store.NewPooledAllocator()

	// Initialize AutoScaler (Part 1.1)
	scaler := autoscale.NewAutoScaler(logger)
	// Initialize vector store
	vectorStore := store.NewVectorStore(mem, logger, cfg.MaxMemory, cfg.MaxWALSize, cfg.TTL)
	vectorStore.SetGCTuner(tuner)
	vectorStore.SetAutoScaler(scaler)
	scaler.SetReconciler(vectorStore)

	go scaler.Start(ctx)
	logger.Info().Msg("Serverless Auto-Scaler monitoring started")

	// Keep ballast alive until the end of run()
	defer runtime.KeepAlive(ballast)

	// Configure memory management
	vectorStore.SetMemoryConfig(store.MemoryConfig{
		MaxMemory:        cfg.MaxMemory,
		EvictionHeadroom: cfg.MemoryEvictionHeadroom,
		EvictionPolicy:   cfg.MemoryEvictionPolicy,
		RejectWrites:     cfg.MemoryRejectWrites,
	})

	// Configure Auto Sharding
	vectorStore.SetAutoShardingConfig(store.AutoShardingConfig{
		Enabled:             cfg.AutoShardingEnabled,
		ShardThreshold:      cfg.AutoShardingThreshold,
		ShardCount:          runtime.NumCPU(),
		ShardSplitThreshold: cfg.AutoShardingSplitThreshold,
		UseRingSharding:     cfg.RingShardingEnabled,
	})

	// Configure hybrid search
	if cfg.HybridSearchEnabled {
		var textCols []string
		if cfg.HybridSearchTextColumns != "" {
			textCols = strings.Split(cfg.HybridSearchTextColumns, ",")
		}
		vectorStore.SetHybridSearchConfig(store.HybridSearchConfig{
			Enabled:     true,
			TextColumns: textCols,
			Alpha:       cfg.HybridSearchAlpha,
			BM25:        store.DefaultBM25Config(),
			RRFk:        60,
		})
		logger.Info().
			Strs("columns", textCols).
			Float32("alpha", cfg.HybridSearchAlpha).
			Msg("Hybrid search enabled")
	}

	// Configure GPU acceleration
	if cfg.GPUEnabled {
		detectedBackend := gpu.DetectGPUBackend()

		if detectedBackend == gpu.BackendCUDA || detectedBackend == gpu.BackendMetal {
			vectorStore.SetGPUConfig(detectedBackend, cfg.GPUDeviceID)
			logger.Info().
				Str("backend", detectedBackend.String()).
				Bool("enabled", cfg.GPUEnabled).
				Int("device", cfg.GPUDeviceID).
				Msg("GPU acceleration configured")
		} else {
			logger.Warn().
				Str("detected_backend", detectedBackend.String()).
				Msg("No GPU detected, falling back to CPU")
			vectorStore.SetGPUConfig(gpu.BackendCPU, 0)
		}
	}

	// Start eviction ticker if TTL is enabled
	if cfg.TTL > 0 {
		checkInterval := time.Minute
		if cfg.TTL/10 < checkInterval {
			checkInterval = cfg.TTL / 10
		}
		if checkInterval < time.Second {
			checkInterval = time.Second
		}
		vectorStore.StartEvictionTicker(checkInterval)
		vectorStore.StartWALCheckTicker(10 * time.Second)
		logger.Info().Dur("interval", checkInterval).Msg("Eviction ticker started")
	}

	// Initialize Persistence (WAL + Snapshots)
	storageCfg := store.StorageConfig{
		DataPath:         cfg.DataPath,
		SnapshotInterval: cfg.SnapshotInterval,
		AsyncFsync:       cfg.StorageAsyncFsync,
		DoPutBatchSize:   cfg.StorageDoPutBatchSize,
		UseIOUring:       cfg.StorageUseIOUring,
		UseDirectIO:      cfg.StorageUseDirectIO,
	}
	if err := vectorStore.InitPersistence(storageCfg); err != nil {
		logger.Panic().Err(err).Msg("Failed to initialize persistence")
	}

	// Initialize CDC (Part 17.1)
	var cdc *store.ChangeDataCapture
	if cfg.CDCEnabled {
		cdc = store.NewChangeDataCapture(vectorStore, logger)
		vectorStore.SetCDC(cdc)
		logger.Info().
			Int("buffer_size", cfg.CDCBufferSize).
			Int("worker_pool_size", cfg.CDCWorkerPoolSize).
			Bool("async_dispatch", cfg.CDCAsyncDispatch).
			Msg("CDC initialized")
	}

	// Initialize WebSocket Server (Part 17.2)
	var wsServer *store.WebSocketServer
	if cfg.WebSocketEnabled && cdc != nil {
		wsServer = store.NewWebSocketServer(logger, cdc)
		go func() {
			if err := wsServer.Start(cfg.WebSocketAddr); err != nil {
				logger.Error().Err(err).Msg("WebSocket server failed")
			}
		}()
		logger.Info().Str("addr", cfg.WebSocketAddr).Msg("WebSocket server started")
	}

	// Initialize MQ Exporter (Part 17.3)
	var mqExporter *store.MessageQueueExporter
	if cfg.MQEnabled && cdc != nil {
		mqCfg := store.MQConfig{
			Type:           store.MQType(cfg.MQType),
			Brokers:        cfg.MQBrokers,
			Topic:          cfg.MQTopic,
			BatchSize:      cfg.MQBatchSize,
			BatchTimeoutMs: 100,
		}
		var err error
		mqExporter, err = store.NewMessageQueueExporter(logger, cdc, mqCfg)
		if err != nil {
			logger.Warn().Err(err).Msg("Failed to initialize MQ exporter, continuing without it")
		} else {
			go mqExporter.Start()
			logger.Info().
				Str("type", cfg.MQType).
				Str("topic", cfg.MQTopic).
				Msg("MQ exporter started")
		}
	}

	// Initialize Learned Index Predictor (Part 16)
	var indexPredictor *store.IndexPerformancePredictor
	var learnedWithOllama *store.LearnedIndexWithOllama
	if cfg.LearnedIndexEnabled {
		indexPredictor = store.NewIndexPerformancePredictor(logger, store.LearnedIndexConfig{
			EnableAutoSelection: true,
			MinTrainingSamples:  cfg.LearnedIndexMinSamples,
			ConfidenceThreshold: cfg.LearnedIndexConfidenceThresh,
			UpdateInterval:      cfg.LearnedIndexUpdateInterval,
		})

		// Initialize Ollama client for learned index (Part 4.1)
		if cfg.OllamaEnabled && cfg.OllamaModel != "" {
			ollamaConfig := store.OllamaConfig{
				Endpoint: cfg.OllamaEndpoint,
				Model:    cfg.OllamaModel,
				Timeout:  cfg.OllamaTimeout,
			}
			learnedWithOllama = store.NewLearnedIndexWithOllama(logger, indexPredictor, ollamaConfig)
			vectorStore.SetIndexPredictor(indexPredictor)
			logger.Info().
				Str("endpoint", cfg.OllamaEndpoint).
				Str("model", cfg.OllamaModel).
				Msg("Learned index with Ollama initialized")
		} else {
			vectorStore.SetIndexPredictor(indexPredictor)
			logger.Info().
				Int("min_samples", cfg.LearnedIndexMinSamples).
				Float64("confidence_thresh", cfg.LearnedIndexConfidenceThresh).
				Msg("Learned index predictor initialized")
		}
	}
	_ = learnedWithOllama // Reserved for future API exposure

	// Initialize Temporal Index (Part 22)
	var temporalIndex *store.TemporalIndex
	if cfg.TemporalEnabled {
		temporalConfig := store.TemporalConfig{
			Enabled:            cfg.TemporalEnabled,
			VersionHistory:     cfg.TemporalVersionHistory,
			MaxVersions:        cfg.TemporalMaxVersions,
			RetentionPeriod:    cfg.TemporalRetentionPeriod,
			TTLEnabled:         cfg.TemporalTTLEnabled,
			DefaultTTL:         cfg.TemporalDefaultTTL,
			CleanupInterval:    cfg.TemporalCleanupInterval,
			AggregationEnabled: cfg.TemporalAggregationEnabled,
			MaxBuckets:         cfg.TemporalMaxBuckets,
		}
		temporalDim := 128
		if dimStr := os.Getenv("LONGBOW_TEMPORAL_DIM"); dimStr != "" {
			if d, err := strconv.Atoi(dimStr); err == nil {
				temporalDim = d
			}
		}
		temporalIndex = store.NewTemporalIndex(temporalDim)
		vectorStore.SetTemporalIndex(temporalIndex, temporalConfig)
		logger.Info().
			Bool("version_history", cfg.TemporalVersionHistory).
			Int("max_versions", cfg.TemporalMaxVersions).
			Dur("retention_period", cfg.TemporalRetentionPeriod).
			Bool("ttl_enabled", cfg.TemporalTTLEnabled).
			Bool("aggregation_enabled", cfg.TemporalAggregationEnabled).
			Msg("Temporal index initialized")
	}
	_ = temporalIndex // Reserved for future API exposure

	// Start background indexing workers
	vectorStore.StartIndexingWorkers(runtime.NumCPU())
	// Start ingestion workers
	vectorStore.StartIngestionWorkers(cfg.IngestionWorkerCount)

	// Initialize OpenTelemetry Tracer
	tp := initTracer()
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			logger.Error().Err(err).Msg("Error shutting down tracer provider")
		}
	}()

	// Start metrics server with timeouts and port retry logic
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())

		mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
			if globalIsReady.Load() {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("OK"))
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("Not Ready"))
			}
		})

		// Profiling endpoints
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

		// Try to bind to the configured port, with fallback to next 5 ports
		baseAddr := cfg.MetricsAddr
		host, portStr, err := net.SplitHostPort(baseAddr)
		if err != nil {
			logger.Error().Err(err).Str("addr", baseAddr).Msg("Invalid metrics address format")
			return
		}

		basePort, err := strconv.Atoi(portStr)
		if err != nil {
			logger.Error().Err(err).Str("port", portStr).Msg("Invalid metrics port")
			return
		}

		var boundAddr string
		var listener net.Listener
		maxRetries := 5
		for i := 0; i < maxRetries; i++ {
			tryPort := basePort + i
			tryAddr := net.JoinHostPort(host, strconv.Itoa(tryPort))

			listener, err = net.Listen("tcp", tryAddr)
			if err == nil {
				boundAddr = tryAddr
				logger.Info().
					Str("addr", boundAddr).
					Int("attempt", i+1).
					Msg("Metrics server bound successfully")
				break
			}

			if i == maxRetries-1 {
				logger.Error().
					Err(err).
					Str("base_addr", baseAddr).
					Int("retries", maxRetries).
					Msg("Failed to bind metrics server after retries")
				return
			}
		}

		srv := &http.Server{
			Handler:      mux,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  60 * time.Second,
		}

		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Error().
				Err(err).
				Str("addr", boundAddr).
				Msg("Metrics server failed")
		}
	}()

	// Initialize Sharding Ring Manager
	// Use configured NodeID or fallback to hostname
	nodeID := cfg.NodeID
	if nodeID == "" {
		hostname, _ := os.Hostname()
		if hostname == "" {
			nodeID = "node-" + strconv.FormatInt(time.Now().UnixNano(), 10)
		} else {
			nodeID = hostname
		}
	}

	ringManager := sharding.NewRingManager(nodeID, logger)
	// Add self to ring immediately
	selfAddr := cfg.ListenAddr
	metaAddr := cfg.MetaAddr
	if strings.HasPrefix(selfAddr, "0.0.0.0:") {
		selfAddr = "127.0.0.1" + selfAddr[7:]
	}
	if strings.HasPrefix(metaAddr, "0.0.0.0:") {
		metaAddr = "127.0.0.1" + metaAddr[7:]
	}
	ringManager.NotifyJoin(&mesh.Member{ID: nodeID, Addr: selfAddr, GRPCAddr: selfAddr, MetaAddr: metaAddr, Status: mesh.StatusAlive})

	// Start Gossip if enabled
	if cfg.GossipEnabled {
		// Use hostname or random ID?

		gossipCfg := mesh.GossipConfig{
			ID:             nodeID,
			Port:           cfg.GossipPort,
			ProtocolPeriod: cfg.GossipInterval,
			Addr:           "0.0.0.0",               // Bind to all interfaces
			AdvertiseAddr:  cfg.GossipAdvertiseAddr, // Public gossip address for containers/K8s
			GRPCAddr:       selfAddr,                // Advertise this for gRPC Data
			MetaAddr:       metaAddr,                // Advertise this for gRPC Meta
			Delegate:       ringManager,
			Discovery: mesh.DiscoveryConfig{
				Provider:    cfg.GossipDiscovery,
				StaticPeers: cfg.GossipStaticPeers,
				DNSRecord:   cfg.GossipDNSRecord,
			},
		}

		g := mesh.NewGossip(&gossipCfg)
		if err := g.Start(); err != nil {
			logger.Error().Err(err).Msg("Failed to start Gossip")
		} else {
			logger.Info().
				Str("id", gossipCfg.ID).
				Int("port", gossipCfg.Port).
				Dur("interval", gossipCfg.ProtocolPeriod).
				Msg("Gossip started")
			vectorStore.SetMesh(g)
			defer g.Stop()
		}
	}

	// Initialize Request Forwarder with RingManager for address resolution
	fwdCfg := sharding.DefaultForwarderConfig()
	forwarder := sharding.NewRequestForwarder(&fwdCfg, ringManager)

	// gRPC Server Options - using env-configurable message sizes and window sizes
	serverOpts := make([]grpc.ServerOption, 0, 9)
	serverOpts = append(serverOpts,
		grpc.MaxRecvMsgSize(cfg.GRPCMaxRecvMsgSize),
		grpc.MaxSendMsgSize(cfg.GRPCMaxSendMsgSize),
		grpc.InitialWindowSize(cfg.GRPCInitialWindowSize),
		grpc.InitialConnWindowSize(cfg.GRPCInitialConnWindowSize),
		grpc.MaxConcurrentStreams(cfg.GRPCMaxConcurrentStreams),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    cfg.KeepAliveTime,
			Timeout: cfg.KeepAliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             cfg.KeepAliveMinTime,
			PermitWithoutStream: cfg.KeepAlivePermitWithoutStream,
		}),
	)

	// Initialize Rate Limiter
	rateLimiter := limiter.NewRateLimiter(limiter.Config{
		RPS:   cfg.RateLimitRPS,
		Burst: cfg.RateLimitBurst,
	})

	// Initialize StreamAggregator for Global Search
	streamAggregator := sharding.NewStreamAggregator(mem, logger)

	// Add Interceptors (Chained)
	serverOpts = append(serverOpts,
		grpc.ChainUnaryInterceptor(
			middleware.AdmissionInterceptor(vectorStore.GetAdmissionController()),
			middleware.CircuitBreakerInterceptor(),
			rateLimiter.UnaryInterceptor(),
			sharding.PartitionProxyInterceptor(ringManager, forwarder),
		),
		grpc.ChainStreamInterceptor(
			middleware.AdmissionStreamInterceptor(vectorStore.GetAdmissionController()),
			rateLimiter.StreamInterceptor(),
			sharding.PartitionProxyStreamInterceptor(ringManager, forwarder, streamAggregator),
		),
	)

	logger.Info().
		Int("max_recv_msg_size", cfg.GRPCMaxRecvMsgSize).
		Int("max_send_msg_size", cfg.GRPCMaxSendMsgSize).
		Int32("initial_window_size", cfg.GRPCInitialWindowSize).
		Int32("initial_conn_window_size", cfg.GRPCInitialConnWindowSize).
		Uint32("max_concurrent_streams", cfg.GRPCMaxConcurrentStreams).
		Msg("gRPC server options configured")

	// Set Prometheus metrics for gRPC configuration
	metrics.GRPCMaxRecvMsgSizeBytes.Set(float64(cfg.GRPCMaxRecvMsgSize))
	metrics.GRPCMaxSendMsgSizeBytes.Set(float64(cfg.GRPCMaxSendMsgSize))
	metrics.GRPCInitialWindowSizeBytes.Set(float64(cfg.GRPCInitialWindowSize))
	metrics.GRPCInitialConnWindowSizeBytes.Set(float64(cfg.GRPCInitialConnWindowSize))
	metrics.GRPCMaxConcurrentStreams.Set(float64(cfg.GRPCMaxConcurrentStreams))

	// --- Data Server Setup ---
	dataService := store.NewDataServer(vectorStore)
	dataServer := grpc.NewServer(serverOpts...)
	flight.RegisterFlightServiceServer(dataServer, dataService)

	dataLisBase, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		logger.Error().
			Err(err).
			Str("addr", cfg.ListenAddr).
			Msg("Failed to listen for Data Server")
		return err
	}
	dataLis := store.NewTCPNoDelayListener(dataLisBase.(*net.TCPListener))

	// --- Meta Server Setup ---
	metaServer := grpc.NewServer(serverOpts...)
	metaService := store.NewMetaServer(vectorStore)
	flight.RegisterFlightServiceServer(metaServer, metaService)

	metaLisBase, err := net.Listen("tcp", cfg.MetaAddr)
	if err != nil {
		logger.Error().
			Err(err).
			Str("addr", cfg.MetaAddr).
			Msg("Failed to listen for Meta Server")
		return err
	}

	metaLis := store.NewTCPNoDelayListener(metaLisBase.(*net.TCPListener))
	// Start Data Server
	go func() {
		logger.Info().Str("addr", cfg.ListenAddr).Msg("Listening for Data gRPC connections")
		if err := dataServer.Serve(dataLis); err != nil {
			logger.Error().Err(err).Msg("Data gRPC server failed")
		}
	}()

	// Start UDS Data Server if configured

	// Start Meta Server
	go func() {
		logger.Info().Str("addr", cfg.MetaAddr).Msg("Listening for Meta gRPC connections")
		if err := metaServer.Serve(metaLis); err != nil {
			logger.Error().Err(err).Msg("Meta gRPC server failed")
		}
	}()

	// Start RDMA Server if enabled
	if cfg.RDMAEnabled {
		rdmaSrv := lbflight.NewRDMAServer(true)
		go func() {
			addr := fmt.Sprintf("0.0.0.0:%d", cfg.RDMAPort)
			logger.Info().Str("addr", addr).Msg("Starting RDMA Zero-Copy Server")
			if err := rdmaSrv.StartRDMAListener(addr); err != nil {
				logger.Error().Err(err).Msg("RDMA server failed")
			}
		}()
	}

	// System is now ready to receive traffic
	globalIsReady.Store(true)

	// Wait for signal
	<-ctx.Done()
	logger.Info().Msg("Received shutdown signal, initiating graceful shutdown")

	// Shutdown Sequence
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			dataServer.GracefulStop()

			logger.Info().Msg("Data server stopped")
		}()
		go func() {
			defer wg.Done()
			metaServer.GracefulStop()
			_ = metaService.Close() // Clean up coordinator clients
			logger.Info().Msg("Meta server stopped")
		}()
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info().Msg("gRPC servers stopped gracefully")
	case <-shutdownCtx.Done():
		logger.Warn().Msg("Shutdown timed out, forcing stop")
		dataServer.Stop()
		metaServer.Stop()
	}

	// Close VectorStore (flushes WAL/Snapshots)
	if err := vectorStore.Close(); err != nil {
		logger.Error().Err(err).Msg("Failed to close vector store")
	} else {
		logger.Info().Msg("Vector store closed successfully")
	}

	return nil
}

func initTracer() *sdktrace.TracerProvider {
	// Define resource attributes
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String("longbow"),
	)

	// Create TracerProvider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		// sdktrace.WithBatcher(exporter), // Add exporter here
	)

	// Register globals
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return tp
}
