package main

import (
	"net"
	"net/http"
	"net/http/pprof" // Register pprof handlers manually
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"context"
	"strconv" // Added for hostname fallback

	"runtime"
	"runtime/debug"

	"github.com/23skdu/longbow/internal/limiter"
	"github.com/23skdu/longbow/internal/logging"
	"github.com/23skdu/longbow/internal/mesh"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/middleware"
	"github.com/23skdu/longbow/internal/sharding"
	"github.com/23skdu/longbow/internal/store"
	"github.com/23skdu/longbow/internal/store/hnsw2"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.uber.org/zap"
	"google.golang.org/grpc"
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
}

// initializeHNSW2 is the hook function that initializes hnsw2 for datasets.
func initializeHNSW2(ds *store.Dataset, logger *zap.Logger) {
	if !ds.UseHNSW2() {
		return
	}
	config := hnsw2.DefaultConfig()
	hnswIndex := hnsw2.NewArrowHNSW(ds, config)
	ds.SetHNSW2Index(hnswIndex)
	if logger != nil {
		logger.Info("hnsw2 initialized", zap.String("dataset", ds.Name))
	}
}

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	// Load .env file if it exists (do this before logger init to read LOG_* vars)
	_ = godotenv.Load()

	var cfg Config
	if err := envconfig.Process("LONGBOW", &cfg); err != nil {
		// Fallback to basic logging if config fails
		panic("Failed to process config: " + err.Error())
	}

	// Initialize zap logger
	logger, err := logging.NewLogger(logging.Config{
		Format: cfg.LogFormat,
		Level:  cfg.LogLevel,
	})
	if err != nil {
		panic("Failed to initialize logger: " + err.Error())
	}
	defer func() { _ = logger.Sync() }()

	logger.Info(">>---> Starting Longbow",
		zap.String("listen_addr", cfg.ListenAddr),
		zap.String("meta_addr", cfg.MetaAddr),
		zap.String("metrics_addr", cfg.MetricsAddr),
		zap.Int64("max_memory", cfg.MaxMemory),
		zap.String("data_path", cfg.DataPath),
		zap.Duration("snapshot_interval", cfg.SnapshotInterval),
		zap.Int64("max_wal_size", cfg.MaxWALSize),
		zap.Duration("ttl", cfg.TTL),
		zap.Int("gc_ballast_g", cfg.GCBallastG),
		zap.Int("gogc", cfg.GOGC),
	)

	// Apply GC Ballast if configured
	var ballast []byte
	if cfg.GCBallastG > 0 {
		ballast = make([]byte, uint64(cfg.GCBallastG)<<30)
		logger.Info("GC Ballast initialized", zap.Int("size_gb", cfg.GCBallastG))
	}

	// Apply GOGC tuning
	if cfg.GOGC != 100 {
		debug.SetGCPercent(cfg.GOGC)
		logger.Info("GOGC tuned", zap.Int("value", cfg.GOGC))
	}

	// Set Memory Limit if MaxMemory is configured (Go 1.19+)
	if cfg.MaxMemory > 0 {
		debug.SetMemoryLimit(cfg.MaxMemory)
		logger.Info("Go Memory Limit set", zap.Int64("limit_bytes", cfg.MaxMemory))
	}

	// Keep ballast alive until the end of run()
	defer runtime.KeepAlive(ballast)

	// Create memory allocator
	mem := memory.NewGoAllocator()

	// Initialize vector store
	vectorStore := store.NewVectorStore(mem, logger, cfg.MaxMemory, cfg.MaxWALSize, cfg.TTL)

	// Register hnsw2 initialization hook
	vectorStore.SetDatasetInitHook(func(ds *store.Dataset) {
		initializeHNSW2(ds, logger)
	})

	// Configure memory management
	vectorStore.SetMemoryConfig(store.MemoryConfig{
		MaxMemory:        cfg.MaxMemory,
		EvictionHeadroom: cfg.MemoryEvictionHeadroom,
		EvictionPolicy:   cfg.MemoryEvictionPolicy,
		RejectWrites:     cfg.MemoryRejectWrites,
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
		logger.Info("Hybrid search enabled", zap.Strings("columns", textCols), zap.Float32("alpha", cfg.HybridSearchAlpha))
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
		logger.Info("Eviction ticker started", zap.Duration("interval", checkInterval))
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
		logger.Panic("Failed to initialize persistence", zap.Error(err))
	}

	// Initialize OpenTelemetry Tracer
	tp := initTracer()
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			logger.Error("Error shutting down tracer provider", zap.Error(err))
		}
	}()

	// Start metrics server with timeouts (G114 fix)
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())

		// Profiling endpoints
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

		srv := &http.Server{
			Addr:         cfg.MetricsAddr,
			Handler:      mux,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  60 * time.Second,
		}

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Metrics server failed", zap.Error(err))
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
			Addr:           "0.0.0.0", // Bind to all interfaces
			GRPCAddr:       selfAddr,  // Advertise this for gRPC Data
			MetaAddr:       metaAddr,  // Advertise this for gRPC Meta
			Delegate:       ringManager,
			Discovery: mesh.DiscoveryConfig{
				Provider:    cfg.GossipDiscovery,
				StaticPeers: cfg.GossipStaticPeers,
				DNSRecord:   cfg.GossipDNSRecord,
			},
		}

		// If provided, override advertise addr logic (TODO: pass to NewGossip if supported or modify Gossip to take AdvertiseAddr)
		// Current gossip.go uses "127.0.0.1" hardcoded in Start(). We should probably fix that too but for now...
		// Let's rely on GossipConfig defaults or update internal/mesh/gossip.go later.
		// Wait, NewGossip takes GossipConfig but g.Start() uses "127.0.0.1". I should fix gossip.go too!
		// Proceeding with initialization.

		g := mesh.NewGossip(gossipCfg)
		if err := g.Start(); err != nil {
			logger.Error("Failed to start Gossip", zap.Error(err))
		} else {
			logger.Info("Gossip started",
				zap.String("id", gossipCfg.ID),
				zap.Int("port", gossipCfg.Port),
				zap.Duration("interval", gossipCfg.ProtocolPeriod))
			vectorStore.SetMesh(g)
			defer g.Stop()
		}
	}

	// Initialize Request Forwarder with RingManager for address resolution
	forwarder := sharding.NewRequestForwarder(sharding.DefaultForwarderConfig(), ringManager)

	// gRPC Server Options - using env-configurable message sizes and window sizes
	if err := cfg.ValidateGRPCConfig(); err != nil {
		logger.Error("Invalid gRPC config", zap.Error(err))
		_ = logger.Sync()
		return err
	}
	serverOpts := cfg.BuildGRPCServerOptions()

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
			middleware.CircuitBreakerInterceptor(),
			rateLimiter.UnaryInterceptor(),
			sharding.PartitionProxyInterceptor(ringManager, forwarder),
		),
		grpc.ChainStreamInterceptor(
			rateLimiter.StreamInterceptor(),
			sharding.PartitionProxyStreamInterceptor(ringManager, forwarder, streamAggregator),
		),
	)

	logger.Info("gRPC server options configured",
		zap.Int("max_recv_msg_size", cfg.GRPCMaxRecvMsgSize),
		zap.Int("max_send_msg_size", cfg.GRPCMaxSendMsgSize),
		zap.Int32("initial_window_size", cfg.GRPCInitialWindowSize),
		zap.Int32("initial_conn_window_size", cfg.GRPCInitialConnWindowSize),
		zap.Uint32("max_concurrent_streams", cfg.GRPCMaxConcurrentStreams),
	)

	// Set Prometheus metrics for gRPC configuration
	metrics.GRPCMaxRecvMsgSizeBytes.Set(float64(cfg.GRPCMaxRecvMsgSize))
	metrics.GRPCMaxSendMsgSizeBytes.Set(float64(cfg.GRPCMaxSendMsgSize))
	metrics.GRPCInitialWindowSizeBytes.Set(float64(cfg.GRPCInitialWindowSize))
	metrics.GRPCInitialConnWindowSizeBytes.Set(float64(cfg.GRPCInitialConnWindowSize))
	metrics.GRPCMaxConcurrentStreams.Set(float64(cfg.GRPCMaxConcurrentStreams))

	// --- Data Server Setup ---
	dataServer := grpc.NewServer(serverOpts...)
	flight.RegisterFlightServiceServer(dataServer, store.NewDataServer(vectorStore))

	dataLisBase, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		logger.Error("Failed to listen for Data Server", zap.Error(err), zap.String("addr", cfg.ListenAddr))
		_ = logger.Sync()
		return err
	}
	dataLis := store.NewTCPNoDelayListener(dataLisBase.(*net.TCPListener))

	// --- Meta Server Setup ---
	metaServer := grpc.NewServer(serverOpts...)
	metaService := store.NewMetaServer(vectorStore)
	flight.RegisterFlightServiceServer(metaServer, metaService)

	metaLisBase, err := net.Listen("tcp", cfg.MetaAddr)
	if err != nil {
		logger.Error("Failed to listen for Meta Server", zap.Error(err), zap.String("addr", cfg.MetaAddr))
		_ = logger.Sync()
		return err
	}

	metaLis := store.NewTCPNoDelayListener(metaLisBase.(*net.TCPListener))
	// Handle signals for graceful shutdown and hot reload
	// Use NotifyContext to cancel context on SIGINT/SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Start Data Server
	go func() {
		logger.Info("Listening for Data gRPC connections", zap.String("addr", cfg.ListenAddr))
		if err := dataServer.Serve(dataLis); err != nil {
			logger.Error("Data gRPC server failed", zap.Error(err))
		}
	}()

	// Start Meta Server
	go func() {
		logger.Info("Listening for Meta gRPC connections", zap.String("addr", cfg.MetaAddr))
		if err := metaServer.Serve(metaLis); err != nil {
			logger.Error("Meta gRPC server failed", zap.Error(err))
		}
	}()

	// Wait for signal
	<-ctx.Done()
	logger.Info("Received shutdown signal, initiating graceful shutdown")

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
			logger.Info("Data server stopped")
		}()
		go func() {
			defer wg.Done()
			metaServer.GracefulStop()
			_ = metaService.Close() // Clean up coordinator clients
			logger.Info("Meta server stopped")
		}()
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("gRPC servers stopped gracefully")
	case <-shutdownCtx.Done():
		logger.Warn("Shutdown timed out, forcing stop")
		dataServer.Stop()
		metaServer.Stop()
	}

	// Close VectorStore (flushes WAL/Snapshots)
	if err := vectorStore.Close(); err != nil {
		logger.Error("Failed to close vector store", zap.Error(err))
	} else {
		logger.Info("Vector store closed successfully")
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
