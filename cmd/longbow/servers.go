package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	lbflight "github.com/23skdu/longbow/internal/flight"
	"github.com/23skdu/longbow/internal/health"
	"github.com/23skdu/longbow/internal/limiter"
	"github.com/23skdu/longbow/internal/mesh"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/middleware"
	"github.com/23skdu/longbow/internal/security"
	"github.com/23skdu/longbow/internal/sharding"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store"
	"github.com/23skdu/longbow/pkg/version"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
)

// metricsListenAddr resolves the REST/metrics listener from the validated
// config, which already includes environment variables and CLI overrides.
// The legacy fallback is retained only for callers that construct an empty
// Config directly.
func metricsListenAddr(cfg *Config) string {
	if cfg == nil || strings.TrimSpace(cfg.MetricsAddr) == "" {
		return ":6000"
	}
	return cfg.MetricsAddr
}

func startHTTPMetricsServer(cfg *Config, vectorStore *store.VectorStore, certReloader *security.CertReloader, startTime time.Time, logger zerolog.Logger) *http.Server {
	metricsSrvChan := make(chan *http.Server, 1)
	go func() {
		metricsAddr := metricsListenAddr(cfg)
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())

		// Wire up health manager with component checkers
		hm := health.NewHealthManager(
			version.Version,
			logger,
			otel.Tracer("longbow.health"),
		)
		hm.RegisterChecker(health.NewStorageChecker(logger, otel.Tracer("longbow.health.storage")))
		hm.RegisterChecker(health.NewMetricsChecker(logger, otel.Tracer("longbow.health.metrics")))
		hm.RegisterChecker(health.NewLoggingChecker(logger, otel.Tracer("longbow.health.logging")))
		hm.RegisterChecker(health.NewTracingChecker(logger, otel.Tracer("longbow.health.tracing")))
		mux.Handle("/health", hm.HTTPHandler())

		mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
		})
		mux.HandleFunc("/progress", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")

			var limitBytes, usedBytes int64
			var ratio float64
			var isHighPressure, isBursting bool
			tuner := vectorStore.GetGCTuner()
			if tuner != nil {
				limitBytes = cfg.MaxMemory
				ratio = tuner.GetUtilizationRatio()
				usedBytes = int64(float64(limitBytes) * ratio)
				isHighPressure = tuner.IsHighPressure()
				isBursting = tuner.IsBursting()
			} else {
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				usedBytes = int64(m.HeapAlloc) // #nosec G115
			}

			backpressure := vectorStore.CheckIngestionBackpressure()
			delay := vectorStore.IngestionBackpressureDelay()

			type DatasetProgress struct {
				Name      string `json:"name"`
				NodeCount int64  `json:"node_count"`
				MaxLevel  int32  `json:"max_level"`
				OffHeap   bool   `json:"off_heap"`
			}
			var datasets []DatasetProgress
			activeNames := vectorStore.GetActiveDatasets()
			for _, name := range activeNames {
				nodeCount, maxLevel, isOffHeap, exists := vectorStore.GetDatasetIndexStats(name)
				if exists {
					datasets = append(datasets, DatasetProgress{
						Name:      name,
						NodeCount: nodeCount,
						MaxLevel:  maxLevel,
						OffHeap:   isOffHeap,
					})
				}
			}

			resp := map[string]any{
				"status":         "running",
				"uptime_seconds": time.Since(startTime).Seconds(),
				"memory": map[string]any{
					"limit_bytes":       limitBytes,
					"used_bytes":        usedBytes,
					"utilization_ratio": ratio,
					"high_pressure":     isHighPressure,
					"bursting":          isBursting,
				},
				"ingestion": map[string]any{
					"queue_len":       vectorStore.IngestionQueueLen(),
					"queue_cap":       vectorStore.IngestionQueueCap(),
					"backpressure":    backpressure,
					"backpressure_ms": delay.Milliseconds(),
				},
				"datasets": datasets,
			}

			if err := json.NewEncoder(w).Encode(resp); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(err.Error()))
			}
		})
		// pprof endpoints (Phase 6)
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

		// /diagnostics: returns SIMD impl details and key memory config.
		// Use: curl http://ancalagon:<port>/diagnostics | jq .
		mux.HandleFunc("/diagnostics", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			resp := simd.GetImplementationDetails()
			resp["max_memory_bytes"] = cfg.MaxMemory
			resp["adaptive_m_max_factor"] = os.Getenv("LONGBOW_ADAPTIVE_M_MAX_FACTOR")
			resp["max_m0"] = os.Getenv("LONGBOW_MAX_M0")
			if err := json.NewEncoder(w).Encode(resp); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
			}
		})

		vectorStore.RegisterHTTPHandlers(mux)

		metricsListener, err := listenWithReuseAddr("tcp", metricsAddr)
		if err != nil {
			logger.Error().Err(err).Str("addr", metricsAddr).Msg("Metrics server listen failed")
			return
		}
		srv := &http.Server{
			Handler:      mux,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
		}
		if certReloader != nil {
			httpTLS, tlsErr := certReloader.BuildServerTLSConfig()
			if tlsErr != nil {
				logger.Error().Err(tlsErr).Msg("Failed to build HTTP TLS config")
				return
			}
			srv.TLSConfig = httpTLS
		}
		metricsSrvChan <- srv

		logger.Info().Str("addr", metricsAddr).Bool("tls", srv.TLSConfig != nil).Msg("Metrics and HTTP REST/GraphQL server starting")
		var serveErr error
		if srv.TLSConfig != nil {
			serveErr = srv.ServeTLS(metricsListener, "", "")
		} else {
			serveErr = srv.Serve(metricsListener)
		}
		if serveErr != nil && serveErr != http.ErrServerClosed {
			logger.Error().Err(serveErr).Msg("Metrics/HTTP server failed")
		}
	}()
	return <-metricsSrvChan
}

func initMeshAndRing(cfg *Config, vectorStore *store.VectorStore, logger zerolog.Logger) (*sharding.RingManager, *sharding.RequestForwarder, *mesh.Gossip) {
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
	var gossipSrv *mesh.Gossip
	if cfg.GossipEnabled {
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
			gossipSrv = g
		}
	}

	// Initialize Request Forwarder with RingManager for address resolution
	fwdCfg := sharding.DefaultForwarderConfig()
	forwarder := sharding.NewRequestForwarder(&fwdCfg, ringManager)

	return ringManager, forwarder, gossipSrv
}

func startFlightAndRDMAServers(
	cfg *Config,
	vectorStore *store.VectorStore,
	mem *store.PooledAllocator,
	ringManager *sharding.RingManager,
	forwarder *sharding.RequestForwarder,
	authValidator security.AccessValidator,
	auditLog *security.AuditLogger,
	certReloader *security.CertReloader,
	logger zerolog.Logger,
) (*grpc.Server, *grpc.Server, *store.MetaServer, *lbflight.RDMAServer, error) {
	// gRPC Server Options - using env-configurable message sizes and window sizes
	serverOpts := make([]grpc.ServerOption, 0, 10)
	if certReloader != nil {
		if credsOpt, err := certReloader.GRPCServerOption(); err != nil {
			return nil, nil, nil, nil, err
		} else if credsOpt != nil {
			serverOpts = append(serverOpts, credsOpt)
		}
	}
	serverOpts = append(serverOpts,
		grpc.MaxRecvMsgSize(cfg.GRPCMaxRecvMsgSize),
		grpc.MaxSendMsgSize(cfg.GRPCMaxSendMsgSize),
		grpc.InitialWindowSize(cfg.GRPCInitialWindowSize),
		grpc.InitialConnWindowSize(cfg.GRPCInitialConnWindowSize),
		grpc.MaxConcurrentStreams(cfg.GRPCMaxConcurrentStreams),
		grpc.KeepaliveParams(BuildKeepaliveParams(cfg)),
		grpc.KeepaliveEnforcementPolicy(BuildKeepalivePolicy(cfg)),
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
			middleware.AuthUnaryInterceptor(authValidator, auditLog),
			middleware.AdmissionInterceptor(vectorStore.GetAdmissionController()),
			middleware.CircuitBreakerInterceptor(),
			rateLimiter.UnaryInterceptor(),
			sharding.PartitionProxyInterceptor(ringManager, forwarder),
		),
		grpc.ChainStreamInterceptor(
			middleware.AuthStreamInterceptor(authValidator, auditLog),
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

	dataLisBase, err := listenWithReuseAddr("tcp", cfg.ListenAddr)
	if err != nil {
		logger.Error().
			Err(err).
			Str("addr", cfg.ListenAddr).
			Msg("Failed to listen for Data Server")
		return nil, nil, nil, nil, err
	}
	tcpLis, ok := dataLisBase.(*net.TCPListener)
	if !ok {
		return nil, nil, nil, nil, fmt.Errorf("unexpected listener type: %T", dataLisBase)
	}
	dataLis := store.NewTCPNoDelayListener(tcpLis)

	// --- Meta Server Setup ---
	metaServer := grpc.NewServer(serverOpts...)
	metaService := store.NewMetaServer(vectorStore)
	flight.RegisterFlightServiceServer(metaServer, metaService)

	metaLisBase, err := listenWithReuseAddr("tcp", cfg.MetaAddr)
	if err != nil {
		logger.Error().
			Err(err).
			Str("addr", cfg.MetaAddr).
			Msg("Failed to listen for Meta Server")
		return nil, nil, nil, nil, err
	}

	metaTcpLis, ok := metaLisBase.(*net.TCPListener)
	if !ok {
		return nil, nil, nil, nil, fmt.Errorf("unexpected listener type: %T", metaLisBase)
	}
	metaLis := store.NewTCPNoDelayListener(metaTcpLis)

	// Start Data Server
	go func() {
		logger.Info().Str("addr", cfg.ListenAddr).Msg("Listening for Data gRPC connections")
		if err := dataServer.Serve(dataLis); err != nil {
			logger.Error().Err(err).Msg("Data gRPC server failed")
		}
	}()

	// Start UDS Data Server if configured
	if cfg.ListenUDS != "" {
		if err := os.RemoveAll(cfg.ListenUDS); err != nil {
			logger.Error().Err(err).Str("path", cfg.ListenUDS).Msg("Failed to remove existing UDS socket")
		}
		udsLisBase, err := net.Listen("unix", cfg.ListenUDS)
		if err != nil {
			logger.Error().Err(err).Str("path", cfg.ListenUDS).Msg("Failed to listen on UDS")
		} else {
			// Ensure the socket is accessible by owner and group
			// Use 0660 for security; deploy with appropriate group membership for multi-process IPC
			if err := os.Chmod(cfg.ListenUDS, 0660); err != nil { // #nosec G302 - UDS requires group access
				logger.Warn().Err(err).Str("path", cfg.ListenUDS).Msg("Failed to set UDS socket permissions")
			}
			udsLis := store.NewUDSListener(udsLisBase)
			go func() {
				logger.Info().Str("path", cfg.ListenUDS).Msg("Listening for Data gRPC connections (UDS)")
				if err := dataServer.Serve(udsLis); err != nil {
					logger.Error().Err(err).Msg("UDS Data gRPC server failed")
				}
			}()
		}
	}

	// Start Meta Server
	go func() {
		logger.Info().Str("addr", cfg.MetaAddr).Msg("Listening for Meta gRPC connections")
		if err := metaServer.Serve(metaLis); err != nil {
			logger.Error().Err(err).Msg("Meta gRPC server failed")
		}
	}()

	// Start RDMA Server if enabled
	var rdmaSrv *lbflight.RDMAServer
	if cfg.RDMAEnabled {
		rdmaSrv = lbflight.NewRDMAServer(true)
		go func() {
			addr := fmt.Sprintf("0.0.0.0:%d", cfg.RDMAPort)
			logger.Info().Str("addr", addr).Msg("Starting RDMA Zero-Copy Server")
			if err := rdmaSrv.StartRDMAListener(addr, serverOpts...); err != nil {
				logger.Error().Err(err).Msg("RDMA server failed")
			}
		}()
	}

	return dataServer, metaServer, metaService, rdmaSrv, nil
}

// listenWithReuseAddr creates a TCP listener with SO_REUSEADDR set,
// allowing the server to bind to a port that may still be in TIME_WAIT
// from a previous instance. This is critical for benchmark rapid restart.
func listenWithReuseAddr(network, addr string) (net.Listener, error) {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			return c.Control(func(fd uintptr) {
				_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1) // best-effort
			})
		},
	}
	return lc.Listen(context.Background(), network, addr)
}
