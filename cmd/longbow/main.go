package main

import (
	"context"
	"errors"
	_ "expvar" // Register expvar handlers
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/23skdu/longbow/internal/logging"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

func main() {
	if err := run(); err != nil {
		reportFatalError(os.Stderr, err)
		os.Exit(1)
	}
}

// reportFatalError makes startup failures visible in container logs instead of
// exiting silently. run() returns errors from many subsystems; callers of the
// binary must be able to see why it stopped.
func reportFatalError(w io.Writer, err error) {
	fmt.Fprintf(w, "Longbow exited with error: %v\n", err)
}

func run() error {
	startTime := time.Now()

	// Load .env file if it exists (do this before logger init to read LOG_* vars)
	_ = godotenv.Load()

	if err := envconfig.Process("LONGBOW", &globalCfg); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to process config: %v\n", err)
		return err
	}
	cfg := globalCfg

	// Parse CLI flags (override env vars). Must happen after envconfig for precedence.
	if err := parseCLIFlags(&cfg, os.Args[1:], os.Stderr); err != nil {
		if errors.Is(err, ErrVersionRequested) || errors.Is(err, ErrHelpRequested) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "Failed to parse flags: %v\n", err)
		os.Exit(1)
	}

	// Handle signals for graceful shutdown and hot reload
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	vectorStore, mem, ballast := initVectorStoreRuntime(ctx, &cfg, logger)
	defer runtime.KeepAlive(ballast)

	configureStoreAndMath(&cfg, vectorStore, logger)
	wsServer, mqExporter := initFeatureSubsystems(&cfg, vectorStore, logger)
	_ = mqExporter

	tp := initTracer()
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			logger.Error().Err(err).Msg("Error shutting down tracer provider")
		}
	}()

	authValidator, auditLog, certReloader, err := initAuthAndTLS(&cfg, vectorStore, logger)
	if err != nil {
		return err
	}
	if certReloader != nil {
		defer certReloader.Stop()
	}

	startWebSocketServer(&cfg, wsServer, authValidator, auditLog, logger)

	metricsSrv := startHTTPMetricsServer(&cfg, vectorStore, certReloader, startTime, logger)

	ringManager, forwarder, gossipSrv := initMeshAndRing(&cfg, vectorStore, logger)
	if gossipSrv != nil {
		defer gossipSrv.Stop()
	}

	dataServer, metaServer, metaService, rdmaSrv, err := startFlightAndRDMAServers(
		&cfg, vectorStore, mem, ringManager, forwarder, authValidator, auditLog, certReloader, logger,
	)
	if err != nil {
		return err
	}

	// System is now ready to receive traffic
	globalIsReady.Store(true)

	// Step 8: Graceful Shutdown (Phase 6)
	<-ctx.Done()
	logger.Info().Msg("Shutdown signal received")

	return gracefulShutdown(logger, vectorStore, wsServer, mqExporter, rdmaSrv, dataServer, metaServer, metaService, metricsSrv)
}
