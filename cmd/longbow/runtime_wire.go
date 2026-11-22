package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/23skdu/longbow/internal/autoscale"
	"github.com/23skdu/longbow/internal/gc"
	"github.com/23skdu/longbow/internal/gpu"
	"github.com/23skdu/longbow/internal/mathutil"
	lbmem "github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/security"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store"
	"github.com/23skdu/longbow/internal/tensor"
	"github.com/rs/zerolog"
)

func initVectorStoreRuntime(ctx context.Context, cfg *Config, logger zerolog.Logger) (*store.VectorStore, *store.PooledAllocator, []byte) {
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
		tuner = lbmem.NewGCTuner(cfg.MaxMemory, cfg.GOGC, 80, &logger)
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
	if err := os.Setenv("GODEBUG", "madvdontneed=1"); err != nil {
		logger.Warn().Err(err).Msg("Failed to set GODEBUG")
	}
	gcConfig := gc.DefaultAdaptiveGCConfig()
	gcConfig.Enabled = (cfg.MaxMemory <= 0)
	gcConfig.MinGOGC = 20
	gcConfig.MaxGOGC = 100
	vectorStore.EnableAdaptiveGC(gcConfig)

	vectorStore.SetGCTuner(tuner)
	vectorStore.SetAutoScaler(scaler)
	scaler.SetReconciler(vectorStore)

	if cfg.AutoScaleEnabled {
		go scaler.Start(ctx)
		logger.Info().Msg("Serverless Auto-Scaler monitoring started")
	} else {
		logger.Info().Msg("Auto-scaler disabled by configuration")
	}

	return vectorStore, mem, ballast
}

func configureStoreAndMath(cfg *Config, vectorStore *store.VectorStore, logger zerolog.Logger) {
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

	if cfg.AutoSpillToDisk {
		_ = os.Setenv("LONGBOW_AUTO_SPILL_DISK", "1")
	} else {
		_ = os.Setenv("LONGBOW_AUTO_SPILL_DISK", "0")
	}
	if cfg.SpillThresholdRatio > 0 {
		_ = os.Setenv("LONGBOW_SPILL_THRESHOLD_RATIO", fmt.Sprintf("%.2f", cfg.SpillThresholdRatio))
	}
	if cfg.AutoQuantize {
		_ = os.Setenv("LONGBOW_AUTO_QUANTIZE", "1")
		logger.Info().
			Int64("threshold", cfg.AutoQuantizeThreshold).
			Int("bits", cfg.AutoQuantizeBits).
			Msg("Automatic TurboQuant standardization enabled")
	}
	if cfg.AutoQuantizeThreshold > 0 {
		_ = os.Setenv("LONGBOW_AUTO_QUANTIZE_THRESHOLD", strconv.FormatInt(cfg.AutoQuantizeThreshold, 10))
	}
	if cfg.AutoQuantizeBits > 0 {
		_ = os.Setenv("LONGBOW_AUTO_QUANTIZE_BITS", strconv.Itoa(cfg.AutoQuantizeBits))
	}

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

	// GPU Binary Diagnostic check
	gpu.CheckBinaryDiagnostic(&logger)

	// SIMD Kernel Validation: run a known-answer test for the selected implementation.
	// Logs the detected SIMD implementation at INFO for remote diagnosis.
	// If the KAT fails (e.g., AVX-512 trapping in a VM), logs a warning and continues
	// on the generic path rather than crashing — the server is still functional.
	simdDetails := simd.GetImplementationDetails()
	arch, _ := simdDetails["arch"].(string)
	vendor, _ := simdDetails["vendor"].(string)
	hasAVX512, _ := simdDetails["has_avx512"].(bool)
	hasAVX2, _ := simdDetails["has_avx2"].(bool)
	hasNEON, _ := simdDetails["has_neon"].(bool)
	logger.Info().
		Str("simd_impl", simd.GetImplementation()).
		Str("arch", arch).
		Str("vendor", vendor).
		Bool("has_avx512", hasAVX512).
		Bool("has_avx2", hasAVX2).
		Bool("has_neon", hasNEON).
		Msg("SIMD dispatch initialized")
	if err := simd.ValidateSIMDKernels(); err != nil {
		logger.Warn().Err(err).Msg("SIMD kernel self-test failed; distance computations may be incorrect")
	} else {
		logger.Info().Str("impl", simd.GetImplementation()).Msg("SIMD kernel self-test passed")
	}

	// Initialize tensor math dispatch: use hardware-accelerated math
	// when a SIMD implementation was detected.
	tensor.InitMathDispatch(simd.GetImplementation() != "generic")

	// Part 7: LONGBOW_FLOAT64_EXCLUDE_EMLGO disables emlgo for float64 batch ops.
	// Default is excluded (true) when unset: emlgo float64 at 500k added +47%
	// memory (10632 vs 7172 MB) with no dense-QPS win (roadmap.md §2).
	// Accepts true/1/yes (exclude) and false/0/no (include).
	f64ExcludeVal := strings.ToLower(strings.TrimSpace(os.Getenv("LONGBOW_FLOAT64_EXCLUDE_EMLGO")))
	switch f64ExcludeVal {
	case "false", "0", "no", "off":
		mathutil.SetFloat64Excluded(false)
		logger.Info().Msg("emlgo enabled for float64 batch operations (LONGBOW_FLOAT64_EXCLUDE_EMLGO=false)")
	default:
		// unset, true, 1, yes, or any other value → exclude (safe default)
		mathutil.SetFloat64Excluded(true)
		if f64ExcludeVal != "" {
			logger.Info().Str("value", f64ExcludeVal).Msg("emlgo excluded for float64 batch operations")
		}
	}

	// Part 10: LONGBOW_MATH_DISPATCH conditional dispatch routing.
	// Values: auto (default), emlgo, standard. ApplyDispatchConfig reads the
	// env var and calls SetDispatchMode → SetMathImpl/SetBackend so the mode
	// actually controls the math backend (previously it was log-only).
	tensor.ApplyDispatchConfig()
	switch tensor.GetDispatchMode() {
	case tensor.DispatchEML:
		logger.Info().Msg("math dispatch: forced emlgo for all operations")
	case tensor.DispatchStandard:
		logger.Info().Msg("math dispatch: forced standard math for all operations")
	case tensor.DispatchAuto:
		logger.Info().
			Int("min_eml_vectors", tensor.MinEMLVectorCount).
			Msg("math dispatch: auto routing (emlgo for complex/TQ>=50k, standard otherwise)")
	}

	// Configure GPU acceleration
	if cfg.GPUEnabled {
		detectedBackend := gpu.DetectGPUBackend()

		if detectedBackend == gpu.BackendCUDA || detectedBackend == gpu.BackendMetal {
			vectorStore.SetGPUConfig(detectedBackend, int32(cfg.GPUDeviceID)) // #nosec G115
			// Item 34: per-kernel device time, off unless asked for. It
			// records an event pair per launch, so it is a measurement mode
			// rather than something to leave on in production.
			if gpu.KernelProfilingEnabled() {
				logger.Info().
					Str("backend", detectedBackend.String()).
					Msg("GPU kernel profiling enabled (LONGBOW_GPU_KERNEL_PROF): every launch is timed with CUDA events, which costs throughput")
			}
			logger.Info().
				Str("backend", detectedBackend.String()).
				Bool("enabled", cfg.GPUEnabled).
				Int("device", cfg.GPUDeviceID).
				Msg("GPU acceleration configured")
			logger.Warn().Msg("GPU acceleration is explicitly enabled. It is highly recommended to enable GPU acceleration only for datasets exceeding 500K vectors to avoid kernel launch overhead on smaller collections.")
		} else {
			logger.Warn().
				Str("detected_backend", detectedBackend.String()).
				Msg("No GPU detected, falling back to CPU")
			vectorStore.SetGPUConfig(gpu.BackendCPU, 0)
		}
	}

	// Start eviction ticker if TTL is enabled
	if checkInterval := CalculateEvictionInterval(cfg.TTL); checkInterval > 0 {
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
}

func initFeatureSubsystems(cfg *Config, vectorStore *store.VectorStore, logger zerolog.Logger) (*store.WebSocketServer, *store.MessageQueueExporter) {
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

	// Create the WebSocket server here, but do not start it until RBAC has
	// been initialized. startWebSocketServer installs the validator first.
	var wsServer *store.WebSocketServer
	if cfg.WebSocketEnabled && cdc != nil {
		wsServer = store.NewWebSocketServer(logger, cdc)
		logger.Info().Str("addr", cfg.WebSocketAddr).Msg("WebSocket server created")
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
	if cfg.LearnedIndexEnabled {
		indexPredictor := store.NewIndexPerformancePredictor(logger, store.LearnedIndexConfig{
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
			learnedWithOllama := store.NewLearnedIndexWithOllama(logger, indexPredictor, ollamaConfig)
			vectorStore.SetIndexPredictor(indexPredictor)
			vectorStore.SetOllamaPredictor(learnedWithOllama)
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

	// Initialize Temporal Index (Part 22)
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
		vectorStore.SetTemporalIndex(temporalConfig)
		logger.Info().
			Bool("version_history", cfg.TemporalVersionHistory).
			Int("max_versions", cfg.TemporalMaxVersions).
			Dur("retention_period", cfg.TemporalRetentionPeriod).
			Bool("ttl_enabled", cfg.TemporalTTLEnabled).
			Bool("aggregation_enabled", cfg.TemporalAggregationEnabled).
			Msg("Temporal index initialized")
	}

	// Start background indexing and ingestion workers
	indexingWorkers := runtime.NumCPU() / 2
	if env := os.Getenv("LONGBOW_INDEXING_WORKERS"); env != "" {
		if w, err := strconv.Atoi(env); err == nil && w > 0 {
			indexingWorkers = w
		}
	} else if indexingWorkers < 2 {
		indexingWorkers = 2
	}
	vectorStore.StartIndexingWorkers(indexingWorkers)
	vectorStore.StartIngestionWorkers(cfg.IngestionWorkerCount)

	return wsServer, mqExporter
}

// startWebSocketServer installs RBAC before opening the listener. Starting the
// listener earlier would expose CDC data before namespaces and keys are loaded.
func startWebSocketServer(cfg *Config, wsServer *store.WebSocketServer, validator security.AccessValidator, audit *security.AuditLogger, logger zerolog.Logger) {
	if wsServer == nil {
		return
	}
	wsServer.SetAuth(validator, audit)
	go func() {
		if err := wsServer.Start(cfg.WebSocketAddr); err != nil {
			logger.Error().Err(err).Msg("WebSocket server failed")
		}
	}()
	logger.Info().Str("addr", cfg.WebSocketAddr).Msg("WebSocket server started")
}
