package main

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

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
	ListenUDS        string        `envconfig:"LISTEN_UDS" default:""` // Path to Unix Domain Socket
	NodeID           string        `envconfig:"NODE_ID" default:""`    // Optional override
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
	GRPCMaxRecvMsgSize        int    `envconfig:"GRPC_MAX_RECV_MSG_SIZE" default:"2147483647"`     // 2GB max
	GRPCMaxSendMsgSize        int    `envconfig:"GRPC_MAX_SEND_MSG_SIZE" default:"2147483647"`     // 2GB max
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
	StorageAsyncFsync     bool    `envconfig:"STORAGE_ASYNC_FSYNC" default:"true"`
	StorageDoPutBatchSize int     `envconfig:"STORAGE_DOPUT_BATCH_SIZE" default:"100"`
	StorageUseIOUring     bool    `envconfig:"STORAGE_USE_IOURING" default:"false"`
	StorageUseDirectIO    bool    `envconfig:"STORAGE_USE_DIRECT_IO" default:"false"`
	AutoSpillToDisk       bool    `envconfig:"AUTO_SPILL_DISK" default:"true"`
	SpillThresholdRatio   float64 `envconfig:"SPILL_THRESHOLD_RATIO" default:"0.70"`
	AutoQuantize          bool    `envconfig:"AUTO_QUANTIZE" default:"false"`
	AutoQuantizeThreshold int64   `envconfig:"AUTO_QUANTIZE_THRESHOLD" default:"100000"`
	AutoQuantizeBits      int     `envconfig:"AUTO_QUANTIZE_BITS" default:"4"`

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
	AutoScaleEnabled           bool `envconfig:"AUTOSCALE_ENABLED" default:"true"`
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

	// Security, RBAC, TLS/mTLS, and KMS Encryption Configuration (Items 3 & 11)
	// RBACEnabled is retained only so existing LONGBOW_RBAC_ENABLED values are
	// accepted. RBAC is always enforced and the value is deliberately ignored.
	RBACEnabled              bool          `envconfig:"RBAC_ENABLED" default:"false"`
	RBACBootstrapKey         string        `envconfig:"RBAC_BOOTSTRAP_KEY" default:""`
	StartupNamespacePolicies string        `envconfig:"STARTUP_NAMESPACE_POLICIES" default:""`
	SecretsProvider          string        `envconfig:"SECRETS_PROVIDER" default:"file"`
	SecretsPath              string        `envconfig:"SECRETS_PATH" default:"data/rbac_secrets"`
	SecretsPrefix            string        `envconfig:"SECRETS_PREFIX" default:"longbow/rbac"`
	SecretsVaultAddr         string        `envconfig:"SECRETS_VAULT_ADDR" default:""`
	SecretsVaultToken        string        `envconfig:"SECRETS_VAULT_TOKEN" default:""`
	SecretsVaultMount        string        `envconfig:"SECRETS_VAULT_MOUNT" default:"secret"`
	SecretsGCPProjectID      string        `envconfig:"SECRETS_GCP_PROJECT_ID" default:""`
	SecretsGCPToken          string        `envconfig:"SECRETS_GCP_TOKEN" default:""`
	SecretsAWSRegion         string        `envconfig:"SECRETS_AWS_REGION" default:""`
	SecretsAWSEndpoint       string        `envconfig:"SECRETS_AWS_ENDPOINT" default:""`
	TLSEnabled               bool          `envconfig:"TLS_ENABLED" default:"false"`
	TLSCertFile              string        `envconfig:"TLS_CERT_FILE" default:""`
	TLSKeyFile               string        `envconfig:"TLS_KEY_FILE" default:""`
	TLSCAFile                string        `envconfig:"TLS_CA_FILE" default:""`
	TLSRequireClientCert     bool          `envconfig:"TLS_REQUIRE_CLIENT_CERT" default:"false"`
	TLSMinVersion            string        `envconfig:"TLS_MIN_VERSION" default:"1.2"`
	TLSReloadInterval        time.Duration `envconfig:"TLS_RELOAD_INTERVAL" default:"5m"`
	EncryptionAtRest         bool          `envconfig:"ENCRYPTION_AT_REST" default:"false"`
	KMSProvider              string        `envconfig:"KMS_PROVIDER" default:"local"`
	KMSMasterKey             string        `envconfig:"KMS_MASTER_KEY" default:""`
	OIDCEnabled              bool          `envconfig:"OIDC_ENABLED" default:"false"`
	OIDCIssuers              string        `envconfig:"OIDC_ISSUERS" default:""`
}

// Global config instance for hook functions
var globalCfg Config
var globalIsReady atomic.Bool

// Config validation errors
var (
	// ErrInvalidListenAddr is a err invalid listen addr value.
	ErrInvalidListenAddr = errors.New("listen_addr cannot be empty")
	// ErrInvalidMetaAddr is a err invalid meta addr value.
	ErrInvalidMetaAddr = errors.New("meta_addr cannot be empty")
	// ErrInvalidMetricsAddr is a err invalid metrics addr value.
	ErrInvalidMetricsAddr = errors.New("metrics_addr cannot be empty")
	// ErrInvalidMaxMemory is a err invalid max memory value.
	ErrInvalidMaxMemory = errors.New("max_memory must be positive")
	// ErrInvalidDataPath is a err invalid data path value.
	ErrInvalidDataPath = errors.New("data_path cannot be empty")
	// ErrInvalidMaxWALSize is a err invalid max wal size value.
	ErrInvalidMaxWALSize = errors.New("max_wal_size must be positive")
	// ErrInvalidLogFormat is a err invalid log format value.
	ErrInvalidLogFormat = errors.New("log_format must be 'json' or 'console'")
	// ErrInvalidLogLevel is a err invalid log level value.
	ErrInvalidLogLevel = errors.New("log_level must be debug, info, warn, or error")
	// ErrInvalidKeepAliveTime is a err invalid keep alive time value.
	ErrInvalidKeepAliveTime = errors.New("keepalive_time must be positive")
	// ErrInvalidSecrets is a err invalid secrets value.
	ErrInvalidSecrets = errors.New("secrets provider configuration is invalid")
)

// ValidateConfig validates the configuration and returns an error if invalid
func ValidateConfig(cfg *Config) error {
	if cfg.ListenAddr == "" {
		return ErrInvalidListenAddr
	}
	if cfg.MetaAddr == "" {
		return ErrInvalidMetaAddr
	}
	if cfg.MetricsAddr == "" {
		return ErrInvalidMetricsAddr
	}
	if cfg.MaxMemory <= 0 {
		return ErrInvalidMaxMemory
	}
	if cfg.DataPath == "" {
		return ErrInvalidDataPath
	}
	if cfg.MaxWALSize <= 0 {
		return ErrInvalidMaxWALSize
	}
	if cfg.LogFormat != "json" && cfg.LogFormat != "console" {
		return ErrInvalidLogFormat
	}
	if cfg.LogLevel != "debug" && cfg.LogLevel != "info" && cfg.LogLevel != "warn" && cfg.LogLevel != "error" {
		return ErrInvalidLogLevel
	}
	if cfg.KeepAliveTime <= 0 {
		return ErrInvalidKeepAliveTime
	}
	switch strings.ToLower(strings.TrimSpace(cfg.SecretsProvider)) {
	case "", "file", "memory":
	case "vault":
		if strings.TrimSpace(cfg.SecretsVaultAddr) == "" {
			return fmt.Errorf("%w: vault requires LONGBOW_SECRETS_VAULT_ADDR", ErrInvalidSecrets)
		}
	case "gcp":
		if strings.TrimSpace(cfg.SecretsGCPProjectID) == "" {
			return fmt.Errorf("%w: gcp requires LONGBOW_SECRETS_GCP_PROJECT_ID", ErrInvalidSecrets)
		}
	case "aws":
		if strings.TrimSpace(cfg.SecretsAWSRegion) == "" {
			return fmt.Errorf("%w: aws requires LONGBOW_SECRETS_AWS_REGION", ErrInvalidSecrets)
		}
	default:
		return fmt.Errorf("%w: unknown provider %q", ErrInvalidSecrets, cfg.SecretsProvider)
	}
	if _, err := ParseStartupNamespacePolicies(cfg.StartupNamespacePolicies); err != nil {
		return err
	}
	return nil
}

// CalculateEvictionInterval computes the eviction check interval based on TTL
// Returns the interval to use for eviction checks, or 0 if TTL is disabled
func CalculateEvictionInterval(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return 0
	}

	checkInterval := time.Minute
	if ttl/10 < checkInterval {
		checkInterval = ttl / 10
	}
	if checkInterval < time.Second {
		checkInterval = time.Second
	}
	return checkInterval
}

// BuildKeepaliveParams creates gRPC keepalive server parameters from config
func BuildKeepaliveParams(cfg *Config) keepalive.ServerParameters {
	return keepalive.ServerParameters{
		Time:    cfg.KeepAliveTime,
		Timeout: cfg.KeepAliveTimeout,
	}
}

// BuildKeepalivePolicy creates gRPC keepalive enforcement policy from config
func BuildKeepalivePolicy(cfg *Config) keepalive.EnforcementPolicy {
	return keepalive.EnforcementPolicy{
		MinTime:             cfg.KeepAliveMinTime,
		PermitWithoutStream: cfg.KeepAlivePermitWithoutStream,
	}
}
