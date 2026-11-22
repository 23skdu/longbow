package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/23skdu/longbow/internal/security"
	"github.com/23skdu/longbow/internal/security/secrets"
	"github.com/23skdu/longbow/internal/store"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

func initAuthAndTLS(cfg *Config, vectorStore *store.VectorStore, logger zerolog.Logger) (security.AccessValidator, *security.AuditLogger, *security.CertReloader, error) {
	// RBAC is always enforced. API keys are stored in the configured provider
	// and rehydrated at startup so legitimate credentials survive restarts.
	secretStore, err := secrets.New(context.Background(), secrets.Config{
		Provider: cfg.SecretsProvider,
		Path:     cfg.SecretsPath,
		Prefix:   cfg.SecretsPrefix,

		VaultAddr:  cfg.SecretsVaultAddr,
		VaultToken: cfg.SecretsVaultToken,
		VaultMount: cfg.SecretsVaultMount,

		GCPProjectID: cfg.SecretsGCPProjectID,
		GCPToken:     cfg.SecretsGCPToken,

		AWSRegion:   cfg.SecretsAWSRegion,
		AWSEndpoint: cfg.SecretsAWSEndpoint,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("configure RBAC secret store: %w", err)
	}
	rbacMgr, err := store.NewRBACManagerWithStore(context.Background(), secretStore)
	if err != nil {
		return nil, nil, nil, err
	}
	vectorStore.EnableRBAC(rbacMgr)

	if strings.TrimSpace(cfg.RBACBootstrapKey) != "" {
		bootstrapOpts := store.APIKeyOptions{
			KeyID: "bootstrap-admin",
			Name:  "bootstrap-admin",
			Role:  store.RoleAdmin,
		}
		if _, err := rbacMgr.RegisterAPIKeyWithSecret(bootstrapOpts, cfg.RBACBootstrapKey); err != nil {
			return nil, nil, nil, fmt.Errorf("invalid LONGBOW_RBAC_BOOTSTRAP_KEY: %w", err)
		}
		logger.Info().Str("provider", cfg.SecretsProvider).Msg("RBAC enforced with configured bootstrap admin API key")
	} else if !rbacMgr.HasGlobalAdmin() {
		return nil, nil, nil, fmt.Errorf(
			"RBAC is always enforced: set LONGBOW_RBAC_BOOTSTRAP_KEY or provision an admin API key in the %s secrets provider",
			cfg.SecretsProvider)
	}
	// Durable ACLs were rehydrated with the API keys above. Recreate their
	// namespace objects so empty namespaces are available immediately after a
	// restart, not only namespaces that contain replayed datasets.
	durablePolicies := rbacMgr.ListNamespacePolicies()
	for _, policy := range durablePolicies {
		if err := vectorStore.EnsureNamespace(policy.Namespace); err != nil {
			return nil, nil, nil, fmt.Errorf("restore durable namespace %q: %w", policy.Namespace, err)
		}
	}
	if len(durablePolicies) > 0 {
		logger.Info().Int("namespaces", len(durablePolicies)).Msg("Restored durable namespace ACLs from secret store")
	}

	startupPolicies, err := ParseStartupNamespacePolicies(cfg.StartupNamespacePolicies)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, policy := range startupPolicies {
		if err := vectorStore.ProvisionNamespaceWithPolicy(policy); err != nil {
			return nil, nil, nil, fmt.Errorf("provision startup namespace %q: %w", policy.Namespace, err)
		}
	}
	if len(startupPolicies) > 0 {
		logger.Info().Int("namespaces", len(startupPolicies)).Msg("Provisioned startup namespace ACLs")
	}

	// WAL and snapshot replay runs before RBAC rehydration and can recreate
	// datasets for namespaces that were deleted or declared runtime-only.
	// Drop those datasets before any server starts accepting traffic.
	keepNamespaces := map[string]struct{}{"default": {}}
	for _, name := range vectorStore.ListNamespaces() {
		keepNamespaces[name] = struct{}{}
	}
	keepDatasets := map[string]struct{}{"default": {}}
	for _, policy := range rbacMgr.ListNamespacePolicies() {
		if !policy.Ephemeral {
			keepDatasets[policy.Namespace] = struct{}{}
		}
	}
	pruned, err := vectorStore.PruneNamespaces(keepNamespaces, keepDatasets)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("prune replayed namespaces: %w", err)
	}
	if pruned > 0 {
		logger.Info().Int("datasets", pruned).Msg("Pruned replayed datasets without durable namespace ACLs")
	}

	if cfg.OIDCEnabled {
		var issuers []security.OIDCIssuerConfig
		if strings.TrimSpace(cfg.OIDCIssuers) != "" {
			if err := json.Unmarshal([]byte(cfg.OIDCIssuers), &issuers); err != nil {
				logger.Error().Err(err).Msg("Failed to parse LONGBOW_OIDC_ISSUERS JSON")
				return nil, nil, nil, err
			}
		}
		oidcMgr := security.NewOIDCManager(security.OIDCConfig{
			Enabled: true,
			Issuers: issuers,
		})
		vectorStore.EnableOIDC(oidcMgr)
		logger.Info().Int("issuers", len(issuers)).Msg("OIDC federation enabled")
	}
	authValidator := vectorStore.DynamicAccessValidator()
	auditLog := vectorStore.AuditLogger()

	// Initialize TLS / mTLS Certificate Reloader before HTTP/gRPC servers start (Item 11)
	var certReloader *security.CertReloader
	if cfg.TLSEnabled {
		tlsCfg := security.TLSConfig{
			Enabled:           cfg.TLSEnabled,
			CertFile:          cfg.TLSCertFile,
			KeyFile:           cfg.TLSKeyFile,
			CAFile:            cfg.TLSCAFile,
			RequireClientCert: cfg.TLSRequireClientCert,
			MinVersion:        cfg.TLSMinVersion,
			ReloadInterval:    cfg.TLSReloadInterval,
		}
		var err error
		certReloader, err = security.NewCertReloader(tlsCfg)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to initialize TLS CertReloader")
			return nil, nil, nil, err
		}
		certReloader.StartAutoReload(cfg.TLSReloadInterval)
	}

	return authValidator, auditLog, certReloader, nil
}

func initTracer() *sdktrace.TracerProvider {
	// Define resource attributes
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String("longbow"),
	)

	// Create TracerProvider options
	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
	}

	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	tracingEnabled := os.Getenv("LONGBOW_TRACING_ENABLED") == "true" || os.Getenv("LONGBOW_TRACING_ENABLED") == "1"

	// Only connect to OTLP collector if an endpoint or explicit tracing flag is set
	if endpoint != "" || tracingEnabled {
		exporter, err := otlptracegrpc.New(context.Background(),
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			fmt.Printf("WARN: Failed to create OTLP trace exporter, tracing disabled: %v\n", err)
		} else if exporter != nil {
			opts = append(opts, sdktrace.WithBatcher(exporter))
		}
	}

	tp := sdktrace.NewTracerProvider(opts...)

	// Register globals
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return tp
}
