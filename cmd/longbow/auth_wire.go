package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/23skdu/longbow/internal/security"
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
	// Initialize RBAC, OIDC Federation & Audit Logging before HTTP/gRPC server registration (Item 3)
	if cfg.RBACEnabled {
		rbacMgr := store.NewRBACManager()
		vectorStore.EnableRBAC(rbacMgr)
		if _, bootstrapRaw, err := rbacMgr.CreateAPIKeyWithOptions(store.APIKeyOptions{
			Name: "bootstrap-admin",
			Role: store.RoleAdmin,
		}); err == nil {
			logger.Info().Str("key_id", "bootstrap-admin").Int("key_len", len(bootstrapRaw)).Msg("RBAC enabled with bootstrap admin API key")
		}
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
	var authValidator security.AccessValidator
	var auditLog *security.AuditLogger
	if cfg.RBACEnabled || cfg.OIDCEnabled {
		authValidator = vectorStore.DynamicAccessValidator()
		auditLog = vectorStore.AuditLogger()
	}

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
