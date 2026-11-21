package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/23skdu/longbow/client"
	"github.com/23skdu/longbow/internal/security"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func getCLIAuthClient(uri, apiKey, tenant, caCert, certFile, keyFile string) *client.SmartClient {
	sc, err := client.NewSmartClientWithConfig(client.ClientConfig{
		Addr:     uri,
		APIKey:   apiKey,
		TenantID: tenant,
		TLS: security.TLSConfig{
			Enabled:  caCert != "" || certFile != "" || strings.HasPrefix(uri, "grpcs://"),
			CAFile:   caCert,
			CertFile: certFile,
			KeyFile:  keyFile,
		},
	})
	if err != nil {
		log.Fatalf("Failed to connect to Longbow: %v", err)
	}
	return sc
}

func runCreateAPIKey(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("create-api-key", flag.ExitOnError)
	name := fs.String("name", "", "API key name")
	role := fs.String("role", "reader", "Role: admin, tenant-admin, writer (read-write), ingest-only, reader (read-only)")
	tenant := fs.String("tenant", "", "Tenant ID / namespace")
	datasets := fs.String("datasets", "", "Comma-separated allowed datasets")
	rowFilters := fs.String("row-filters", "", "Optional JSON object of RLS row filters (e.g. '{\"department\":\"eng\"}')")
	ttl := fs.Int64("ttl", 0, "Optional TTL in seconds")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "Admin API key")
	_ = fs.Parse(args)

	if *name == "" {
		log.Fatal("Usage: longbow-cli create-api-key -name <name> [-role <role>] [-tenant <ns>] [-datasets d1,d2] [-row-filters '<json>'] [-ttl 0]")
	}
	sc := getCLIAuthClient(*uri, *apiKey, *tenant, "", "", "")
	defer func() { _ = sc.Close() }()

	var dsList []string
	if *datasets != "" {
		dsList = strings.Split(*datasets, ",")
	}
	var rfMap map[string]string
	if strings.TrimSpace(*rowFilters) != "" {
		if err := json.Unmarshal([]byte(*rowFilters), &rfMap); err != nil {
			log.Fatalf("Invalid -row-filters JSON: %v", err)
		}
	}
	body, _ := json.Marshal(map[string]any{
		"name":        *name,
		"role":        *role,
		"tenant_id":   *tenant,
		"namespace":   *tenant,
		"datasets":    dsList,
		"row_filters": rfMap,
		"ttl_seconds": *ttl,
	})
	executeCLIAction(sc.WithAuthContext(ctx), sc, "CreateAPIKey", body)
}

func runRotateAPIKey(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("rotate-api-key", flag.ExitOnError)
	targetKey := fs.String("key", "", "Existing API key to rotate")
	grace := fs.Int64("grace", 3600, "Grace period in seconds")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "Admin API key")
	_ = fs.Parse(args)
	if *targetKey == "" {
		log.Fatal("Usage: longbow-cli rotate-api-key -key <raw-key> [-grace 3600]")
	}
	sc := getCLIAuthClient(*uri, *apiKey, "", "", "", "")
	defer func() { _ = sc.Close() }()
	body, _ := json.Marshal(map[string]any{
		"api_key":              *targetKey,
		"grace_period_seconds": *grace,
	})
	executeCLIAction(sc.WithAuthContext(ctx), sc, "RotateAPIKey", body)
}

func runRevokeAPIKey(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("revoke-api-key", flag.ExitOnError)
	targetKey := fs.String("key", "", "API key or KeyID to revoke")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "Admin API key")
	_ = fs.Parse(args)
	if *targetKey == "" {
		log.Fatal("Usage: longbow-cli revoke-api-key -key <key-or-id>")
	}
	sc := getCLIAuthClient(*uri, *apiKey, "", "", "", "")
	defer func() { _ = sc.Close() }()
	body, _ := json.Marshal(map[string]any{"api_key": *targetKey})
	executeCLIAction(sc.WithAuthContext(ctx), sc, "RevokeAPIKey", body)
}

func runListAPIKeys(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("list-api-keys", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "Admin API key")
	_ = fs.Parse(args)
	sc := getCLIAuthClient(*uri, *apiKey, "", "", "", "")
	defer func() { _ = sc.Close() }()
	executeCLIAction(sc.WithAuthContext(ctx), sc, "ListAPIKeys", nil)
}

func runBackup(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("backup", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset to back up")
	dest := fs.String("dest", "", "Destination path or URI (file://, s3://, gs://)")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "API key")
	_ = fs.Parse(args)
	if *dataset == "" {
		log.Fatal("Usage: longbow-cli backup -dataset <name> [-dest <uri>]")
	}
	sc := getCLIAuthClient(*uri, *apiKey, "", "", "", "")
	defer func() { _ = sc.Close() }()
	body, _ := json.Marshal(map[string]any{"dataset": *dataset, "destination": *dest})
	executeCLIAction(sc.WithAuthContext(ctx), sc, "TriggerBackup", body)
}

func runRestore(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("restore", flag.ExitOnError)
	backupID := fs.String("backup-id", "", "Backup ID to restore")
	dataset := fs.String("dataset", "", "Dataset name")
	pitr := fs.Int64("pitr-ts", 0, "Optional point-in-time Unix nanoseconds")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "API key")
	_ = fs.Parse(args)
	if *dataset == "" || *backupID == "" {
		log.Fatal("Usage: longbow-cli restore -backup-id <id> -dataset <name> [-pitr-ts <unix-ns>]")
	}
	sc := getCLIAuthClient(*uri, *apiKey, "", "", "", "")
	defer func() { _ = sc.Close() }()
	body, _ := json.Marshal(map[string]any{
		"backup_id":      *backupID,
		"dataset":        *dataset,
		"pitr_timestamp": *pitr,
	})
	executeCLIAction(sc.WithAuthContext(ctx), sc, "RestoreBackup", body)
}

func formatCLIAuthError(actionType string, err error) string {
	st, ok := status.FromError(err)
	if ok {
		switch st.Code() {
		case codes.Unauthenticated:
			return fmt.Sprintf("Authentication failed on %s (%s): %s\nHint: pass -api-key / LONGBOW_API_KEY or refresh your federated OIDC JWT via `longbow-cli oidc-exchange`.", actionType, st.Code(), st.Message())
		case codes.PermissionDenied:
			return fmt.Sprintf("Authorization denied on %s (%s): %s\nHint: verify your RBAC role/OIDC groups, target namespace, and allowed datasets.", actionType, st.Code(), st.Message())
		}
	}
	return fmt.Sprintf("Action %s failed: %v", actionType, err)
}

func executeCLIAction(ctx context.Context, sc *client.SmartClient, actionType string, body []byte) {
	stream, err := sc.DoAction(ctx, &flight.Action{Type: actionType, Body: body})
	if err != nil {
		log.Fatal(formatCLIAuthError(actionType, err))
	}
	for {
		res, err := stream.Recv()
		if err != nil {
			if st, ok := status.FromError(err); ok && (st.Code() == codes.Unauthenticated || st.Code() == codes.PermissionDenied) {
				log.Fatal(formatCLIAuthError(actionType, err))
			}
			break
		}
		if len(res.Body) > 0 {
			fmt.Println(string(res.Body))
		}
	}
}

func runOIDCListProviders(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("oidc-list-providers", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "Admin API key")
	_ = fs.Parse(args)
	sc := getCLIAuthClient(*uri, *apiKey, "", "", "", "")
	defer func() { _ = sc.Close() }()
	executeCLIAction(sc.WithAuthContext(ctx), sc, "ListOIDCProviders", nil)
}

func runOIDCAddProvider(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("oidc-add-provider", flag.ExitOnError)
	provider := fs.String("provider", "generic", "Provider kind: generic, aws, google")
	issuer := fs.String("issuer", "", "OIDC issuer URL")
	jwks := fs.String("jwks", "", "Optional explicit JWKS URL")
	audience := fs.String("audience", "longbow", "Expected JWT audience")
	tenantClaim := fs.String("tenant-claim", "tenant_id", "JWT claim mapped to tenant_id")
	roleClaim := fs.String("role-claim", "groups", "JWT claim mapped to roles")
	rowFilterClaim := fs.String("row-filter-claim", "row_filters", "JWT claim mapped to RLS row filters")
	adminClaim := fs.String("admin-claim", "role", "JWT claim checked for admin elevation")
	adminValue := fs.String("admin-value", "admin", "Claim value granting admin role")
	trustARN := fs.String("trust-arn", "", "Optional AWS role ARN or Google service account principal")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "Admin API key")
	_ = fs.Parse(args)
	if *issuer == "" && *provider != "google" {
		log.Fatal("Usage: longbow-cli oidc-add-provider -issuer <url> -audience <aud> [-provider aws|google|generic]")
	}
	sc := getCLIAuthClient(*uri, *apiKey, "", "", "", "")
	defer func() { _ = sc.Close() }()
	var oidcCfg security.OIDCIssuerConfig
	switch security.OIDCProviderKind(*provider) {
	case security.OIDCProviderAWS:
		oidcCfg = security.AWSOIDCIssuerConfig(*issuer, *audience)
	case security.OIDCProviderGoogle:
		oidcCfg = security.GoogleOIDCIssuerConfig(*audience)
		if *issuer != "" {
			oidcCfg.IssuerURL = *issuer
		}
	default:
		oidcCfg = security.OIDCIssuerConfig{
			Provider:    security.OIDCProviderKind(*provider),
			IssuerURL:   *issuer,
			Audience:    *audience,
			AllowedAlgs: []string{"RS256", "ES256"},
		}
	}
	oidcCfg.JWKSURL = *jwks
	oidcCfg.TenantClaim = *tenantClaim
	oidcCfg.RoleClaim = *roleClaim
	oidcCfg.RowFilterClaim = *rowFilterClaim
	oidcCfg.AdminClaim = *adminClaim
	oidcCfg.AdminValue = *adminValue
	oidcCfg.TrustPolicyARN = *trustARN
	body, _ := json.Marshal(oidcCfg)
	executeCLIAction(sc.WithAuthContext(ctx), sc, "AddOIDCProvider", body)
}

func runOIDCRemoveProvider(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("oidc-remove-provider", flag.ExitOnError)
	issuer := fs.String("issuer", "", "OIDC issuer URL to remove")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "Admin API key")
	_ = fs.Parse(args)
	if *issuer == "" {
		log.Fatal("Usage: longbow-cli oidc-remove-provider -issuer <url>")
	}
	sc := getCLIAuthClient(*uri, *apiKey, "", "", "", "")
	defer func() { _ = sc.Close() }()
	body, _ := json.Marshal(map[string]any{"issuer_url": *issuer})
	executeCLIAction(sc.WithAuthContext(ctx), sc, "RemoveOIDCProvider", body)
}

func runOIDCExchange(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("oidc-exchange", flag.ExitOnError)
	token := fs.String("token", os.Getenv("LONGBOW_OIDC_TOKEN"), "Federated OIDC JWT")
	ttl := fs.Int64("ttl", 900, "Requested session TTL in seconds (max 3600)")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Server URI")
	_ = fs.Parse(args)
	if *token == "" {
		log.Fatal("Usage: longbow-cli oidc-exchange -token <jwt> [-ttl 900]")
	}
	sc := getCLIAuthClient(*uri, *token, "", "", "", "")
	defer func() { _ = sc.Close() }()
	body, _ := json.Marshal(map[string]any{"ttl_seconds": *ttl})
	executeCLIAction(sc.WithAuthContext(ctx), sc, "ExchangeOIDCToken", body)
}
