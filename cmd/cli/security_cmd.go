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
	role := fs.String("role", "reader", "Role: admin, tenant-admin, writer, reader")
	tenant := fs.String("tenant", "", "Tenant ID / namespace")
	datasets := fs.String("datasets", "", "Comma-separated allowed datasets")
	ttl := fs.Int64("ttl", 0, "Optional TTL in seconds")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "Admin API key")
	_ = fs.Parse(args)

	if *name == "" {
		log.Fatal("Usage: longbow-cli create-api-key -name <name> [-role <role>] [-tenant <ns>]")
	}
	sc := getCLIAuthClient(*uri, *apiKey, *tenant, "", "", "")
	defer func() { _ = sc.Close() }()

	var dsList []string
	if *datasets != "" {
		dsList = strings.Split(*datasets, ",")
	}
	body, _ := json.Marshal(map[string]any{
		"name":        *name,
		"role":        *role,
		"tenant_id":   *tenant,
		"namespace":   *tenant,
		"datasets":    dsList,
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

func executeCLIAction(ctx context.Context, sc *client.SmartClient, actionType string, body []byte) {
	stream, err := sc.DoAction(ctx, &flight.Action{Type: actionType, Body: body})
	if err != nil {
		log.Fatalf("Action %s failed: %v", actionType, err)
	}
	for {
		res, err := stream.Recv()
		if err != nil {
			break
		}
		if len(res.Body) > 0 {
			fmt.Println(string(res.Body))
		}
	}
}
