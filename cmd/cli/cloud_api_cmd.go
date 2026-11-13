package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/metadata"
)

func executeCloudOrGraphQLAction(ctx context.Context, uri, apiKey, actionType string, payload any) {
	sc := mustGetClient(uri)
	defer func() { _ = sc.Close() }()

	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+apiKey)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		log.Fatalf("marshal payload: %v", err)
	}
	stream, err := sc.DoAction(ctx, &flight.Action{Type: actionType, Body: raw})
	if err != nil {
		log.Fatalf("%s failed: %v", actionType, err)
	}
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("%s stream error: %v", actionType, err)
		}
		fmt.Println(string(res.Body))
	}
}

func runCloudCreateCluster(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("cloud-create-cluster", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	id := fs.String("id", "", "Cluster ID")
	name := fs.String("name", "", "Cluster name (required)")
	tenant := fs.String("tenant", "default", "Tenant ID")
	tier := fs.String("tier", "dedicated", "Cluster tier (serverless|dedicated|enterprise)")
	region := fs.String("primary-region", "us-east-1", "Primary region")
	nodes := fs.Int("nodes", 3, "Node count")
	mode := fs.String("replication-mode", "semi_sync", "Replication mode (async|semi_sync|sync)")
	_ = fs.Parse(args)
	if *name == "" {
		log.Fatal("-name is required")
	}
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "CreateCloudCluster", map[string]any{
		"id":               *id,
		"name":             *name,
		"tenant_id":        *tenant,
		"tier":             *tier,
		"primary_region":   *region,
		"nodes":            *nodes,
		"replication_mode": *mode,
	})
}

func runCloudListClusters(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("cloud-list-clusters", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	tenant := fs.String("tenant", "", "Filter by tenant ID")
	_ = fs.Parse(args)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "ListCloudClusters", map[string]any{"tenant_id": *tenant})
}

func runCloudScaleCluster(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("cloud-scale", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	id := fs.String("id", "", "Cluster ID (required)")
	nodes := fs.Int("nodes", 0, "Target node count")
	tier := fs.String("tier", "", "Target tier")
	_ = fs.Parse(args)
	if *id == "" {
		log.Fatal("-id is required")
	}
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "ScaleCloudCluster", map[string]any{
		"id":    *id,
		"nodes": *nodes,
		"tier":  *tier,
	})
}

func runCloudPromoteRegion(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("cloud-promote", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	id := fs.String("id", "", "Cluster ID (required)")
	region := fs.String("region", "", "Target region to promote (optional)")
	reason := fs.String("reason", "operator failover", "Failover reason")
	_ = fs.Parse(args)
	if *id == "" {
		log.Fatal("-id is required")
	}
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "PromoteClusterRegion", map[string]any{
		"cluster_id":    *id,
		"target_region": *region,
		"reason":        *reason,
	})
}

func runCloudSLAReport(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("cloud-sla", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	id := fs.String("id", "", "Cluster ID (required)")
	p99 := fs.Float64("p99-ms", 0, "Observed P99 latency in ms")
	_ = fs.Parse(args)
	if *id == "" {
		log.Fatal("-id is required")
	}
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "GetCloudSLAReport", map[string]any{
		"cluster_id":      *id,
		"observed_p99_ms": *p99,
	})
}

func runGraphQLCommand(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("graphql", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	query := fs.String("query", "{ health { status activeDatasets namespaces } }", "GraphQL query or mutation")
	varsJSON := fs.String("variables", "", "Optional JSON variables map")
	opName := fs.String("operation", "", "Optional GraphQL operation name")
	_ = fs.Parse(args)

	var vars map[string]any
	if *varsJSON != "" {
		if err := json.Unmarshal([]byte(*varsJSON), &vars); err != nil {
			log.Fatalf("invalid -variables JSON: %v", err)
		}
	}
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "GraphQLQuery", map[string]any{
		"query":         *query,
		"variables":     vars,
		"operationName": *opName,
	})
}
