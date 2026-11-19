package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"github.com/apache/arrow-go/v18/arrow/flight"
)

// Flight action types for graph operations. These must match the action
// types dispatched by the server (internal/store/store_actions.go for the
// data plane, store_meta_actions.go for the meta plane) and the RBAC
// allowlist in internal/store/rbac_actions.go. The server rejects unknown
// types with codes.Unimplemented.
const (
	actionAddEdge           = "add-edge"
	actionTraverseGraph     = "traverse-graph"
	actionGetGraphStats     = "GetGraphStats"
	actionCalculatePageRank = "calculate-pagerank"
	actionDetectCommunities = "detect-communities"
)

func marshalAction(actionType string, body any) (*flight.Action, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal %s request: %w", actionType, err)
	}
	return &flight.Action{Type: actionType, Body: raw}, nil
}

func buildAddEdgeAction(dataset string, subject int, predicate string, object int, weight float64) (*flight.Action, error) {
	return marshalAction(actionAddEdge, map[string]any{
		"dataset":   dataset,
		"subject":   subject,
		"predicate": predicate,
		"object":    object,
		"weight":    weight,
	})
}

func buildTraverseAction(dataset string, start, maxHops int, incoming, weighted bool) (*flight.Action, error) {
	return marshalAction(actionTraverseGraph, map[string]any{
		"dataset":  dataset,
		"start":    start,
		"max_hops": maxHops,
		"incoming": incoming,
		"weighted": weighted,
	})
}

func buildGetGraphStatsAction(dataset string) (*flight.Action, error) {
	return marshalAction(actionGetGraphStats, map[string]string{"dataset": dataset})
}

func buildPageRankAction(dataset string, dampingFactor float64, maxIterations int) (*flight.Action, error) {
	return marshalAction(actionCalculatePageRank, map[string]any{
		"dataset":        dataset,
		"damping_factor": dampingFactor,
		"max_iterations": maxIterations,
	})
}

func buildDetectCommunitiesAction(dataset string, maxIterations int) (*flight.Action, error) {
	return marshalAction(actionDetectCommunities, map[string]any{
		"dataset":        dataset,
		"max_iterations": maxIterations,
	})
}

func runAddEdge(_ context.Context, args []string) {
	fs := flag.NewFlagSet("add-edge", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	sub := fs.Int("subject", 0, "Subject ID")
	pred := fs.String("predicate", "related", "Predicate")
	obj := fs.Int("object", 0, "Object ID")
	weight := fs.Float64("weight", 1.0, "Edge weight")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	sc := mustGetClient(*uri)
	defer sc.Close()

	action, err := buildAddEdgeAction(*dataset, *sub, *pred, *obj, *weight)
	if err != nil {
		log.Fatalf("Add edge failed: %v", err)
	}

	_, err = sc.DoAction(context.Background(), action)
	if err != nil {
		log.Fatalf("Add edge failed: %v", err)
	}
	fmt.Printf("Added edge: %d --[%s]--> %d\n", *sub, *pred, *obj)
}

func runTraverse(_ context.Context, args []string) {
	fs := flag.NewFlagSet("traverse", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	start := fs.Int("start", 0, "Start node ID")
	hops := fs.Int("hops", 2, "Max hops")
	incoming := fs.Bool("incoming", false, "Traverse incoming edges instead of outgoing")
	weighted := fs.Bool("weighted", false, "Prioritize traversal by edge weight")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	sc := mustGetClient(*uri)
	defer sc.Close()

	action, err := buildTraverseAction(*dataset, *start, *hops, *incoming, *weighted)
	if err != nil {
		log.Fatalf("Traverse failed: %v", err)
	}

	stream, err := sc.DoAction(context.Background(), action)
	if err != nil {
		log.Fatalf("Traverse failed: %v", err)
	}

	for {
		res, err := stream.Recv()
		if err != nil {
			break
		}
		fmt.Printf("%s\n", string(res.Body))
	}
}

func runGetGraphStats(_ context.Context, args []string) {
	fs := flag.NewFlagSet("get-graph-stats", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	sc := mustGetClient(*uri)
	defer sc.Close()

	action, err := buildGetGraphStatsAction(*dataset)
	if err != nil {
		log.Fatalf("Get graph stats failed: %v", err)
	}
	stream, err := sc.DoAction(context.Background(), action)
	if err != nil {
		log.Fatalf("Get graph stats failed: %v", err)
	}

	res, _ := stream.Recv()
	fmt.Printf("%s\n", string(res.Body))
}

func runPageRank(_ context.Context, args []string) {
	fs := flag.NewFlagSet("pagerank", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	damping := fs.Float64("damping", 0.85, "Damping factor")
	iter := fs.Int("iterations", 20, "Max iterations")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	sc := mustGetClient(*uri)
	defer sc.Close()

	action, err := buildPageRankAction(*dataset, *damping, *iter)
	if err != nil {
		log.Fatalf("PageRank failed: %v", err)
	}

	stream, err := sc.DoAction(context.Background(), action)
	if err != nil {
		log.Fatalf("PageRank failed: %v", err)
	}

	res, _ := stream.Recv()
	fmt.Printf("%s\n", string(res.Body))
}

func runDetectCommunities(_ context.Context, args []string) {
	fs := flag.NewFlagSet("detect-communities", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	iter := fs.Int("iterations", 10, "Max label-propagation iterations")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	sc := mustGetClient(*uri)
	defer sc.Close()

	action, err := buildDetectCommunitiesAction(*dataset, *iter)
	if err != nil {
		log.Fatalf("Community detection failed: %v", err)
	}

	stream, err := sc.DoAction(context.Background(), action)
	if err != nil {
		log.Fatalf("Community detection failed: %v", err)
	}

	res, _ := stream.Recv()
	fmt.Printf("%s\n", string(res.Body))
}
