package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"github.com/apache/arrow-go/v18/arrow/flight"
)

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

	req := map[string]interface{}{
		"dataset":   *dataset,
		"subject":   *sub,
		"predicate": *pred,
		"object":    *obj,
		"weight":    *weight,
	}
	actionBody, _ := json.Marshal(req)
	action := &flight.Action{Type: "AddEdge", Body: actionBody}

	_, err := sc.DoAction(context.Background(), action)
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
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	sc := mustGetClient(*uri)
	defer sc.Close()

	req := map[string]interface{}{
		"dataset":  *dataset,
		"start":    *start,
		"max_hops": *hops,
	}
	actionBody, _ := json.Marshal(req)
	action := &flight.Action{Type: "TraverseGraph", Body: actionBody}

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

	req := map[string]string{"dataset": *dataset}
	actionBody, _ := json.Marshal(req)
	action := &flight.Action{Type: "GetGraphStats", Body: actionBody}
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
	iter := fs.Int("iterations", 20, "Max iterations")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	sc := mustGetClient(*uri)
	defer sc.Close()

	req := map[string]interface{}{"dataset": *dataset, "max_iterations": *iter}
	actionBody, _ := json.Marshal(req)
	action := &flight.Action{Type: "CalculatePageRank", Body: actionBody}

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
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	sc := mustGetClient(*uri)
	defer sc.Close()

	req := map[string]string{"dataset": *dataset}
	actionBody, _ := json.Marshal(req)
	action := &flight.Action{Type: "DetectCommunities", Body: actionBody}

	stream, err := sc.DoAction(context.Background(), action)
	if err != nil {
		log.Fatalf("Community detection failed: %v", err)
	}

	res, _ := stream.Recv()
	fmt.Printf("%s\n", string(res.Body))
}
