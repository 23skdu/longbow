//go:build onnx
// +build onnx

package onnx_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/23skdu/longbow/internal/onnx"
	"github.com/23skdu/longbow/internal/onnx/metal"
)

// BenchmarkONNXEngine benchmarks the unified ONNX engine across backends (CPU, CUDA, Metal).
// Note: Requires a valid model file at LONGBOW_BENCHMARK_MODEL_PATH.
// If no model is provided, it skips.

func BenchmarkONNX_Score(b *testing.B) {
	modelPath := os.Getenv("LONGBOW_BENCHMARK_MODEL_PATH")
	if modelPath == "" {
		b.Skip("Skipping BenchmarkONNX_Score: LONGBOW_BENCHMARK_MODEL_PATH not set")
	}

	session, err := onnx.NewSession(modelPath)
	if err != nil {
		b.Fatalf("Failed to create ONNX session: %v", err)
	}
	defer session.Close()

	query := "What is the capital of France?"
	docs := []string{
		"Paris is the capital and most populous city of France.",
		"London is the capital of the United Kingdom.",
		"Berlin is the capital of Germany.",
		"Madrid is the capital of Spain.",
		"Rome is the capital of Italy.",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := session.Score(context.Background(), query, docs)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkONNX_Score_BatchSizes(b *testing.B) {
	modelPath := os.Getenv("LONGBOW_BENCHMARK_MODEL_PATH")
	if modelPath == "" {
		b.Skip("Skipping BenchmarkONNX_Score_BatchSizes: LONGBOW_BENCHMARK_MODEL_PATH not set")
	}

	session, err := onnx.NewSession(modelPath)
	if err != nil {
		b.Fatalf("Failed to create ONNX session: %v", err)
	}
	defer session.Close()

	query := "test query"
	batchSizes := []int{1, 10, 50, 100}

	for _, size := range batchSizes {
		docs := make([]string, size)
		for j := 0; j < size; j++ {
			docs[j] = fmt.Sprintf("This is document %d for benchmarking purposes and contains some text.", j)
		}

		b.Run(fmt.Sprintf("BatchSize-%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := session.Score(context.Background(), query, docs)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMetal_Direct(b *testing.B) {
	if !metal.IsAvailable() {
		b.Skip("Metal not available")
	}

	engine, err := metal.NewEngine()
	if err != nil {
		b.Skipf("Cannot create Metal engine: %v", err)
	}
	defer engine.Close()

	query := "test query"
	docs := []string{
		"doc 1", "doc 2", "doc 3", "doc 4", "doc 5",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := engine.Score(context.Background(), query, docs)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkONNX_Parallel(b *testing.B) {
	modelPath := os.Getenv("LONGBOW_BENCHMARK_MODEL_PATH")
	if modelPath == "" {
		b.Skip("Skipping: LONGBOW_BENCHMARK_MODEL_PATH not set")
	}

	session, err := onnx.NewSession(modelPath)
	if err != nil {
		b.Fatal(err)
	}
	defer session.Close()

	query := "test query"
	docs := []string{"document content for parallel testing of the inference engine"}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			_, err := session.Score(ctx, query, docs)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
