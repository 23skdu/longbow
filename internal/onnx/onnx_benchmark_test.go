package onnx_test

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/onnx/metal"
)

func BenchmarkMetalRerankerScore(b *testing.B) {
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
		"document one with some content",
		"document two with different content",
		"document three with unique text",
		"document four with sample data",
		"document five with various words",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := engine.Score(context.Background(), query, docs)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMetalRerankerScoreParallel(b *testing.B) {
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
		"document one with some content",
		"document two with different content",
		"document three with unique text",
		"document four with sample data",
		"document five with various words",
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			_, err := engine.Score(ctx, query, docs)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMetalRerankerScoreLargeBatch(b *testing.B) {
	if !metal.IsAvailable() {
		b.Skip("Metal not available")
	}

	engine, err := metal.NewEngine()
	if err != nil {
		b.Skipf("Cannot create Metal engine: %v", err)
	}
	defer engine.Close()

	query := "test query"
	docs := make([]string, 100)
	for i := range docs {
		docs[i] = "document number " + string(rune('0'+i%10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := engine.Score(context.Background(), query, docs)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMetalRerankerScoreVaryingQueryLength(b *testing.B) {
	if !metal.IsAvailable() {
		b.Skip("Metal not available")
	}

	engine, err := metal.NewEngine()
	if err != nil {
		b.Skipf("Cannot create Metal engine: %v", err)
	}
	defer engine.Close()

	docs := []string{
		"document one with some content here and there",
		"document two with different content entirely",
		"document three with unique text for testing",
		"document four with sample data for comparison",
		"document five with various words for evaluation",
	}

	queries := []string{
		"test",
		"test query",
		"this is a longer test query for benchmarking purposes",
		"short",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := queries[i%len(queries)]
		_, err := engine.Score(context.Background(), query, docs)
		if err != nil {
			b.Fatal(err)
		}
	}
}
