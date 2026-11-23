package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/23skdu/longbow/client"
	"github.com/apache/arrow-go/v18/arrow/flight"
)

// BenchmarkResult represents a benchmark result value.
type BenchmarkResult struct {
	Name             string    `json:"name"`
	DurationSeconds  float64   `json:"duration_seconds"`
	Throughput       float64   `json:"throughput"`
	ThroughputUnit   string    `json:"throughput_unit"`
	ThroughputMBs    float64   `json:"throughput_mbs"`
	Rows             int64     `json:"rows"`
	BytesProcessed   int64     `json:"bytes_processed"`
	LatenciesMs      []float64 `json:"latencies_ms,omitempty"`
	MeanLatencyMs    float64   `json:"mean_latency_ms,omitempty"`
	P50LatencyMs     float64   `json:"p50_latency_ms,omitempty"`
	P95LatencyMs     float64   `json:"p95_latency_ms,omitempty"`
	P99LatencyMs     float64   `json:"p99_latency_ms,omitempty"`
	IndexingDuration float64   `json:"indexing_duration_seconds,omitempty"`
	TqBits           int       `json:"tq_bits,omitempty"`

	// R10: a mode whose context deadline expired was truncated, so its QPS is a
	// floor, not a measurement. Recorded explicitly so it cannot be read as a
	// low number and treated as a regression.
	ContextDeadlineExceeded bool  `json:"context_deadline_exceeded,omitempty"`
	QueriesRequested        int64 `json:"queries_requested,omitempty"`
	QueriesFailed           int64 `json:"queries_failed,omitempty"`
	QueriesTruncated        int64 `json:"queries_truncated,omitempty"`

	// R12: a baseline collected from a different mode list, or with a mode in a
	// different position, is not comparable - the modes before this one decide
	// its cache and GC state.
	Seed int64 `json:"seed,omitempty"`

	ModeIndex  int      `json:"mode_index,omitempty"`
	ModeCount  int      `json:"mode_count,omitempty"`
	ModeOrders []string `json:"mode_order,omitempty"`

	// Item 31: a digest over every hit this run returned, in query order, with
	// the score bits rather than the printed value. Two harness invocations
	// that claim the same seed must produce the same digest; that is the only
	// form in which "bit-identical search output" is checkable from the
	// outside. Latencies are deliberately not in it - they are the measurement,
	// not the output.
	SearchResultsDigest string `json:"search_results_digest,omitempty"`

	// Item 31: a digest of the vectors this run actually put into the server.
	// Without it a mismatching search digest is ambiguous - it could be the
	// corpus or it could be the index - and the corpus is the half that is
	// supposed to be a pure function of the seed.
	CorpusDigest string `json:"corpus_digest,omitempty"`
}

// searchHit is one row of a search response: the "id"/"score" pair the server
// writes into the DoGet record.
type searchHit struct {
	ID    uint64  `json:"id"`
	Score float32 `json:"score"`
}

// searchResultsOutput is what -results-out writes: enough to diff two runs by
// eye when their digests disagree.
type searchResultsOutput struct {
	Seed    int64                    `json:"seed"`
	Dataset string                   `json:"dataset"`
	Dim     int                      `json:"dim"`
	Dtype   string                   `json:"dtype"`
	Queries int                      `json:"queries"`
	Modes   []string                 `json:"modes"`
	Digest  string                   `json:"digest"`
	Results map[string][][]searchHit `json:"results"` // mode -> query index -> hits
}

// digestSearchResults hashes the search output of a whole run: modes in
// sorted order, then queries in index order, then hits in the order the server
// returned them. Scores go in as their IEEE-754 bits, so a difference in the
// last mantissa bit counts - which is what "bit-identical" means.
func digestSearchResults(byMode map[string][][]searchHit) string {
	modes := make([]string, 0, len(byMode))
	for m := range byMode {
		modes = append(modes, m)
	}
	sort.Strings(modes)

	h := sha256.New()
	var b strings.Builder
	for _, m := range modes {
		b.WriteString(m)
		b.WriteByte('\n')
		for q, hits := range byMode[m] {
			fmt.Fprintf(&b, "%d:", q)
			if hits == nil {
				b.WriteString("-\n")
				continue
			}
			for _, hit := range hits {
				fmt.Fprintf(&b, "%d/%x ", hit.ID, math.Float32bits(hit.Score))
			}
			b.WriteByte('\n')
		}
		_, _ = io.WriteString(h, b.String())
		b.Reset()
	}
	return hex.EncodeToString(h.Sum(nil))
}

// gpuKernelProfileResponse is what the server's gpu_kernel_profile action
// answers with.
type gpuKernelProfileResponse struct {
	ProfilingEnabled bool    `json:"profiling_enabled"`
	AttributedPct    float64 `json:"attributed_pct"`
	Profile          struct {
		Ops []struct {
			Name     string  `json:"name"`
			Calls    int64   `json:"calls"`
			TotalMS  float64 `json:"total_ms"`
			SharePct float64 `json:"share_pct"`
		} `json:"ops"`
		TotalMS       float64 `json:"total_ms"`
		UntimedCalls  int64   `json:"untimed_calls"`
		PoolExhausted int64   `json:"pool_exhausted"`
	} `json:"profile"`
}

// writeGPUKernelProfile asks the server for its per-kernel GPU time and writes
// it as JSON, printing the same numbers as a table so a run log carries the
// attribution without needing the file.
func writeGPUKernelProfile(path string, sc *client.SmartClient) error {
	ctx, cancel := context.WithTimeout(sc.WithAuthContext(context.Background()), 30*time.Second)
	defer cancel()

	stream, err := sc.DoAction(ctx, &flight.Action{Type: "gpu_kernel_profile"})
	if err != nil {
		return fmt.Errorf("gpu_kernel_profile action failed: %w", err)
	}
	result, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("gpu_kernel_profile stream: %w", err)
	}
	var resp gpuKernelProfileResponse
	if err := json.Unmarshal(result.Body, &resp); err != nil {
		return fmt.Errorf("gpu_kernel_profile payload: %w", err)
	}

	// #nosec G304 -- the path is the operator's own -gpu-profile-out argument,
	// chosen on this command line. Writing where the operator pointed is the
	// flag's whole purpose; there is no untrusted input between it and here.
	f, err := os.Create(path) // #nosec G304
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(resp); err != nil {
		return err
	}

	if !resp.ProfilingEnabled {
		log.Printf("[GPU-PROFILE] server has kernel profiling off (LONGBOW_GPU_KERNEL_PROF); nothing to attribute\n")
		return nil
	}
	log.Printf("[GPU-PROFILE] %.3f ms of GPU time over %d operations, %.1f%% attributed to named kernels, %d launches untimed -> %s\n",
		resp.Profile.TotalMS, len(resp.Profile.Ops), resp.AttributedPct, resp.Profile.UntimedCalls, path)
	for _, op := range resp.Profile.Ops {
		log.Printf("[GPU-PROFILE]   %-44s %10d calls %10.3f ms %6.2f%%\n", op.Name, op.Calls, op.TotalMS, op.SharePct)
	}
	return nil
}

func writeSummaryAndArtifacts(
	sc *client.SmartClient,
	results []BenchmarkResult,
	searchHitsByMode map[string][][]searchHit,
	modes []string,
	dataset, dtype string,
	dim, queries int,
	seed int64,
	gpuProfileOut, resultsOut, outputJSON string,
) {
	resultsDigest := digestSearchResults(searchHitsByMode)
	totalHits := 0
	for _, hits := range searchHitsByMode {
		for _, q := range hits {
			totalHits += len(q)
		}
	}
	for i := range results {
		if strings.HasPrefix(results[i].Name, "Search_") {
			results[i].SearchResultsDigest = resultsDigest
		}
	}
	log.Printf("[SEARCH] results digest %s over %d hits across %d modes (seed %d); two runs with the same seed must agree on it\n",
		resultsDigest, totalHits, len(searchHitsByMode), seed)

	// Item 34: pull the server's per-kernel GPU time breakdown.
	if gpuProfileOut != "" {
		if err := writeGPUKernelProfile(gpuProfileOut, sc); err != nil {
			log.Printf("[GPU-PROFILE] could not collect: %v\n", err)
		}
	}

	if resultsOut != "" {
		out := searchResultsOutput{
			Seed:    seed,
			Dataset: dataset,
			Dim:     dim,
			Dtype:   dtype,
			Queries: queries,
			Modes:   modes,
			Digest:  resultsDigest,
			Results: searchHitsByMode,
		}
		f, err := os.Create(resultsOut) // #nosec G304 -- CLI operator flag
		if err != nil {
			log.Fatalf("Failed to create results file: %v", err)
		}
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			log.Fatalf("Failed to write results file: %v", err)
		}
		_ = f.Close()
		log.Printf("Search results written to %s\n", resultsOut)
	}

	// Print Summary
	fmt.Printf("\n%s\n", "BENCHMARK SUITE SUMMARY")
	fmt.Printf("%-20s | %-18s | %-18s | %-10s | %-10s | %-10s | %-10s | %-10s\n", "Name", "Throughput (vec/s)", "Throughput (MB/s)", "Rows", "mean(ms)", "P50(ms)", "P95(ms)", "P99(ms)")
	for _, r := range results {
		fmt.Printf("%-20s | %-18.2f | %-18.2f | %-10d | %-10.3f | %-10.2f | %-10.2f | %-10.2f\n", r.Name, r.Throughput, r.ThroughputMBs, r.Rows, r.MeanLatencyMs, r.P50LatencyMs, r.P95LatencyMs, r.P99LatencyMs)
	}

	if outputJSON != "" {
		f, err := os.Create(outputJSON) // #nosec G304 -- CLI operator flag
		if err != nil {
			log.Fatalf("Failed to create JSON: %v", err)
		}
		defer f.Close()
		_ = json.NewEncoder(f).Encode(results)
		log.Printf("Results saved to %s\n", outputJSON)
	}
}

func logLoadHints(sc *client.SmartClient) {
	// Trigger a GetFlightInfo to refresh hints
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"_health"},
	}
	// We use a short timeout for this background refresh
	ctx, cancel := context.WithTimeout(sc.WithAuthContext(context.Background()), 2*time.Second)
	defer cancel()

	_, err := sc.GetFlightInfo(ctx, desc)
	if err != nil {
		// Log error but don't fail, hints might still be available from previous calls
		log.Printf("[LOAD-REFRESH-ERROR] %v\n", err)
	}

	hints := sc.GetLastLoadHints()
	if hints != nil {
		log.Printf("[LOAD] CPU: %d%%, Mem: %d%%, Queue: %d, Health: %d%%\n",
			hints.CPULoad, hints.MemLoad, hints.QueueDepth, hints.Health)
	}
}

func logDetailedError(cmd string, err error, sc *client.SmartClient) {
	log.Printf("[ERROR] %s failed: %v\n", cmd, err)
	hints := sc.GetLastLoadHints()
	if hints != nil {
		log.Printf("[LOAD-AT-FAILURE] CPU: %d%%, Mem: %d%%, Queue: %d, Health: %d%%\n",
			hints.CPULoad, hints.MemLoad, hints.QueueDepth, hints.Health)
	}
}
