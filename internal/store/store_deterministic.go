package store

import (
	"os"
	"sync/atomic"
)

// DeterministicIngest pins the parts of the ingest pipeline whose behaviour
// otherwise depends on wall-clock timing, so that two runs of the same client
// against the same corpus produce the same index and the same search output.
//
// It exists because the benchmark harness could not verify reproducibility any
// other way (roadmap Item 31). Four things made an index depend on when it was
// built rather than on what was built:
//
//  1. Level assignment drew from the process-global math/rand, which Go seeds
//     from the clock. Fixed separately and unconditionally in
//     index.LevelSeed, since a graph whose shape depends on the clock is not
//     something a deployed system should ship either.
//  2. EfConstruction was resized from the index queue depth at the moment each
//     batch was picked up - 100/200/400 depending on how far behind the queue
//     was, so the same corpus built with different construction parameters.
//  3. The index worker grouped "whatever was in the queue" (1-4 jobs) into one
//     AddBatch, and AddBatch assigns internal node IDs by position, so the
//     mapping from row to node - and which rows were linked together as one
//     batch - moved with scheduling.
//  4. Ingestion workers published record batches in pop order from a queue fed
//     by a streaming DoPut, so with more than one worker a row's global ID
//     depended on which worker got there first.
//
// The mode costs throughput: one ingestion worker, one index worker, one batch
// per AddBatch, no queue-depth adaptation. It is for measurement runs, not for
// throughput runs, and nothing about it changes the search path or the graph
// any given corpus converges to - only that it converges to the same one.
//
// Enabled by LONGBOW_BENCH_SEED being set, the same switch that hands the seed
// to the client: a benchmark that claims a fixed seed should get a fixed graph
// out of it.
//
// The flag is read from worker goroutines on every batch, so it lives in an
// atomic rather than in a plain package variable: a plain one is a data race
// for anything that flips it after workers start, which is exactly what a test
// does when it restores the default.
var deterministicIngest atomic.Bool

func init() {
	deterministicIngest.Store(os.Getenv("LONGBOW_BENCH_SEED") != "")
}

// DeterministicIngestEnabled reports whether the ingest pipeline is pinned.
func DeterministicIngestEnabled() bool { return deterministicIngest.Load() }

// SetDeterministicIngest turns the pinning on or off. Tests use it; a running
// server sets it through LONGBOW_BENCH_SEED before any worker starts.
func SetDeterministicIngest(on bool) { deterministicIngest.Store(on) }
