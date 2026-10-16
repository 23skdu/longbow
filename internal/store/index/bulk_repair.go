package index

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
)

// BulkGraphQualityFloor is the layer-0 reachability a bulk-inserted batch must
// show, measured from the entry point, before the batch is accepted. It only
// applies when the gate is enabled; see bulkGraphQualityGuardEnabled.
//
// Set LONGBOW_HNSW_BULK_REACHABILITY_FLOOR to override.
var BulkGraphQualityFloor = func() float64 {
	if v := os.Getenv("LONGBOW_HNSW_BULK_REACHABILITY_FLOOR"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f <= 1 {
			return f
		}
	}
	return 0.80
}()

// bulkGraphQualityGuardEnabled reports whether the R8 graph-quality gate
// enforces its floors, or only reports them. Enforcing is the default, because
// the bulk path on its own is a regression: a 100k float32 corpus built
// entirely through bulk insertion indexed in 23.5s and served 700 dense QPS,
// against 85-215s and 3286-3474 QPS for the same corpus with the gate rejecting
// batches and rebuilding them sequentially.
//
// Set LONGBOW_HNSW_BULK_QUALITY_GUARD=0 to report the measurements without
// enforcing them.
//
// Which floor does the rejecting is still open, and all three are documented on
// their own:
//
//   - Sampled reachability is the only one that tracked search throughput in
//     practice, but it is only a valid measurement when no other bulk insert is
//     mutating the graph, so it is skipped under concurrent AddBatch.
//   - Mean degree correlates with throughput across whole builds but not within
//     one, and is disabled by default: enforcing it at 0.50 rejected four of the
//     nine batches in a 100k float32 build for a 215s index build that served
//     3286 dense QPS against 3474 ungated.
//   - Descent depth is the closest proxy to what search actually does, and it
//     stayed within budget (2.3-4.0 hops against a 5.0-6.4 budget) on both the
//     graphs that served 3474 QPS and the one that served 650.
var bulkGraphQualityGuardEnabled = func() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LONGBOW_HNSW_BULK_QUALITY_GUARD"))) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}()

// BulkGraphQualityMinDegreeRatio is the fraction of MMax0 that layer 0 must
// reach, on average, for a bulk-inserted batch to be accepted.
//
// Disabled by default, which is a measured decision. Enforcing it at 0.50 makes
// the gate reject four of the nine batches in a 100k float32 build, each one
// falling back to sequential insertion, and the resulting 215s index build
// misses TestArrowHNSW_ConcurrentAddBatch_Int8_50k_Stress's 90s budget. It also
// buys nothing: the runs it accepts served 3286 dense QPS against 3474 for the
// ungated build, inside the +-5% run-to-run spread. The sequential rebuilds it
// triggers are not cheaper.
//
// Degree is reported on every batch so the trade is visible, and the knob is
// here for deployments that would rather pay the index time than ship a sparse
// layer. Set LONGBOW_HNSW_BULK_MIN_DEGREE_RATIO to enable; 0 or unset disables.
var BulkGraphQualityMinDegreeRatio = func() float64 {
	if v := os.Getenv("LONGBOW_HNSW_BULK_MIN_DEGREE_RATIO"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f <= 1 {
			return f
		}
	}
	return 0
}()

// BulkGraphMaxHopDepth is the mean layer-0 descent depth a bulk-inserted batch
// may have before the gate rejects it, expressed as a multiple of the depth a
// navigable graph of this size should need: log(N)/log(MMax0), which is about 5
// hops at 100k with MMax0=16, so the default admits up to 7.5.
//
// It only applies when the gate is enabled. Set LONGBOW_HNSW_BULK_MAX_HOP_DEPTH
// to override; 0 disables the check.
var BulkGraphMaxHopDepth = func() float64 {
	if v := os.Getenv("LONGBOW_HNSW_BULK_MAX_HOP_DEPTH"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 {
			return f
		}
	}
	return 1.5
}()

// bulkHopFloor is the smallest hop budget the gate will apply, in mean layer-0
// descent hops.
//
// The budget is log(N)/log(MMax0) times BulkGraphMaxHopDepth, and that estimate
// degenerates on small graphs: at 2,000 nodes with MMax0=64 it is 1.8 hops, so
// the gate rejects anything past 2.7 - including a perfectly navigable graph
// measured at 3.0, which is what it did to TestArrowHNSW_ConcurrentAddBatch_-
// Int8_2Batches. The gate's remedy is a sequential rebuild of the whole batch,
// and sequential insertion with MMax0=64 runs the O(m^2) diversity heuristic per
// node, so a 0.3-hop overshoot on a 2,000-node batch turned a sub-second build
// into one that did not finish inside a five-minute deadline under -race. That
// is the same trade BulkGraphQualityMinDegreeRatio documents and the reason it
// is off by default.
//
// A floor does not weaken the gate where it matters: at 100k with MMax0=16 the
// estimate is 4.1 hops and the budget stays above the floor, and the graphs this
// gate was built to catch are caught by the reachability floor, not by hop
// depth. Measured on the pre-a955a0c1 TurboQuant graph - the defect the gate
// exists for - the hop check passed (2.6 against a 6.0 budget) and only
// reachability (8.2% against an 80% floor) fired.
const bulkHopFloor = 4.0

// bulkHopBudget is the mean descent depth a batch of this size is allowed.
func bulkHopBudget(idealHops float64) float64 {
	budget := idealHops * BulkGraphMaxHopDepth
	if budget < bulkHopFloor {
		return bulkHopFloor
	}
	return budget
}

// bulkGraphQuality is what measureBulkGraphQuality measured, so the caller can
// report it whether or not the floor was met.
type bulkGraphQuality struct {
	Nodes        int
	Reachable    int
	Sampled      int
	Reachability float64
	MeanDegree   float64
	MeanHops     float64
	IdealHops    float64
}

// measureHopDepth greedy-descends layer 0 from the entry point towards each of
// the sampled targets and reports the mean number of hops.
//
// This walks the same accessor the gate already used for the reachability BFS
// and computes distances through the index's own typed distance path, so it
// costs a handful of distance evaluations per sample and needs no new
// plumbing per element type. Descent stops as soon as no neighbour of the
// current node is closer to the target than the node itself, which is the
// standard HNSW greedy rule: a node the descent cannot improve on is where the
// path ends, whether that is the target or not.
func (h *ArrowHNSW) measureHopDepth(ctx context.Context, ep uint32, targets []uint32) (mean, ideal float64) {
	ideal = h.idealHopDepth()
	if len(targets) == 0 {
		return 0, ideal
	}

	searchCtx := h.searchPool.Get()
	defer h.searchPool.PutWithMetrics(searchCtx, h.config.DataType.String(), strconv.Itoa(int(h.dims.Load())))
	searchCtx.MaxNodeCount = h.nodeCount.Load()
	searchCtx.MaxGeneration = h.GetMetadataSnapshot().Generation
	searchCtx.AllowUncommitted = true

	total := 0.0
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			return total / float64(len(targets)), ideal
		}
		total += float64(h.greedyHops(searchCtx, ep, target))
	}
	return total / float64(len(targets)), ideal
}

// idealHopDepth is the depth a navigable layer 0 of this size needs to reach an
// arbitrary node: log(N)/log(MMax0), the logarithmic scaling HNSW is built on.
//
// It has to come from the configured degree, not the measured one. A degenerate
// layer 0 measures a small degree, which inflates the ideal and hands the worst
// graphs the loosest budget - exactly backwards.
func (h *ArrowHNSW) idealHopDepth() float64 {
	degree := float64(h.mMax0.Load())
	if degree < 2 {
		return math.Log(float64(h.nodeCount.Load()))
	}
	return math.Log(float64(h.nodeCount.Load())) / math.Log(degree)
}

// maxGreedyHops bounds a single descent so a disconnected target cannot spin.
const maxGreedyHops = 512

func (h *ArrowHNSW) greedyHops(ctx *ArrowSearchContext, ep, target uint32) int {
	cur := ep
	if cur == target {
		return 0
	}
	var dist [1]float32
	h.computeDistances(ctx, h.data.Load(), cur, []uint32{target}, dist[:])
	best := dist[0]

	hops := 0
	for hops < maxGreedyHops {
		hops++
		nb, err := h.GetLayerNeighbors(cur, 0)
		if err != nil || len(nb) == 0 {
			return hops
		}
		dists := make([]float32, len(nb))
		h.computeDistances(ctx, h.data.Load(), target, nb, dists)

		next := uint32(0)
		nextDist := float32(math.MaxFloat32)
		for i, v := range nb {
			if v == target {
				return hops
			}
			if dists[i] < nextDist {
				next, nextDist = v, dists[i]
			}
		}
		if nextDist >= best {
			// Nothing adjacent to cur is closer to the target than cur is, so
			// the descent has converged. That is the standard greedy stopping
			// rule and it is where the hop count ends whether or not cur is
			// the target.
			return hops
		}
		cur, best = next, nextDist
	}
	return hops
}

// checkBulkGraphQuality measures layer-0 reachability and mean degree for the
// batch just linked in, and falls back to sequential insertion when either
// indicates a graph the search path would not navigate well (R8).
func (h *ArrowHNSW) checkBulkGraphQuality(ctx context.Context, startID uint32, n int) error {
	q := h.measureBulkGraphQuality(ctx, startID, n)
	if q.Sampled == 0 {
		return nil
	}
	enforce := bulkGraphQualityGuardEnabled && h.inAddBatch.Load() <= 1
	minDegree := float64(h.mMax0.Load()) * BulkGraphQualityMinDegreeRatio
	maxHops := bulkHopBudget(q.IdealHops)
	degradedHops := enforce && BulkGraphMaxHopDepth > 0 && q.MeanHops > maxHops
	degradedDegree := enforce && minDegree > 0 && q.MeanDegree < minDegree
	degradedReach := enforce && q.Reachability < BulkGraphQualityFloor
	// Always report: this is the only place the numbers exist, and a threshold
	// nobody can see is a threshold nobody can tune.
	fmt.Printf("[HNSW] bulk batch nodes=%d reachable=%d/%d sampled=%.1f%% mean_degree=%.2f hops=%.1f/%.1f addbatch_inflight=%d fallback=%v\n",
		q.Nodes, q.Reachable, q.Nodes, 100*q.Reachability, q.MeanDegree, q.MeanHops, maxHops,
		h.inAddBatch.Load(), degradedHops || degradedDegree || degradedReach)
	switch {
	case degradedHops:
		return fmt.Errorf("bulk graph needs %.1f descent hops to reach its own batch (budget %.1f)", q.MeanHops, maxHops)
	case degradedDegree:
		return fmt.Errorf("bulk layer-0 mean degree %.2f < %.2f", q.MeanDegree, minDegree)
	case degradedReach:
		return fmt.Errorf("bulk graph reachability degraded (%.1f%% < %.0f%%)", q.Reachability*100, BulkGraphQualityFloor*100)
	}
	return nil
}

// measureBulkGraphQuality walks layer 0 from the entry point and reports what it
// found. The walk is bounded by the node count rather than by a visit budget,
// because the cheapest way to sample a graph whose quality is in question is to
// look at all of it; the sample of batch nodes is what gets turned into a
// verdict.
func (h *ArrowHNSW) measureBulkGraphQuality(ctx context.Context, startID uint32, n int) bulkGraphQuality {
	// The walk is bounded by how many nodes the graph has, not by where this
	// batch ends. Concurrent AddBatch calls reserve their ids before the bulk
	// link releases bulkMu, so by the time a batch is measured another one may
	// already have published thousands of nodes past startID+n; bounding by the
	// batch end then stops the BFS part-way through the graph and reports the
	// nodes it happened to reach first as if they were all of them. That is how
	// a healthy int8 build reported "reachable=5024/5000 sampled=30.0%" and
	// tripped the gate into a full sequential rebuild.
	graphNodes := int(h.nodeCount.Load())
	batchEnd := int(startID) + n
	if graphNodes < batchEnd {
		graphNodes = batchEnd
	}
	out := bulkGraphQuality{Nodes: graphNodes}
	ep := h.entryPoint.Load()
	if ep == math.MaxUint32 {
		return out
	}

	seen := make(map[uint32]struct{}, graphNodes)
	queue := make([]uint32, 0, 1024)
	seen[ep] = struct{}{}
	queue = append(queue, ep)

	degreeSum := 0
	for len(queue) > 0 && len(seen) < out.Nodes {
		if err := ctx.Err(); err != nil {
			return out
		}
		cur := queue[0]
		queue = queue[1:]
		nb, err := h.GetLayerNeighbors(cur, 0)
		if err != nil {
			continue
		}
		degreeSum += len(nb)
		for _, v := range nb {
			if _, ok := seen[v]; ok {
				continue
			}
			seen[v] = struct{}{}
			queue = append(queue, v)
		}
	}
	out.Reachable = len(seen)
	if out.Reachable > out.Nodes {
		// The walk stops on a bound check, so the last node processed can still
		// enqueue neighbours past it. Never report more reachable nodes than
		// the graph is said to have: a ratio over 100% is the kind of number
		// that stops anyone reading the line that follows it.
		out.Nodes = out.Reachable
	}
	if out.Reachable > 0 {
		out.MeanDegree = float64(degreeSum) / float64(out.Reachable)
	}

	sampleCount := 20
	if n < sampleCount {
		sampleCount = n
	}
	step := n / sampleCount
	if step == 0 {
		step = 1
	}
	for i := 0; i < sampleCount; i++ {
		targetID := startID + uint32(i*step) // #nosec G115
		if targetID >= startID+uint32(n) {   // #nosec G115
			break
		}
		if _, ok := seen[targetID]; ok {
			out.Sampled++
		}
	}
	if sampleCount > 0 {
		out.Reachability = float64(out.Sampled) / float64(sampleCount)
	}

	targets := make([]uint32, 0, sampleCount)
	for i := 0; i < sampleCount; i++ {
		targetID := startID + uint32(i*step) // #nosec G115
		if targetID >= startID+uint32(n) {   // #nosec G115
			break
		}
		targets = append(targets, targetID)
	}
	out.MeanHops, out.IdealHops = h.measureHopDepth(ctx, ep, targets)
	return out
}
