package index

// Tests for SIMD kernel fallback attribution.
//
// resolveDistanceKernel validates each resolved SIMD kernel against the scalar
// reference once at construction and silently prefers the scalar path when they
// disagree. Falling back is correct - a kernel that disagrees would feed wrong
// distances into neighbour selection and corrupt the graph quietly - but doing it
// invisibly is the problem. An invisible fallback has a characteristic signature:
// one dtype sitting far below its siblings at identical element count, which is
// otherwise indistinguishable from that dtype simply being slower.
//
// These tests pin that the two rejection reasons stay distinguishable and that the
// mismatched one is reachable and attributed.

import (
	"math"
	"os"
	"strings"
	"testing"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// wrongKernel disagrees with the scalar reference, the way a real defect would: it
// returns plausible-looking numbers that are simply not the right distance.
func wrongKernel(a, b []float32) (float32, error) { return 1, nil }

// TestKernelMismatchIsDetected is the load-bearing test: if the mismatch check
// stops working, every wrong kernel is used and every graph is quietly wrong.
func TestKernelMismatchIsDetected(t *testing.T) {
	if kernelMatchesReference(wrongKernel, simd.MetricEuclidean, distanceFallbacks[float32]{
		euclidean: simd.EuclideanDistance,
	}, 128) {
		t.Error("a kernel returning a constant was accepted as matching the reference")
	}
}

// TestMatchingKernelIsAccepted is the other half: a correct kernel must not be
// rejected, or every dtype would silently run scalar and the whole point is lost.
func TestMatchingKernelIsAccepted(t *testing.T) {
	if !kernelMatchesReference(simd.EuclideanDistance, simd.MetricEuclidean, distanceFallbacks[float32]{
		euclidean: simd.EuclideanDistance,
	}, 128) {
		t.Error("the scalar reference was rejected as not matching itself")
	}
}

// TestResolutionOutcomesAreCounted covers the reporting split. `unavailable` is
// routine; `mismatch` is a kernel defect. Collapsing them would make the metric
// useless for telling a slow dtype from a broken one.
func TestResolutionOutcomesAreCounted(t *testing.T) {
	fb := distanceFallbacks[float32]{euclidean: simd.EuclideanDistance}

	// No float32 kernel is registered for MetricL2Squared at any dimension, so
	// this must resolve to the scalar path and be counted as `unavailable`.
	//
	// Metric and dims together, not dims alone: the registry falls back from an
	// exact dimension to the generic dims=0 entry, so an unusual width is still
	// resolvable whenever the type has a generic kernel. Before R33 that made
	// dims=7 a usable stand-in for "unregistered", because float32 resolved
	// nothing at all; now it resolves, and only an unregistered metric is a
	// reliable way to reach the `unavailable` branch.
	if k := simd.GetKernel[float32](simd.MetricL2Squared, 7); k != nil {
		t.Fatal("expected no float32 L2Squared kernel; the unavailable branch cannot be exercised")
	}
	got := resolveDistanceKernel(simd.MetricL2Squared, 7, fb, "resolvertest_unavailable")
	if got == nil {
		t.Fatal("resolver returned nil for a type with a scalar fallback")
	}

	if n := testutil.ToFloat64(metrics.HNSWSIMDKernelFallbacksTotal.
		WithLabelValues("resolvertest_unavailable", "l2_squared", "unavailable")); n < 1 {
		t.Errorf("no `unavailable` fallback counted for an unregistered metric: %v", n)
	}
	if n := testutil.ToFloat64(metrics.HNSWSIMDKernelFallbacksTotal.
		WithLabelValues("resolvertest_unavailable", "l2_squared", "mismatch")); n != 0 {
		t.Errorf("a missing kernel was misreported as a mismatch: %v", n)
	}

	// An element type that *is* registered resolves to SIMD and is counted so.
	// int8 and float32 both are.
	resolveDistanceKernel(simd.MetricEuclidean, 128,
		distanceFallbacks[int8]{euclidean: simd.EuclideanDistanceInt8}, "resolvertest_int8")
	if n := testutil.ToFloat64(metrics.HNSWSIMDKernelResolvedTotal.
		WithLabelValues("resolvertest_int8", "euclidean", "simd")); n < 1 {
		t.Errorf("a registered element type was not counted as SIMD: %v", n)
	}
	resolveDistanceKernel(simd.MetricEuclidean, 128,
		distanceFallbacks[float32]{euclidean: simd.EuclideanDistance}, "resolvertest_float32")
	if n := testutil.ToFloat64(metrics.HNSWSIMDKernelResolvedTotal.
		WithLabelValues("resolvertest_float32", "euclidean", "simd")); n < 1 {
		t.Errorf("float32 did not resolve to SIMD and is not counted as such: %v", n)
	}
}

// TestFloat32KernelsResolveAndPassValidation records what used to be the single
// largest gap in this file.
//
// Until R33, GetKernel[float32] returned nil for every metric and dimension even
// though 21 float32 kernels were registered, so resolveDistanceKernel took the
// `unavailable` fallback for float32 on *every* index. That was not "float32 has
// no kernel"; it was "float32's kernels cannot be reached". The five named kernel
// types in internal/simd/simd_types.go were defined types rather than aliases, and
// a value of a defined type does not satisfy a type assertion to its underlying
// signature, so every kernel registered under one of them was dead. The same held
// for float16, complex64 and complex128, in part.
//
// Two things follow, and both matter:
//
//   - The validation gate now covers float32, which is the dtype the product uses
//     most. Previously the gate protected twelve element types and not that one.
//   - float32 search now reaches the registered dimension-specific kernels instead
//     of the auto-dispatching fallback, which is measurably faster - about 1.8x at
//     dims=128 and 1.25-1.55x at 384-1024 on the kernel microbenchmark. That is a
//     kernel-level figure, not end-to-end QPS; the distance kernel is only part of
//     a search, so the search-level gain is smaller.
//
// float32 correctness no longer rests solely on simd.EuclideanDistance dispatching
// correctly - a float32 kernel that disagrees with its scalar reference is now
// rejected here, with reason="mismatch", exactly like every other type.
func TestFloat32KernelsResolveAndPassValidation(t *testing.T) {
	ref := simd.EuclideanDistance
	for _, dims := range []int{128, 256, 384, 512, 768, 1024} {
		for _, m := range []simd.MetricType{simd.MetricEuclidean, simd.MetricCosine, simd.MetricDotProduct} {
			k := simd.GetKernel[float32](m, dims)
			if k == nil {
				t.Errorf("dims=%d %s: no float32 kernel resolves; float32 would silently "+
					"fall back to scalar for every query", dims, m)
			}
		}
		if !kernelMatchesReference(simd.GetKernel[float32](simd.MetricEuclidean, dims),
			simd.MetricEuclidean, distanceFallbacks[float32]{euclidean: ref}, dims) {
			t.Errorf("dims=%d: the float32 euclidean kernel disagrees with its scalar "+
				"reference and would be rejected as a mismatch", dims)
		}
	}
	// The integer types and the other float/complex types are registered too, so
	// the gate applies to them, and it passes - which disproves the hypothesis in
	// roadmap section 8.7 that a silent mismatch explains the int16/uint16 deficit.
	for name, registered := range map[string]bool{
		"int8":   simd.GetKernel[int8](simd.MetricEuclidean, 128) != nil,
		"int16":  simd.GetKernel[int16](simd.MetricEuclidean, 128) != nil,
		"uint8":  simd.GetKernel[uint8](simd.MetricEuclidean, 128) != nil,
		"uint16": simd.GetKernel[uint16](simd.MetricEuclidean, 128) != nil,
		"int64":  simd.GetKernel[int64](simd.MetricEuclidean, 128) != nil,
		"uint64": simd.GetKernel[uint64](simd.MetricEuclidean, 128) != nil,
	} {
		if !registered {
			t.Errorf("%s has no registered euclidean kernel at dims=128", name)
		}
	}
}

// TestIntegerKernelsPassValidation is the direct test of the roadmap hypothesis
// that a silent mismatch explains the integer-type spread. It does not: the
// registered integer kernels agree with their references, so nothing is being
// rejected, and the `mismatch` counter stays at zero for them.
func TestIntegerKernelsPassValidation(t *testing.T) {
	const dims = 128
	probe := func(name string, k simd.DistanceKernel[int16], ref simd.DistanceKernel[int16]) {
		if k == nil {
			t.Fatalf("%s: no kernel registered", name)
		}
		if !kernelMatchesReference(k, simd.MetricEuclidean,
			distanceFallbacks[int16]{euclidean: ref}, dims) {
			t.Errorf("%s kernel disagrees with its reference and would be rejected", name)
		}
	}
	probe("int16", simd.GetKernel[int16](simd.MetricEuclidean, dims), simd.EuclideanDistanceInt16)
	if !kernelMatchesReference(simd.GetKernel[int8](simd.MetricEuclidean, dims),
		simd.MetricEuclidean, distanceFallbacks[int8]{euclidean: simd.EuclideanDistanceInt8}, dims) {
		t.Error("int8 kernel disagrees with its reference and would be rejected")
	}

	// And the values agree numerically, not merely within the validation tolerance.
	a := make([]int16, dims)
	b := make([]int16, dims)
	for i := range a {
		a[i] = int16(i + 1)
		b[i] = int16(i + 3)
	}
	got, err := simd.GetKernel[int16](simd.MetricEuclidean, dims)(a, b)
	if err != nil {
		t.Fatal(err)
	}
	want, err := simd.EuclideanDistanceInt16(a, b)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("int16 kernel %.6f != scalar reference %.6f", got, want)
	}
}

// TestEveryElementTypeIsLabelled guards the attribution itself. All 13 element
// types are resolved through the same generic function, so an unlabelled call site
// would report a fallback with no way to tell which dtype caused it - exactly the
// situation this change exists to fix.
func TestEveryElementTypeIsLabelled(t *testing.T) {
	src := readSelf(t)

	want := []string{
		"float32", "float16", "float64", "complex64", "complex128",
		"int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64",
	}
	for _, label := range want {
		if !strings.Contains(src, `}, "`+label+`")`) {
			t.Errorf("no resolveDistanceKernel call is labelled %q", label)
		}
	}

	// And no call may omit the label: every call ends with a string argument.
	idx := 0
	for {
		i := strings.Index(src[idx:], "resolveDistanceKernel(sm, dims, distanceFallbacks[")
		if i < 0 {
			break
		}
		start := idx + i
		// Walk to the matching close of the composite literal.
		depth, j := 0, start
		for j < len(src) {
			if src[j] == '{' {
				depth++
			} else if src[j] == '}' {
				depth--
				if depth == 0 {
					break
				}
			}
			j++
		}
		closing := strings.TrimSpace(src[j+1 : j+40])
		if !strings.HasPrefix(closing, `, "`) {
			t.Errorf("resolveDistanceKernel call is not labelled: %q", closing)
		}
		idx = j
	}
}

// readSelf reads this package's distance_resolvers.go so the test does not depend
// on an absolute path.
func readSelf(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("distance_resolvers.go")
	if err != nil {
		t.Fatalf("read distance_resolvers.go: %v", err)
	}
	return string(data)
}

// TestZeroDimsIsTrusted documents the deliberate skip: with no dimensions there is
// nothing to probe, so the kernel is trusted rather than rejected on no evidence.
func TestZeroDimsIsTrusted(t *testing.T) {
	if !kernelMatchesReference(wrongKernel, simd.MetricEuclidean,
		distanceFallbacks[float32]{euclidean: simd.EuclideanDistance}, 0) {
		t.Error("a zero-dimension kernel was rejected; there is nothing to probe")
	}
}

// TestNaNAndInfAreRejected: a kernel returning NaN would poison neighbour
// ordering, and it cannot be caught by an equality test alone.
func TestNaNAndInfAreRejected(t *testing.T) {
	for name, k := range map[string]simd.DistanceKernel[float32]{
		"nan": func(a, b []float32) (float32, error) { return float32(math.NaN()), nil },
		"inf": func(a, b []float32) (float32, error) { return float32(math.Inf(1)), nil },
	} {
		if kernelMatchesReference(k, simd.MetricEuclidean, distanceFallbacks[float32]{
			euclidean: simd.EuclideanDistance,
		}, 128) {
			t.Errorf("kernel returning %s was accepted", name)
		}
	}
}

// TestNilReferenceIsTrusted: with no scalar reference to compare against, rejecting
// the kernel would be a guess.
func TestNilReferenceIsTrusted(t *testing.T) {
	if !kernelMatchesReference(simd.EuclideanDistance, simd.MetricEuclidean,
		distanceFallbacks[float32]{}, 128) {
		t.Error("kernel rejected despite there being no reference to disagree with")
	}
}
