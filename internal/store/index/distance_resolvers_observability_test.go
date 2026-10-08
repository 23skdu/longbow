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

	// Dims 7 has no registered kernel, so this must resolve to the scalar path
	// and be counted as `unavailable`.
	got := resolveDistanceKernel(simd.MetricEuclidean, 7, fb, "resolvertest_unavailable")
	if got == nil {
		t.Fatal("resolver returned nil for a type with a scalar fallback")
	}

	if n := testutil.ToFloat64(metrics.HNSWSIMDKernelFallbacksTotal.
		WithLabelValues("resolvertest_unavailable", "euclidean", "unavailable")); n < 1 {
		t.Errorf("no `unavailable` fallback counted for an unregistered dimension: %v", n)
	}
	if n := testutil.ToFloat64(metrics.HNSWSIMDKernelFallbacksTotal.
		WithLabelValues("resolvertest_unavailable", "euclidean", "mismatch")); n != 0 {
		t.Errorf("a missing kernel was misreported as a mismatch: %v", n)
	}

	// An element type that *is* registered resolves to SIMD and is counted so.
	// float32 is not, which is the finding below; int8 is.
	resolveDistanceKernel(simd.MetricEuclidean, 128,
		distanceFallbacks[int8]{euclidean: simd.EuclideanDistanceInt8}, "resolvertest_int8")
	if n := testutil.ToFloat64(metrics.HNSWSIMDKernelResolvedTotal.
		WithLabelValues("resolvertest_int8", "euclidean", "simd")); n < 1 {
		t.Errorf("a registered element type was not counted as SIMD: %v", n)
	}
}

// TestFloat32HasNoRegisteredKernel is the finding that shapes how these metrics
// should be read.
//
// No float32 kernel is registered in simd.GetKernel on an AVX2 host, for any
// dimension or metric. So resolveDistanceKernel takes the `unavailable` fallback for
// float32 on *every* index - and that fallback, simd.EuclideanDistance, is itself
// an auto-dispatching AVX2 kernel, which is why float32 is the fastest dtype in the
// matrix rather than the slowest.
//
// Two consequences, both of which this test exists to prevent someone "fixing":
//
//   - `longbow_hnsw_simd_kernel_fallbacks_total{element_type="float32"}` is
//     permanently non-zero and entirely benign. An alert that fires on "any
//     fallback" would fire forever; only `reason="mismatch"` indicates a defect.
//   - The validation gate never runs for float32, because there is nothing to
//     validate. float32 correctness rests on simd.EuclideanDistance dispatching
//     correctly, not on this check.
//
// Integer types and float64 are registered, so for those the gate is live - and it
// passes, which disproves the hypothesis in roadmap section 8.7 that a silent
// mismatch explains the int16/uint16 deficit.
func TestFloat32HasNoRegisteredKernel(t *testing.T) {
	for _, dims := range []int{128, 256, 384, 512, 768, 1024} {
		for _, m := range []simd.MetricType{simd.MetricEuclidean, simd.MetricCosine, simd.MetricDotProduct} {
			if simd.GetKernel[float32](m, dims) != nil {
				t.Errorf("dims=%d %s: a float32 kernel is now registered; "+
					"the findings in this file about float32 taking the fallback need revisiting",
					dims, m)
			}
		}
	}
	// The integer types are registered, so the gate applies to them.
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
