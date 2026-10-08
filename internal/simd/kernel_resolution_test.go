package simd

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// registeredKernelTypes is the set of element types that appear in the registry
// and for which GetKernel can be asked. Adding a new dtype means adding a row
// here; a type omitted from this list simply is not checked, which is the same
// gap this file exists to close.
var registeredKernelTypes = []struct {
	name  string
	dt    DataType
	check func(m MetricType, d int) bool
}{
	{"float32", DataTypeFloat32, func(m MetricType, d int) bool { return GetKernel[float32](m, d) != nil }},
	{"float64", DataTypeFloat64, func(m MetricType, d int) bool { return GetKernel[float64](m, d) != nil }},
	{"float16", DataTypeFloat16, func(m MetricType, d int) bool { return GetKernel[float16.Num](m, d) != nil }},
	{"complex64", DataTypeComplex64, func(m MetricType, d int) bool { return GetKernel[complex64](m, d) != nil }},
	{"complex128", DataTypeComplex128, func(m MetricType, d int) bool { return GetKernel[complex128](m, d) != nil }},
	{"int8", DataTypeInt8, func(m MetricType, d int) bool { return GetKernel[int8](m, d) != nil }},
	{"uint8", DataTypeUint8, func(m MetricType, d int) bool { return GetKernel[uint8](m, d) != nil }},
	{"int16", DataTypeInt16, func(m MetricType, d int) bool { return GetKernel[int16](m, d) != nil }},
	{"uint16", DataTypeUint16, func(m MetricType, d int) bool { return GetKernel[uint16](m, d) != nil }},
	{"int32", DataTypeInt32, func(m MetricType, d int) bool { return GetKernel[int32](m, d) != nil }},
	{"uint32", DataTypeUint32, func(m MetricType, d int) bool { return GetKernel[uint32](m, d) != nil }},
	{"int64", DataTypeInt64, func(m MetricType, d int) bool { return GetKernel[int64](m, d) != nil }},
	{"uint64", DataTypeUint64, func(m MetricType, d int) bool { return GetKernel[uint64](m, d) != nil }},
}

// TestEveryRegisteredKernelIsResolvable is the invariant that was silently
// violated for float32: the kernel was in the registry, and GetKernel could not
// return it. A registered-but-unresolvable kernel is worse than an unregistered
// one, because the registration reads as coverage while every search quietly
// takes the scalar path.
func TestEveryRegisteredKernelIsResolvable(t *testing.T) {
	metrics := []MetricType{MetricEuclidean, MetricCosine, MetricDotProduct, MetricL2Squared}
	dims := []int{0, 128, 384, 768, 1024, 1536, 3072}

	byType := map[string]int{}
	checked := 0

	for _, m := range metrics {
		for _, dt := range []DataType{
			DataTypeFloat32, DataTypeFloat64, DataTypeFloat16,
			DataTypeComplex64, DataTypeComplex128, DataTypeInt8, DataTypeUint8,
			DataTypeInt16, DataTypeUint16, DataTypeInt32, DataTypeUint32,
			DataTypeInt64, DataTypeUint64,
		} {
			var check func(MetricType, int) bool
			for _, e := range registeredKernelTypes {
				if e.dt == dt {
					check = e.check
					break
				}
			}
			if check == nil {
				continue
			}
			for _, d := range dims {
				if Registry.Get(m, dt, d) == nil {
					continue // not registered for this key; not this test's business
				}
				checked++
				if !check(m, d) {
					byType[dt.String()]++
				}
			}
		}
	}

	// Nothing may be registered and unreachable.
	for name, n := range byType {
		assert.Zero(t, n, "%s: %d kernel(s) are registered but GetKernel cannot resolve them", name, n)
	}
	assert.Greater(t, checked, 100,
		"expected to walk a substantial registry; the probe may be selecting nothing")
}

// TestFloat32KernelsResolve is a narrower, more readable statement of the same
// invariant for the dtype the product uses most. If this fails, float32 search
// is running scalar for every query and the SIMD counters will say so.
func TestFloat32KernelsResolve(t *testing.T) {
	for _, m := range []MetricType{MetricEuclidean, MetricCosine, MetricDotProduct} {
		for _, d := range []int{0, 128, 384, 768, 1024, 1536, 3072} {
			if Registry.Get(m, DataTypeFloat32, d) == nil {
				continue
			}
			require.NotNil(t, GetKernel[float32](m, d),
				"float32 %s dim=%d is registered but does not resolve", m, d)
		}
	}
}

// TestNamedKernelTypesAreAliases pins the shape of the fix. The five kernel
// types are aliases precisely so that a type assertion to their underlying
// signature succeeds; turning any of them back into a defined type reintroduces
// the bug and this fails before a single search runs.
func TestNamedKernelTypesAreAliases(t *testing.T) {
	var a distanceFunc
	var b func(a, b []float32) (float32, error)
	assertSameType(t, a, b)

	var c distanceF16Func
	var d func(a, b []float16.Num) (float32, error)
	assertSameType(t, c, d)

	var e distanceFloat64Func
	var f func(a, b []float64) (float32, error)
	assertSameType(t, e, f)

	var g distanceComplex64Func
	var h func(a, b []complex64) (float32, error)
	assertSameType(t, g, h)

	var i distanceComplex128Func
	var j func(a, b []complex128) (float32, error)
	assertSameType(t, i, j)
}

// assertSameType reports whether a and b have the same dynamic type. For an
// alias, converting either to interface yields one type; for a defined type and
// its underlying type, the interface holds two different types.
func assertSameType(t *testing.T, a, b any) {
	t.Helper()
	ta, tb := fmt.Sprintf("%T", a), fmt.Sprintf("%T", b)
	assert.Equal(t, ta, tb,
		"%s and %s must be the same type; a defined type here breaks GetKernel's type assertion", ta, tb)
}
