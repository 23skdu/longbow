//go:build amd64 && granite

package simd

import (
	"math"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

// Assembly declarations
//
//go:noescape
func enableAMX()

//go:noescape
func releaseTiles()

//go:noescape
func dotF16AMXKernel(a, b uintptr, n int) float32

func euclideanF16AMX(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, ErrDimensionMismatch
	}
	if len(a) == 0 {
		return 0, nil
	}
	if !features.HasAMX {
		return euclideanF16Unrolled4x(a, b)
	}

	// L2 = dot(a,a) + dot(b,b) - 2*dot(a,b)
	aPtr := uintptr(unsafe.Pointer(&a[0])) // #nosec G103
	bPtr := uintptr(unsafe.Pointer(&b[0])) // #nosec G103
	dotAA := dotF16AMXKernel(aPtr, aPtr, len(a))
	dotBB := dotF16AMXKernel(bPtr, bPtr, len(a))
	dotAB := dotF16AMXKernel(aPtr, bPtr, len(a))
	sum := dotAA + dotBB - 2*dotAB
	if sum < 0 {
		sum = 0
	}
	return float32(math.Sqrt(float64(sum))), nil
}

func dotF16AMX(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, ErrDimensionMismatch
	}
	if len(a) == 0 {
		return 0, nil
	}
	if !features.HasAMX {
		return dotF16Unrolled4x(a, b)
	}
	return dotF16AMXKernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a),
	), nil
}

func matMulF16AMX(a, b []float16.Num, m, n, k int, dst []float16.Num) {
	fa := make([]float32, len(a))
	fb := make([]float32, len(b))
	fdst := make([]float32, len(dst))
	float16ToFloat32Generic(a, fa)
	float16ToFloat32Generic(b, fb)
	matMulGeneric(fa, fb, m, n, k, fdst)
	for i := range dst {
		dst[i] = float16.New(fdst[i])
	}
}

func euclideanF16BatchAMX(query []float16.Num, vectors [][]float16.Num, results []float32) error {
	if len(query) == 0 || len(vectors) == 0 {
		return nil
	}
	if !features.HasAMX {
		return euclideanF16BatchGeneric(query, vectors, results)
	}

	n := len(vectors)
	qLen := len(query)
	qPtr := uintptr(unsafe.Pointer(&query[0])) // #nosec G103
	dotQQ := dotF16AMXKernel(qPtr, qPtr, qLen)

	for i := 0; i < n; i++ {
		if len(vectors[i]) != qLen {
			return ErrDimensionMismatch
		}
		vPtr := uintptr(unsafe.Pointer(&vectors[i][0])) // #nosec G103
		dotVV := dotF16AMXKernel(vPtr, vPtr, qLen)
		dotQV := dotF16AMXKernel(qPtr, vPtr, qLen)
		sum := dotQQ + dotVV - 2*dotQV
		if sum < 0 {
			sum = 0
		}
		results[i] = float32(math.Sqrt(float64(sum)))
	}
	return nil
}

func dotF16BatchAMX(query []float16.Num, vectors [][]float16.Num, results []float32) error {
	if len(query) == 0 || len(vectors) == 0 {
		return nil
	}
	if !features.HasAMX {
		for i, v := range vectors {
			if v == nil {
				continue
			}
			d, err := dotF16Unrolled4x(query, v)
			if err != nil {
				return err
			}
			results[i] = d
		}
		return nil
	}

	n := len(vectors)
	qLen := len(query)
	qPtr := uintptr(unsafe.Pointer(&query[0])) // #nosec G103

	for i := 0; i < n; i++ {
		if len(vectors[i]) != qLen {
			return ErrDimensionMismatch
		}
		vPtr := uintptr(unsafe.Pointer(&vectors[i][0])) // #nosec G103
		results[i] = dotF16AMXKernel(qPtr, vPtr, qLen)
	}
	return nil
}
