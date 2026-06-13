//go:build amd64 && emerald

package simd

import (
	"math"
	"unsafe"
)

// Assembly declarations
//
//go:noescape
func enableAMX()

//go:noescape
func releaseTiles()

//go:noescape
func dotAMXKernelBF16(a, b uintptr, n int) float32

//go:noescape
func dotAMXKernelINT8(a, b uintptr, n int) int32

//go:noescape
func matMulAMXKernelBF16(a, b, dst uintptr, m, n, k int)

// f32ToBF16 converts a float32 value to BF16 (upper 16 bits of float32).
func f32ToBF16(v float32) uint16 {
	return uint16(math.Float32bits(v) >> 16)
}

func euclideanAMX(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, ErrDimensionMismatch
	}
	if len(a) == 0 {
		return 0, nil
	}
	if !features.HasAMX {
		return euclideanGeneric(a, b)
	}
	sum, err := l2SquaredAMX(a, b)
	if err != nil {
		return 0, err
	}
	return float32(math.Sqrt(float64(sum))), nil
}

func dotAMX(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, ErrDimensionMismatch
	}
	if len(a) == 0 {
		return 0, nil
	}
	if !features.HasAMX {
		return dotGeneric(a, b)
	}

	n := len(a)
	aBF16 := make([]uint16, n)
	bBF16 := make([]uint16, n)
	for i := range a {
		aBF16[i] = f32ToBF16(a[i])
		bBF16[i] = f32ToBF16(b[i])
	}
	return dotAMXKernelBF16(
		uintptr(unsafe.Pointer(&aBF16[0])), // #nosec G103
		uintptr(unsafe.Pointer(&bBF16[0])), // #nosec G103
		n,
	), nil
}

func l2SquaredAMX(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, ErrDimensionMismatch
	}
	if len(a) == 0 {
		return 0, nil
	}
	if !features.HasAMX {
		return L2SquaredFloat32(a, b)
	}

	n := len(a)
	aBF16 := make([]uint16, n)
	bBF16 := make([]uint16, n)
	for i := range a {
		aBF16[i] = f32ToBF16(a[i])
		bBF16[i] = f32ToBF16(b[i])
	}

	aPtr := uintptr(unsafe.Pointer(&aBF16[0])) // #nosec G103
	bPtr := uintptr(unsafe.Pointer(&bBF16[0])) // #nosec G103

	dotAA := dotAMXKernelBF16(aPtr, aPtr, n)
	dotBB := dotAMXKernelBF16(bPtr, bPtr, n)
	dotAB := dotAMXKernelBF16(aPtr, bPtr, n)
	return dotAA + dotBB - 2*dotAB, nil
}

func matMulAMX(a, b []float32, m, n, k int, dst []float32) {
	if features.HasAVX2 && n%8 == 0 {
		matMulAVX2(a, b, m, n, k, dst)
		return
	}
	matMulGeneric(a, b, m, n, k, dst)
}

func euclideanBatchAMX(query []float32, vectors [][]float32, results []float32) error {
	if len(query) == 0 || len(vectors) == 0 {
		return nil
	}
	if !features.HasAMX {
		return euclideanBatchGeneric(query, vectors, results)
	}

	n := len(vectors)
	qLen := len(query)
	qBF16 := make([]uint16, qLen)
	for i := range query {
		qBF16[i] = f32ToBF16(query[i])
	}
	qPtr := uintptr(unsafe.Pointer(&qBF16[0])) // #nosec G103
	dotQQ := dotAMXKernelBF16(qPtr, qPtr, qLen)

	for i := 0; i < n; i++ {
		if len(vectors[i]) != qLen {
			return ErrDimensionMismatch
		}
		vBF16 := make([]uint16, qLen)
		for j := range vectors[i] {
			vBF16[j] = f32ToBF16(vectors[i][j])
		}
		vPtr := uintptr(unsafe.Pointer(&vBF16[0])) // #nosec G103
		dotVV := dotAMXKernelBF16(vPtr, vPtr, qLen)
		dotQV := dotAMXKernelBF16(qPtr, vPtr, qLen)
		sum := dotQQ + dotVV - 2*dotQV
		if sum < 0 {
			sum = 0
		}
		results[i] = float32(math.Sqrt(float64(sum)))
	}
	return nil
}

func dotBatchAMX(query []float32, vectors [][]float32, results []float32) error {
	if len(query) == 0 || len(vectors) == 0 {
		return nil
	}
	if !features.HasAMX {
		return dotBatchGeneric(query, vectors, results)
	}

	n := len(vectors)
	qLen := len(query)
	qBF16 := make([]uint16, qLen)
	for i := range query {
		qBF16[i] = f32ToBF16(query[i])
	}
	qPtr := uintptr(unsafe.Pointer(&qBF16[0])) // #nosec G103

	for i := 0; i < n; i++ {
		if len(vectors[i]) != qLen {
			return ErrDimensionMismatch
		}
		vBF16 := make([]uint16, qLen)
		for j := range vectors[i] {
			vBF16[j] = f32ToBF16(vectors[i][j])
		}
		vPtr := uintptr(unsafe.Pointer(&vBF16[0])) // #nosec G103
		results[i] = dotAMXKernelBF16(qPtr, vPtr, qLen)
	}
	return nil
}
