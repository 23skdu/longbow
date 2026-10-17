//go:build amd64
// +build amd64

package simd

import (
	"math"
	"math/rand"
	"testing"
	"unsafe"
)

func TestInt8AVX512Parity(t *testing.T) {
	sizes := []int{1, 2, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 256, 384, 512, 768, 1024}
	rng := rand.New(rand.NewSource(42))

	for _, n := range sizes {
		a := make([]int8, n)
		b := make([]int8, n)
		for i := range a {
			a[i] = int8(rng.Intn(256) - 128)
			b[i] = int8(rng.Intn(256) - 128)
		}

		refDot, _ := dotInt8Unrolled4x(a, b)
		refL2Sq, _ := l2SquaredInt8Unrolled4x(a, b)
		refEucl, _ := euclideanInt8Unrolled4x(a, b)

		ptrA := uintptr(unsafe.Pointer(&a[0]))
		ptrB := uintptr(unsafe.Pointer(&b[0]))

		// Test dotInt8AVX512Kernel directly on supported hardware
		if features.HasAVX512 {
			gotDot := dotInt8AVX512Kernel(ptrA, ptrB, n)
			if math.Abs(float64(gotDot-refDot)) > 1e-4*math.Max(math.Abs(float64(refDot)), 1) {
				t.Errorf("dotInt8AVX512Kernel n=%d: got %v, want %v", n, gotDot, refDot)
			}
			gotL2Sq := l2SquaredInt8AVX512Kernel(ptrA, ptrB, n)
			if math.Abs(float64(gotL2Sq-refL2Sq)) > 1e-4*math.Max(math.Abs(float64(refL2Sq)), 1) {
				t.Errorf("l2SquaredInt8AVX512Kernel n=%d: got %v, want %v", n, gotL2Sq, refL2Sq)
			}
			gotEucl := euclideanInt8AVX512Kernel(ptrA, ptrB, n)
			if math.Abs(float64(gotEucl-refEucl)) > 1e-4*math.Max(math.Abs(float64(refEucl)), 1) {
				t.Errorf("euclideanInt8AVX512Kernel n=%d: got %v, want %v", n, gotEucl, refEucl)
			}
		}

		// Test dotInt8VNNIKernel directly on supported hardware
		if features.HasVNNI {
			gotDotVNNI := dotInt8VNNIKernel(ptrA, ptrB, n)
			if math.Abs(float64(gotDotVNNI-refDot)) > 1e-4*math.Max(math.Abs(float64(refDot)), 1) {
				t.Errorf("dotInt8VNNIKernel n=%d: got %v, want %v", n, gotDotVNNI, refDot)
			}
		}

		// Test dotInt8AVX512 wrapper (gracefully falls back on non-AVX512 hosts)
		wrapDot, err := dotInt8AVX512(a, b)
		if err != nil {
			t.Fatalf("dotInt8AVX512 n=%d err: %v", n, err)
		}
		if math.Abs(float64(wrapDot-refDot)) > 1e-4*math.Max(math.Abs(float64(refDot)), 1) {
			t.Errorf("dotInt8AVX512 wrapper n=%d: got %v, want %v", n, wrapDot, refDot)
		}
	}
}

func TestUint8AVX512Parity(t *testing.T) {
	sizes := []int{1, 2, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 256, 384, 512, 768, 1024}
	rng := rand.New(rand.NewSource(1337))

	for _, n := range sizes {
		a := make([]uint8, n)
		b := make([]uint8, n)
		for i := range a {
			a[i] = uint8(rng.Intn(256))
			b[i] = uint8(rng.Intn(256))
		}

		refDot, _ := dotUint8Unrolled4x(a, b)
		refL2Sq, _ := l2SquaredUint8Unrolled4x(a, b)
		refEucl, _ := euclideanUint8Unrolled4x(a, b)

		ptrA := uintptr(unsafe.Pointer(&a[0]))
		ptrB := uintptr(unsafe.Pointer(&b[0]))

		// Test direct AVX-512 kernels on supported hardware
		if features.HasAVX512 {
			gotDot := dotUint8AVX512Kernel(ptrA, ptrB, n)
			if math.Abs(float64(gotDot-refDot)) > 1e-4*math.Max(math.Abs(float64(refDot)), 1) {
				t.Errorf("dotUint8AVX512Kernel n=%d: got %v, want %v", n, gotDot, refDot)
			}
			gotEucl := euclideanUint8AVX512Kernel(ptrA, ptrB, n)
			if math.Abs(float64(gotEucl-refEucl)) > 1e-4*math.Max(math.Abs(float64(refEucl)), 1) {
				t.Errorf("euclideanUint8AVX512Kernel n=%d: got %v, want %v", n, gotEucl, refEucl)
			}
			gotL2Sq := l2SquaredUint8AVX512Kernel(ptrA, ptrB, n)
			if math.Abs(float64(gotL2Sq-refL2Sq)) > 1e-4*math.Max(math.Abs(float64(refL2Sq)), 1) {
				t.Errorf("l2SquaredUint8AVX512Kernel n=%d: got %v, want %v", n, gotL2Sq, refL2Sq)
			}
		}

		// Test dotUint8VNNIKernel directly on supported hardware
		if features.HasVNNI {
			gotDotVNNI := dotUint8VNNIKernel(ptrA, ptrB, n)
			if math.Abs(float64(gotDotVNNI-refDot)) > 1e-4*math.Max(math.Abs(float64(refDot)), 1) {
				t.Errorf("dotUint8VNNIKernel n=%d: got %v, want %v", n, gotDotVNNI, refDot)
			}
		}

		// Test dotUint8AVX512 wrapper (gracefully falls back on non-AVX512 hosts)
		wrapDot, err := dotUint8AVX512(a, b)
		if err != nil {
			t.Fatalf("dotUint8AVX512 n=%d err: %v", n, err)
		}
		if math.Abs(float64(wrapDot-refDot)) > 1e-4*math.Max(math.Abs(float64(refDot)), 1) {
			t.Errorf("dotUint8AVX512 wrapper n=%d: got %v, want %v", n, wrapDot, refDot)
		}

		// Test euclideanUint8AVX512 wrapper
		wrapEucl, err := euclideanUint8AVX512(a, b)
		if err != nil {
			t.Fatalf("euclideanUint8AVX512 n=%d err: %v", n, err)
		}
		if math.Abs(float64(wrapEucl-refEucl)) > 1e-4*math.Max(math.Abs(float64(refEucl)), 1) {
			t.Errorf("euclideanUint8AVX512 wrapper n=%d: got %v, want %v", n, wrapEucl, refEucl)
		}

		// Test l2SquaredUint8AVX512 wrapper
		wrapL2Sq, err := l2SquaredUint8AVX512(a, b)
		if err != nil {
			t.Fatalf("l2SquaredUint8AVX512 n=%d err: %v", n, err)
		}
		if math.Abs(float64(wrapL2Sq-refL2Sq)) > 1e-4*math.Max(math.Abs(float64(refL2Sq)), 1) {
			t.Errorf("l2SquaredUint8AVX512 wrapper n=%d: got %v, want %v", n, wrapL2Sq, refL2Sq)
		}
	}
}

func TestCosineInt8AndUint8AVX512Parity(t *testing.T) {
	sizes := []int{8, 16, 32, 64, 128, 384, 768}
	rng := rand.New(rand.NewSource(99))

	for _, n := range sizes {
		// int8
		aI8 := make([]int8, n)
		bI8 := make([]int8, n)
		for i := range aI8 {
			aI8[i] = int8(rng.Intn(250) - 125)
			bI8[i] = int8(rng.Intn(250) - 125)
		}
		refCosI8, _ := cosineDistanceInt8Unrolled4x(aI8, bI8)
		gotCosI8, err := cosineInt8AVX512(aI8, bI8)
		if err != nil {
			t.Fatalf("cosineInt8AVX512 n=%d err: %v", n, err)
		}
		if math.Abs(float64(gotCosI8-refCosI8)) > 5e-3 {
			t.Errorf("cosineInt8AVX512 n=%d: got %v, want %v", n, gotCosI8, refCosI8)
		}

		// uint8
		aU8 := make([]uint8, n)
		bU8 := make([]uint8, n)
		for i := range aU8 {
			aU8[i] = uint8(rng.Intn(250) + 1)
			bU8[i] = uint8(rng.Intn(250) + 1)
		}
		refCosU8, _ := cosineDistanceUint8Unrolled4x(aU8, bU8)
		gotCosU8, err := cosineUint8AVX512(aU8, bU8)
		if err != nil {
			t.Fatalf("cosineUint8AVX512 n=%d err: %v", n, err)
		}
		if math.Abs(float64(gotCosU8-refCosU8)) > 5e-3 {
			t.Errorf("cosineUint8AVX512 n=%d: got %v, want %v", n, gotCosU8, refCosU8)
		}
	}
}
