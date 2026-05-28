//go:build gpu && linux

package main

/*
#cgo LDFLAGS: -lcudart -lm ${SRCDIR}/../../internal/gpu/cuda/kernels.o
#include <cuda_runtime.h>
#include <stdint.h>

extern void launch_l2_distance_kernel(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream);
extern void launch_topk_kernel(const float* distances, const int64_t* ids, int n, int k, float* outDistances, int64_t* outIds, cudaStream_t stream);
extern void launch_l2_distance_large_kernel(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream);
extern void launch_l2_distance_filtered_kernel(const float* vectors, const float* query, unsigned int* results, int* resultCount, const unsigned long long* bitset, int dim, int count, int k, cudaStream_t stream);
extern void launch_l2_distance_kernel_v2(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream);
extern void launch_l2_distance_large_kernel_v2(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream);
extern void launch_turboquant_distance_kernel_v2(const float* query, const unsigned char* tqData, float* distances, int dim, int pow2, int bitsPerAngle, int count, cudaStream_t stream);
*/
import "C"

import (
	"flag"
	"fmt"
	"math/rand"
	"unsafe"
)

var h_query []float32

func main() {
	kernel := flag.String("kernel", "v2", "kernel to profile: v1, v2, large_v2, tq_v2, topk, v1_large")
	dim := flag.Int("dim", 768, "vector dimension")
	count := flag.Int("count", 2048, "number of vectors")
	flag.Parse()

	pow2 := 128
	if *dim < pow2 {
		pow2 = 64
	}
	bitsPerAngle := 8

	fmt.Printf("=== CUDA Kernel Profiling ===\n")
	fmt.Printf("kernel=%s dim=%d count=%d\n\n", *kernel, *dim, *count)

	h_query = make([]float32, *dim)
	for i := range h_query {
		h_query[i] = float32(i) * 0.01
	}

	switch *kernel {
	case "v1":
		runV1(*dim, *count)
	case "v1_large":
		runV1Large(*dim, *count)
	case "v2":
		runV2(*dim, *count)
	case "large_v2":
		runLargeV2(*dim, *count)
	case "tq_v2":
		runTQV2(*dim, *count, pow2, bitsPerAngle)
	case "topk":
		runTopK(*count, 10)
	default:
		fmt.Printf("unknown kernel: %s\n", *kernel)
	}

	fmt.Println("\nDone.")
}

func runV1(dim, count int) {
	fmt.Printf("--- l2_distance_kernel (v1) dim=%d count=%d ---\n", dim, count)
	d_vectors, d_query, d_dists := allocFP32(dim, count, dim)
	C.cudaDeviceSynchronize()
	C.launch_l2_distance_kernel(
		(*C.float)(d_vectors), (*C.float)(d_query), (*C.float)(d_dists),
		C.int(dim), C.int(count), nil,
	)
	C.cudaDeviceSynchronize()
	C.cudaFree(d_vectors)
	C.cudaFree(d_query)
	C.cudaFree(d_dists)
	fmt.Println("  done")
}

func runV1Large(dim, count int) {
	fmt.Printf("--- l2_distance_large_kernel (v1) dim=%d count=%d ---\n", dim, count)
	d_vectors, d_query, d_dists := allocFP32(dim, count, dim)
	C.cudaDeviceSynchronize()
	C.launch_l2_distance_large_kernel(
		(*C.float)(d_vectors), (*C.float)(d_query), (*C.float)(d_dists),
		C.int(dim), C.int(count), nil,
	)
	C.cudaDeviceSynchronize()
	C.cudaFree(d_vectors)
	C.cudaFree(d_query)
	C.cudaFree(d_dists)
	fmt.Println("  done")
}

func runV2(dim, count int) {
	fmt.Printf("--- l2_distance_kernel_v2 dim=%d count=%d ---\n", dim, count)
	d_vectors, d_query, d_dists := allocFP32(dim, count, dim)
	C.cudaDeviceSynchronize()
	C.launch_l2_distance_kernel_v2(
		(*C.float)(d_vectors), (*C.float)(d_query), (*C.float)(d_dists),
		C.int(dim), C.int(count), nil,
	)
	C.cudaDeviceSynchronize()
	C.cudaFree(d_vectors)
	C.cudaFree(d_query)
	C.cudaFree(d_dists)
	fmt.Println("  done")
}

func runLargeV2(dim, count int) {
	fmt.Printf("--- l2_distance_kernel_large_v2 dim=%d count=%d ---\n", dim, count)
	d_vectors, d_query, d_dists := allocFP32(dim, count, dim)
	C.cudaDeviceSynchronize()
	C.launch_l2_distance_large_kernel_v2(
		(*C.float)(d_vectors), (*C.float)(d_query), (*C.float)(d_dists),
		C.int(dim), C.int(count), nil,
	)
	C.cudaDeviceSynchronize()
	C.cudaFree(d_vectors)
	C.cudaFree(d_query)
	C.cudaFree(d_dists)
	fmt.Println("  done")
}

func runTQV2(dim, count, pow2, bitsPerAngle int) {
	fmt.Printf("--- turboquant_distance_kernel_v2 dim=%d count=%d pow2=%d bitsPerAngle=%d ---\n", dim, count, pow2, bitsPerAngle)

	angleCount := pow2 - 1
	angleBytes := (angleCount * bitsPerAngle + 7) / 8
	qjlBytes := (pow2 + 7) / 8
	stride := 4 + angleBytes + qjlBytes

	h_tq := make([]byte, count*stride)
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < count; i++ {
		off := i * stride
		*(*float32)(unsafe.Pointer(&h_tq[off])) = 1.0 + rng.Float32()
		for j := 0; j < angleBytes; j++ {
			h_tq[off+4+j] = byte(rng.Intn(256))
		}
		for j := 0; j < qjlBytes; j++ {
			h_tq[off+4+angleBytes+j] = byte(rng.Intn(256))
		}
	}

	var d_tq unsafe.Pointer
	C.cudaMalloc(&d_tq, C.size_t(count*stride))
	C.cudaMemcpy(d_tq, unsafe.Pointer(&h_tq[0]), C.size_t(count*stride), C.cudaMemcpyHostToDevice)

	var d_query, d_dists unsafe.Pointer
	C.cudaMalloc(&d_query, C.size_t(dim*4))
	C.cudaMalloc(&d_dists, C.size_t(count*4))
	C.cudaMemcpy(d_query, unsafe.Pointer(&h_query[0]), C.size_t(dim*4), C.cudaMemcpyHostToDevice)

	C.cudaDeviceSynchronize()
	C.launch_turboquant_distance_kernel_v2(
		(*C.float)(d_query), (*C.uchar)(d_tq), (*C.float)(d_dists),
		C.int(dim), C.int(pow2), C.int(bitsPerAngle), C.int(count), nil,
	)
	C.cudaDeviceSynchronize()

	C.cudaFree(d_tq)
	C.cudaFree(d_query)
	C.cudaFree(d_dists)
	fmt.Println("  done")
}

func runTopK(count, k int) {
	fmt.Printf("--- select_topk_kernel count=%d k=%d ---\n", count, k)
	d_dists, d_ids, d_outDists, d_outIds := allocTopK(count, k)
	C.cudaDeviceSynchronize()
	C.launch_topk_kernel(
		(*C.float)(d_dists), (*C.int64_t)(d_ids),
		C.int(count), C.int(k),
		(*C.float)(d_outDists), (*C.int64_t)(d_outIds), nil,
	)
	C.cudaDeviceSynchronize()
	C.cudaFree(d_dists)
	C.cudaFree(d_ids)
	C.cudaFree(d_outDists)
	C.cudaFree(d_outIds)
	fmt.Println("  done")
}

func allocFP32(dim, count, queryDim int) (unsafe.Pointer, unsafe.Pointer, unsafe.Pointer) {
	var d_vectors, d_query, d_dists unsafe.Pointer
	C.cudaMalloc(&d_vectors, C.size_t(dim*count*4))
	C.cudaMalloc(&d_query, C.size_t(queryDim*4))
	C.cudaMalloc(&d_dists, C.size_t(count*4))
	C.cudaMemcpy(d_query, unsafe.Pointer(&h_query[0]), C.size_t(queryDim*4), C.cudaMemcpyHostToDevice)
	h_vectors := make([]float32, dim*count)
	for i := range h_vectors {
		h_vectors[i] = float32(i) * 0.001
	}
	C.cudaMemcpy(d_vectors, unsafe.Pointer(&h_vectors[0]), C.size_t(dim*count*4), C.cudaMemcpyHostToDevice)
	return d_vectors, d_query, d_dists
}

func allocTopK(count, k int) (unsafe.Pointer, unsafe.Pointer, unsafe.Pointer, unsafe.Pointer) {
	var d_dists, d_ids, d_outDists, d_outIds unsafe.Pointer
	C.cudaMalloc(&d_dists, C.size_t(count*4))
	C.cudaMalloc(&d_ids, C.size_t(count*8))
	C.cudaMalloc(&d_outDists, C.size_t(k*4))
	C.cudaMalloc(&d_outIds, C.size_t(k*8))
	h_scores := make([]float32, count)
	h_ids := make([]int64, count)
	for i := range h_ids {
		h_scores[i] = float32(count - i)
		h_ids[i] = int64(i)
	}
	C.cudaMemcpy(d_dists, unsafe.Pointer(&h_scores[0]), C.size_t(count*4), C.cudaMemcpyHostToDevice)
	C.cudaMemcpy(d_ids, unsafe.Pointer(&h_ids[0]), C.size_t(count*8), C.cudaMemcpyHostToDevice)
	return d_dists, d_ids, d_outDists, d_outIds
}
