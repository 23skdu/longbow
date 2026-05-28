//go:build gpu && linux

package main

/*
#cgo LDFLAGS: -lcudart -lm ${SRCDIR}/../../internal/gpu/cuda/kernels.o
#include <cuda_runtime.h>
#include <stdint.h>

extern void launch_l2_distance_kernel(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream);
extern void launch_l2_distance_fp16_kernel(const unsigned short* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream);
extern void launch_turboquant_distance_kernel(const float* query, const unsigned char* tqData, float* distances, int dim, int pow2, int bitsPerAngle, int count, cudaStream_t stream);
extern void launch_topk_kernel(const float* distances, const int64_t* ids, int k, int count, float* outDistances, int64_t* outIds);
extern void launch_l2_distance_large_kernel(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream);
extern void launch_l2_distance_filtered_kernel(const float* vectors, const float* query, unsigned int* results, int* resultCount, const unsigned long long* bitset, int dim, int count, int k, cudaStream_t stream);
*/
import "C"

import (
	"fmt"
	"unsafe"
)

var h_query []float32

func main() {
	dims := []int{128, 384, 768, 1536, 3072}
	count := 2048
	k := 10

	fmt.Printf("=== CUDA Kernel Profiling ===\n\n")

	// Pre-populate host query
	h_query = make([]float32, dims[len(dims)-1])
	for i := range h_query {
		h_query[i] = float32(i) * 0.01
	}

	for _, dim := range dims {
		fmt.Printf("=== dim=%d count=%d ===\n", dim, count)

		// 1. FP32 L2 distance kernel
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
		fmt.Printf("  l2_distance_kernel: done\n")

		// 2. TQ distance kernel
		bitsPerAngle := 8
		pow2 := 128
		for pow2 < dim {
			pow2 <<= 1
		}
		tqStride := dim * bitsPerAngle / 8
		d_query2, d_tqdata, d_dists2 := allocTQ(dim, count, tqStride)
		C.cudaDeviceSynchronize()
		C.launch_turboquant_distance_kernel(
			(*C.float)(d_query2), (*C.uchar)(d_tqdata), (*C.float)(d_dists2),
			C.int(dim), C.int(pow2), C.int(bitsPerAngle), C.int(count), nil,
		)
		C.cudaDeviceSynchronize()
		C.cudaFree(d_query2)
		C.cudaFree(d_tqdata)
		C.cudaFree(d_dists2)
		fmt.Printf("  turboquant_distance_kernel: done\n")

		// 3. Top-K kernel
		d_dists3, d_ids, d_outDists, d_outIds := allocTopK(count, k)
		C.cudaDeviceSynchronize()
		C.launch_topk_kernel(
			(*C.float)(d_dists3), (*C.int64_t)(d_ids),
			C.int(k), C.int(count),
			(*C.float)(d_outDists), (*C.int64_t)(d_outIds),
		)
		C.cudaDeviceSynchronize()
		C.cudaFree(d_dists3)
		C.cudaFree(d_ids)
		C.cudaFree(d_outDists)
		C.cudaFree(d_outIds)
		fmt.Printf("  launch_topk_kernel: done\n")

		// 4. Large L2 distance kernel (blocked for dim > 1024)
		if dim > 1024 {
			d_vectors4, d_query4, d_dists4 := allocFP32(dim, count, dim)
			C.cudaDeviceSynchronize()
			C.launch_l2_distance_large_kernel(
				(*C.float)(d_vectors4), (*C.float)(d_query4), (*C.float)(d_dists4),
				C.int(dim), C.int(count), nil,
			)
			C.cudaDeviceSynchronize()
			C.cudaFree(d_vectors4)
			C.cudaFree(d_query4)
			C.cudaFree(d_dists4)
			fmt.Printf("  l2_distance_large_kernel: done\n")
		}
	}

	fmt.Println("\nAll kernels executed. Ready for ncu profiling.")
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

func allocTQ(dim, count, tqStride int) (unsafe.Pointer, unsafe.Pointer, unsafe.Pointer) {
	var d_query, d_tqdata, d_dists unsafe.Pointer
	C.cudaMalloc(&d_query, C.size_t(dim*4))
	C.cudaMalloc(&d_tqdata, C.size_t(count*tqStride))
	C.cudaMalloc(&d_dists, C.size_t(count*4))
	C.cudaMemcpy(d_query, unsafe.Pointer(&h_query[0]), C.size_t(dim*4), C.cudaMemcpyHostToDevice)
	h_tqdata := make([]byte, count*tqStride)
	for i := range h_tqdata {
		h_tqdata[i] = byte(i % 256)
	}
	C.cudaMemcpy(d_tqdata, unsafe.Pointer(&h_tqdata[0]), C.size_t(count*tqStride), C.cudaMemcpyHostToDevice)
	return d_query, d_tqdata, d_dists
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
