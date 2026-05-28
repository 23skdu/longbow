//go:build gpu && linux

package main

/*
#cgo LDFLAGS: -lcudart -lm ${SRCDIR}/../../internal/gpu/cuda/kernels.o
#include <cuda_runtime.h>
#include <stdint.h>

extern void launch_l2_distance_kernel(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream);
extern void launch_pq_distance_kernel(const float* query, const float* pq_codebook, const unsigned char* pq_codes, float* distances, int dim, int numSubQuantizers, int numSubVectors, int count, cudaStream_t stream);
extern void launch_turboquant_distance_kernel(const float* query, const unsigned char* tqData, float* distances, int dim, int pow2, int bitsPerAngle, int count, cudaStream_t stream);
extern void launch_l2_distance_fp16_kernel_optimized(const unsigned short* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream);
extern void select_topk_kernel(const float* distances, const int64_t* ids, int k, int count, float* outDistances, int64_t* outIds);
*/
import "C"

import (
	"fmt"
	"unsafe"
)

func main() {
	dim := 128
	count := 1024
	k := 10

	fmt.Printf("=== CUDA Kernel Profiling ===\n")
	fmt.Printf("dim=%d count=%d k=%d\n\n", dim, count, k)

	h_query := make([]float32, dim)
	for i := range h_query {
		h_query[i] = float32(i) * 0.01
	}

	// 1. l2_distance_kernel
	fmt.Printf("--- l2_distance_kernel (FP32, dim=%d, count=%d) ---\n", dim, count)
	var d_vectors, d_query, d_dists unsafe.Pointer
	C.cudaMalloc(&d_vectors, C.size_t(dim*count*4))
	C.cudaMalloc(&d_query, C.size_t(dim*4))
	C.cudaMalloc(&d_dists, C.size_t(count*4))
	C.cudaMemcpy(d_query, unsafe.Pointer(&h_query[0]), C.size_t(dim*4), C.cudaMemcpyHostToDevice)

	h_vectors := make([]float32, dim*count)
	for i := range h_vectors {
		h_vectors[i] = float32(i) * 0.001
	}
	C.cudaMemcpy(d_vectors, unsafe.Pointer(&h_vectors[0]), C.size_t(dim*count*4), C.cudaMemcpyHostToDevice)
	C.cudaDeviceSynchronize()
	C.launch_l2_distance_kernel(
		(*C.float)(d_vectors), (*C.float)(d_query), (*C.float)(d_dists),
		C.int(dim), C.int(count), nil,
	)
	C.cudaDeviceSynchronize()
	h_dists := make([]float32, count)
	C.cudaMemcpy(unsafe.Pointer(&h_dists[0]), d_dists, C.size_t(count*4), C.cudaMemcpyDeviceToHost)
	fmt.Printf("  first dist=%.4f last dist=%.4f\n", h_dists[0], h_dists[count-1])
	C.cudaFree(d_vectors)
	C.cudaFree(d_query)
	C.cudaFree(d_dists)

	// 2. pq_distance_kernel
	m := 8
	subDim := dim / m
	pqCodebookSize := 256 * m * subDim
	pqCodesSize := count * m

	fmt.Printf("\n--- pq_distance_kernel (dim=%d, m=%d, count=%d) ---\n", dim, m, count)
	var d_query2, d_codebook, d_pqcodes, d_dists2 unsafe.Pointer
	C.cudaMalloc(&d_query2, C.size_t(dim*4))
	C.cudaMalloc(&d_codebook, C.size_t(pqCodebookSize*4))
	C.cudaMalloc(&d_pqcodes, C.size_t(pqCodesSize))
	C.cudaMalloc(&d_dists2, C.size_t(count*4))
	C.cudaMemcpy(d_query2, unsafe.Pointer(&h_query[0]), C.size_t(dim*4), C.cudaMemcpyHostToDevice)

	h_codebook := make([]float32, pqCodebookSize)
	for i := range h_codebook {
		h_codebook[i] = float32(i) * 0.0001
	}
	C.cudaMemcpy(d_codebook, unsafe.Pointer(&h_codebook[0]), C.size_t(pqCodebookSize*4), C.cudaMemcpyHostToDevice)
	h_pqcodes := make([]byte, pqCodesSize)
	for i := range h_pqcodes {
		h_pqcodes[i] = byte(i % 256)
	}
	C.cudaMemcpy(d_pqcodes, unsafe.Pointer(&h_pqcodes[0]), C.size_t(pqCodesSize), C.cudaMemcpyHostToDevice)
	C.cudaDeviceSynchronize()
	C.launch_pq_distance_kernel(
		(*C.float)(d_query2), (*C.float)(d_codebook), (*C.uchar)(d_pqcodes), (*C.float)(d_dists2),
		C.int(dim), C.int(m), C.int(subDim), C.int(count), nil,
	)
	C.cudaDeviceSynchronize()
	h_dists2 := make([]float32, count)
	C.cudaMemcpy(unsafe.Pointer(&h_dists2[0]), d_dists2, C.size_t(count*4), C.cudaMemcpyDeviceToHost)
	fmt.Printf("  first dist=%.4f last dist=%.4f\n", h_dists2[0], h_dists2[count-1])
	C.cudaFree(d_query2)
	C.cudaFree(d_codebook)
	C.cudaFree(d_pqcodes)
	C.cudaFree(d_dists2)

	// 3. turboquant_distance_kernel
	bitsPerAngle := 8
	pow2 := 128
	for pow2 < dim {
		pow2 <<= 1
	}
	tqStride := dim * bitsPerAngle / 8

	fmt.Printf("\n--- turboquant_distance_kernel (dim=%d, bitsPerAngle=%d, count=%d) ---\n", dim, bitsPerAngle, count)
	var d_query3, d_tqdata, d_dists3 unsafe.Pointer
	C.cudaMalloc(&d_query3, C.size_t(dim*4))
	C.cudaMalloc(&d_tqdata, C.size_t(count*tqStride))
	C.cudaMalloc(&d_dists3, C.size_t(count*4))
	C.cudaMemcpy(d_query3, unsafe.Pointer(&h_query[0]), C.size_t(dim*4), C.cudaMemcpyHostToDevice)
	h_tqdata := make([]byte, count*tqStride)
	for i := range h_tqdata {
		h_tqdata[i] = byte(i % 256)
	}
	C.cudaMemcpy(d_tqdata, unsafe.Pointer(&h_tqdata[0]), C.size_t(count*tqStride), C.cudaMemcpyHostToDevice)
	C.cudaDeviceSynchronize()
	C.launch_turboquant_distance_kernel(
		(*C.float)(d_query3), (*C.uchar)(d_tqdata), (*C.float)(d_dists3),
		C.int(dim), C.int(pow2), C.int(bitsPerAngle), C.int(count), nil,
	)
	C.cudaDeviceSynchronize()
	h_dists3 := make([]float32, count)
	C.cudaMemcpy(unsafe.Pointer(&h_dists3[0]), d_dists3, C.size_t(count*4), C.cudaMemcpyDeviceToHost)
	fmt.Printf("  first dist=%.4f last dist=%.4f\n", h_dists3[0], h_dists3[count-1])
	C.cudaFree(d_query3)
	C.cudaFree(d_tqdata)
	C.cudaFree(d_dists3)

	// 4. l2_distance_fp16_kernel_optimized
	fmt.Printf("\n--- l2_distance_fp16_kernel_optimized (dim=%d, count=%d) ---\n", dim, count)
	var d_fp16, d_query4, d_dists4 unsafe.Pointer
	C.cudaMalloc(&d_fp16, C.size_t(dim*count*2))
	C.cudaMalloc(&d_query4, C.size_t(dim*4))
	C.cudaMalloc(&d_dists4, C.size_t(count*4))
	C.cudaMemcpy(d_query4, unsafe.Pointer(&h_query[0]), C.size_t(dim*4), C.cudaMemcpyHostToDevice)
	h_fp16 := make([]uint16, dim*count)
	for i := range h_fp16 {
		f := float32(i) * 0.001
		bits := *(*uint32)(unsafe.Pointer(&f))
		h_fp16[i] = uint16(bits >> 16)
	}
	C.cudaMemcpy(d_fp16, unsafe.Pointer(&h_fp16[0]), C.size_t(dim*count*2), C.cudaMemcpyHostToDevice)
	C.cudaDeviceSynchronize()
	C.launch_l2_distance_fp16_kernel_optimized(
		(*C.ushort)(d_fp16), (*C.float)(d_query4), (*C.float)(d_dists4),
		C.int(dim), C.int(count), nil,
	)
	C.cudaDeviceSynchronize()
	h_dists4 := make([]float32, count)
	C.cudaMemcpy(unsafe.Pointer(&h_dists4[0]), d_dists4, C.size_t(count*4), C.cudaMemcpyDeviceToHost)
	fmt.Printf("  first dist=%.4f last dist=%.4f\n", h_dists4[0], h_dists4[count-1])
	C.cudaFree(d_fp16)
	C.cudaFree(d_query4)
	C.cudaFree(d_dists4)

	// 5. select_topk_kernel
	fmt.Printf("\n--- select_topk_kernel (count=%d, k=%d) ---\n", count, k)
	var d_dists5, d_ids, d_outDists, d_outIds unsafe.Pointer
	C.cudaMalloc(&d_dists5, C.size_t(count*4))
	C.cudaMalloc(&d_ids, C.size_t(count*8))
	C.cudaMalloc(&d_outDists, C.size_t(k*4))
	C.cudaMalloc(&d_outIds, C.size_t(k*8))

	h_scores := make([]float32, count)
	h_ids := make([]int64, count)
	for i := range h_ids {
		h_scores[i] = float32(count - i)
		h_ids[i] = int64(i)
	}
	C.cudaMemcpy(d_dists5, unsafe.Pointer(&h_scores[0]), C.size_t(count*4), C.cudaMemcpyHostToDevice)
	C.cudaMemcpy(d_ids, unsafe.Pointer(&h_ids[0]), C.size_t(count*8), C.cudaMemcpyHostToDevice)
	C.cudaDeviceSynchronize()
	C.select_topk_kernel(
		(*C.float)(d_dists5), (*C.int64_t)(d_ids),
		C.int(k), C.int(count),
		(*C.float)(d_outDists), (*C.int64_t)(d_outIds),
	)
	C.cudaDeviceSynchronize()
	h_outDists := make([]float32, k)
	h_outIds := make([]int64, k)
	C.cudaMemcpy(unsafe.Pointer(&h_outDists[0]), d_outDists, C.size_t(k*4), C.cudaMemcpyDeviceToHost)
	C.cudaMemcpy(unsafe.Pointer(&h_outIds[0]), d_outIds, C.size_t(k*8), C.cudaMemcpyDeviceToHost)
	fmt.Printf("  top-1 id=%d dist=%.4f top-%d id=%d dist=%.4f\n",
		h_outIds[0], h_outDists[0], k, h_outIds[k-1], h_outDists[k-1])
	C.cudaFree(d_dists5)
	C.cudaFree(d_ids)
	C.cudaFree(d_outDists)
	C.cudaFree(d_outIds)

	fmt.Println("\nAll kernels executed successfully. Ready for ncu profiling.")
}
