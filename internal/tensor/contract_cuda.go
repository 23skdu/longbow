//go:build cuda

package tensor

// #cgo LDFLAGS: -lcudart -lcublas
// #include <cuda_runtime.h>
// #include <cublas_v2.h>
import "C"
import "unsafe"

// contractCUDA performs tensor contraction on the GPU using cuBLAS.
// Returns true if the contraction was offloaded.
func contractCUDA(a, b, out *Tensor, aAxes, bAxes, aFree, bFree []int) bool {
	if a.Dtype() != DtypeFloat32 || b.Dtype() != DtypeFloat32 || out.Dtype() != DtypeFloat32 {
		return false
	}
	// Only handle 2D contraction (matrix multiply) via cublasSgemm for now
	if len(aFree) != 1 || len(bFree) != 1 || len(aAxes) != 1 {
		return false
	}
	m := a.Shape()[aFree[0]]
	n := b.Shape()[bFree[0]]
	k := a.Shape()[aAxes[0]]

	var handle C.cublasHandle_t
	if stat := C.cublasCreate(&handle); stat != C.CUBLAS_STATUS_SUCCESS {
		return false
	}
	defer C.cublasDestroy(handle)

	alpha := C.float(1.0)
	beta := C.float(0.0)

	aGPU := allocCUDA(a.Data(), m*k*4)
	bGPU := allocCUDA(b.Data(), k*n*4)
	outGPU := allocCUDA(nil, m*n*4)
	defer freeCUDA(aGPU)
	defer freeCUDA(bGPU)
	defer freeCUDA(outGPU)

	C.cublasSgemm(
		handle,
		C.CUBLAS_OP_N, C.CUBLAS_OP_N,
		C.int(m), C.int(n), C.int(k),
		&alpha,
		(*C.float)(aGPU), C.int(m),
		(*C.float)(bGPU), C.int(k),
		&beta,
		(*C.float)(outGPU), C.int(m),
	)

	C.cudaMemcpy(
		unsafe.Pointer(&out.Data()[0]),
		outGPU,
		C.size_t(m*n*4),
		C.cudaMemcpyDeviceToHost,
	)
	return true
}

func allocCUDA(src []byte, size int) unsafe.Pointer {
	var ptr unsafe.Pointer
	C.cudaMalloc(&ptr, C.size_t(size))
	if src != nil {
		C.cudaMemcpy(ptr, unsafe.Pointer(&src[0]), C.size_t(size), C.cudaMemcpyHostToDevice)
	}
	return ptr
}

func freeCUDA(ptr unsafe.Pointer) {
	C.cudaFree(ptr)
}
