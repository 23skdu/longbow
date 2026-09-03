//go:build gpu && linux

package cuda

// #cgo LDFLAGS: -lcudart -lcublas
// #include <cuda_runtime.h>
// #include <cublas_v2.h>
import "C"
import "unsafe"

// ContractCUDA performs tensor contraction on the GPU using cuBLAS.
func ContractCUDA(aData, bData, outData []byte, isFloat64 bool, m, n, k int) bool {
	var handle C.cublasHandle_t
	if stat := C.cublasCreate(&handle); stat != C.CUBLAS_STATUS_SUCCESS {
		return false
	}
	defer C.cublasDestroy(handle)

	if !isFloat64 {
		alpha := C.float(1.0)
		beta := C.float(0.0)

		aGPU := allocCUDA(aData, m*k*4)
		bGPU := allocCUDA(bData, k*n*4)
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
			unsafe.Pointer(&outData[0]), // #nosec G103
			outGPU,
			C.size_t(m*n*4),
			C.cudaMemcpyDeviceToHost,
		)
		return true
	}

	// Float64
	alpha := C.double(1.0)
	beta := C.double(0.0)

	aGPU := allocCUDA(aData, m*k*8)
	bGPU := allocCUDA(bData, k*n*8)
	outGPU := allocCUDA(nil, m*n*8)
	defer freeCUDA(aGPU)
	defer freeCUDA(bGPU)
	defer freeCUDA(outGPU)

	C.cublasDgemm(
		handle,
		C.CUBLAS_OP_N, C.CUBLAS_OP_N,
		C.int(m), C.int(n), C.int(k),
		&alpha,
		(*C.double)(aGPU), C.int(m),
		(*C.double)(bGPU), C.int(k),
		&beta,
		(*C.double)(outGPU), C.int(m),
	)

	C.cudaMemcpy(
		unsafe.Pointer(&outData[0]), // #nosec G103
		outGPU,
		C.size_t(m*n*8),
		C.cudaMemcpyDeviceToHost,
	)
	return true
}

func allocCUDA(src []byte, size int) unsafe.Pointer {
	var ptr unsafe.Pointer
	C.cudaMalloc(&ptr, C.size_t(size))
	if src != nil {
		C.cudaMemcpy(ptr, unsafe.Pointer(&src[0]), C.size_t(size), C.cudaMemcpyHostToDevice) // #nosec G103
	}
	return ptr
}

func freeCUDA(ptr unsafe.Pointer) {
	C.cudaFree(ptr)
}
