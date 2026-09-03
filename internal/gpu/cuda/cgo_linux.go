//go:build gpu && linux

package cuda

/*
#cgo LDFLAGS: -lcudart -lcublas
#include <cuda_runtime.h>
#include <cublas_v2.h>
#include <stdlib.h>

// Initialize CUDA runtime
int lb_cudaInit() {
    cudaError_t err = cudaFree(0);
    return (err == cudaSuccess) ? 0 : -1;
}

// Get number of CUDA devices
int lb_cudaGetDeviceCount() {
    int count = 0;
    cudaError_t err = cudaGetDeviceCount(&count);
    return (err == cudaSuccess) ? count : -1;
}

// Get device properties
int lb_cudaGetDeviceName(int device, char* name, int maxLen) {
    struct cudaDeviceProp prop;
    cudaError_t err = cudaGetDeviceProperties(&prop, device);
    if (err != cudaSuccess) {
        return -1;
    }

    int i;
    for (i = 0; i < maxLen - 1 && prop.name[i] != '\0'; i++) {
        name[i] = prop.name[i];
    }
    name[i] = '\0';
    return 0;
}

// Get compute capability
int lb_cudaGetComputeCapability(int device, int* major, int* minor) {
    struct cudaDeviceProp prop;
    cudaError_t err = cudaGetDeviceProperties(&prop, device);
    if (err != cudaSuccess) {
        return -1;
    }
    *major = prop.major;
    *minor = prop.minor;
    return 0;
}

// Get total memory
size_t lb_cudaGetTotalMem(int device) {
    struct cudaDeviceProp prop;
    cudaError_t err = cudaGetDeviceProperties(&prop, device);
    return (err == cudaSuccess) ? prop.totalGlobalMem : 0;
}

// Get free and total memory
int lb_cudaGetMemInfo(size_t* free, size_t* total) {
    cudaError_t err = cudaMemGetInfo(free, total);
    return (err == cudaSuccess) ? 0 : -1;
}

// Set device
int lb_cudaSetDevice(int device) {
    cudaError_t err = cudaSetDevice(device);
    return (err == cudaSuccess) ? 0 : -1;
}

// Synchronize device
int lb_cudaDeviceSynchronize() {
    cudaError_t err = cudaDeviceSynchronize();
    return (err == cudaSuccess) ? 0 : -1;
}

// Reset device
int lb_cudaDeviceReset() {
    cudaError_t err = cudaDeviceReset();
    return (err == cudaSuccess) ? 0 : -1;
}

// Check if CUDA is available
int lb_cudaIsAvailable() {
    int count = 0;
    cudaError_t err = cudaGetDeviceCount(&count);
    return (err == cudaSuccess && count > 0) ? 1 : 0;
}

// Check last CUDA error (does NOT reset error state)
int lb_cudaPeekLastError() {
    cudaError_t err = cudaPeekAtLastError();
    return (int)err;
}

// Get and reset last CUDA error
int lb_cudaGetLastError() {
    cudaError_t err = cudaGetLastError();
    return (int)err;
}

// Pinned host memory allocation
int lb_cudaHostAlloc(void** ptr, size_t size, unsigned int flags) {
    return (int)cudaHostAlloc(ptr, size, flags);
}

int lb_cudaFreeHost(void* ptr) {
    return (int)cudaFreeHost(ptr);
}

int lb_cudaMemcpyAsync(void* dst, const void* src, size_t count, int kind, cudaStream_t stream) {
    return (int)cudaMemcpyAsync(dst, src, count, (enum cudaMemcpyKind)kind, stream);
}

int lb_cudaStreamSynchronize(cudaStream_t stream) {
    return (int)cudaStreamSynchronize(stream);
}
*/
import "C"
import (
	"fmt"
	"sync"
	"unsafe"
)

// Init initializes the CUDA runtime
func Init() error {
	ret := C.lb_cudaInit()
	if ret != 0 {
		return fmt.Errorf("failed to initialize CUDA runtime")
	}
	return nil
}

// GetDeviceCount returns the number of CUDA-capable devices
func GetDeviceCount() int {
	count := C.lb_cudaGetDeviceCount()
	if count < 0 {
		return 0
	}
	return int(count)
}

// GetDeviceName returns the name of the specified CUDA device
func GetDeviceName(deviceID int32) (string, error) {
	buf := make([]C.char, 256)
	ret := C.lb_cudaGetDeviceName(C.int(deviceID), &buf[0], C.int(len(buf)))
	if ret != 0 {
		return "", fmt.Errorf("failed to get device name for device %d", deviceID)
	}
	return C.GoString(&buf[0]), nil
}

// GetComputeCapability returns the compute capability of the specified device
func GetComputeCapability(deviceID int32) (major, minor int, err error) {
	var cMajor, cMinor C.int
	ret := C.lb_cudaGetComputeCapability(C.int(deviceID), &cMajor, &cMinor)
	if ret != 0 {
		return 0, 0, fmt.Errorf("failed to get compute capability for device %d", deviceID)
	}
	return int(cMajor), int(cMinor), nil
}

// GetTotalMemory returns the total global memory of the specified device
func GetTotalMemory(deviceID int32) uint64 {
	mem := C.lb_cudaGetTotalMem(C.int(deviceID))
	return uint64(mem)
}

// GetMemInfo returns the free and total memory of the current device
func GetMemInfo() (free, total uint64, err error) {
	var cFree, cTotal C.size_t
	ret := C.lb_cudaGetMemInfo(&cFree, &cTotal)
	if ret != 0 {
		return 0, 0, fmt.Errorf("failed to get memory info")
	}
	return uint64(cFree), uint64(cTotal), nil
}

// SetDevice sets the current CUDA device
func SetDevice(deviceID int32) error {
	ret := C.lb_cudaSetDevice(C.int(deviceID))
	if ret != 0 {
		return fmt.Errorf("failed to set CUDA device %d", deviceID)
	}
	return nil
}

// Synchronize waits for the current device to finish all operations
func Synchronize() error {
	ret := C.lb_cudaDeviceSynchronize()
	if ret != 0 {
		return fmt.Errorf("failed to synchronize CUDA device")
	}
	return nil
}

// Reset resets the current CUDA device
func Reset() error {
	ret := C.lb_cudaDeviceReset()
	if ret != 0 {
		return fmt.Errorf("failed to reset CUDA device")
	}
	return nil
}

// IsAvailable checks if CUDA is available on the system
func IsAvailable() bool {
	return C.lb_cudaIsAvailable() == 1
}

// PeekLastError returns the last CUDA error without resetting it.
func PeekLastError() error {
	err := C.lb_cudaPeekLastError()
	if err == 0 {
		return nil
	}
	return fmt.Errorf("CUDA error: %d", int(err))
}

// GetLastError returns and resets the last CUDA error.
func GetLastError() error {
	err := C.lb_cudaGetLastError()
	if err == 0 {
		return nil
	}
	return fmt.Errorf("CUDA error: %d", int(err))
}

// MemcpyKind defines direction of CUDA memory copy
type MemcpyKind int

const (
	MemcpyHostToHost     MemcpyKind = 0
	MemcpyHostToDevice   MemcpyKind = 1
	MemcpyDeviceToHost   MemcpyKind = 2
	MemcpyDeviceToDevice MemcpyKind = 3
)

// HostAlloc allocates size bytes of page-locked (pinned) host memory.
func HostAlloc(size int64) (unsafe.Pointer, error) {
	if size <= 0 {
		return nil, fmt.Errorf("invalid size: %d", size)
	}
	var ptr unsafe.Pointer
	ret := C.lb_cudaHostAlloc((*unsafe.Pointer)(unsafe.Pointer(&ptr)), C.size_t(size), C.cudaHostAllocDefault)
	if ret != 0 {
		return nil, fmt.Errorf("cudaHostAlloc failed for size %d (error code %d)", size, int(ret))
	}
	return ptr, nil
}

// FreeHost frees page-locked host memory allocated by HostAlloc.
func FreeHost(ptr unsafe.Pointer) error {
	if ptr == nil {
		return nil
	}
	ret := C.lb_cudaFreeHost(ptr)
	if ret != 0 {
		return fmt.Errorf("cudaFreeHost failed (error code %d)", int(ret))
	}
	return nil
}

// MemcpyAsync copies memory asynchronously between host and device on a stream.
func MemcpyAsync(dst, src unsafe.Pointer, size int64, kind MemcpyKind, stream unsafe.Pointer) error {
	if dst == nil || src == nil || size <= 0 {
		return nil
	}
	ret := C.lb_cudaMemcpyAsync(dst, src, C.size_t(size), C.int(kind), (C.cudaStream_t)(stream))
	if ret != 0 {
		return fmt.Errorf("cudaMemcpyAsync failed (error code %d)", int(ret))
	}
	return nil
}

// StreamSynchronize waits for stream operations to complete.
func StreamSynchronize(stream unsafe.Pointer) error {
	ret := C.lb_cudaStreamSynchronize((C.cudaStream_t)(stream))
	if ret != 0 {
		return fmt.Errorf("cudaStreamSynchronize failed (error code %d)", int(ret))
	}
	return nil
}

// PinnedHostPool provides a reusable cache of pinned host memory buffers to avoid repeated cudaHostAlloc/cudaFreeHost calls.
type PinnedHostPool struct {
	mu      sync.Mutex
	buffers map[int64][]unsafe.Pointer
	closed  bool
}

// NewPinnedHostPool creates a new pool for pinned host buffers.
func NewPinnedHostPool() *PinnedHostPool {
	return &PinnedHostPool{
		buffers: make(map[int64][]unsafe.Pointer),
	}
}

// Get borrows a pinned host buffer of at least size bytes.
func (p *PinnedHostPool) Get(size int64) (unsafe.Pointer, error) {
	if size <= 0 {
		return nil, fmt.Errorf("invalid size %d", size)
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("pool is closed")
	}
	if bufs, ok := p.buffers[size]; ok && len(bufs) > 0 {
		ptr := bufs[len(bufs)-1]
		p.buffers[size] = bufs[:len(bufs)-1]
		p.mu.Unlock()
		return ptr, nil
	}
	p.mu.Unlock()

	return HostAlloc(size)
}

// Put returns a pinned host buffer back to the pool.
func (p *PinnedHostPool) Put(ptr unsafe.Pointer, size int64) {
	if ptr == nil || size <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		_ = FreeHost(ptr)
		return
	}
	// Cap pooled buffers per size to 32 to prevent unbounded growth
	if len(p.buffers[size]) < 32 {
		p.buffers[size] = append(p.buffers[size], ptr)
	} else {
		_ = FreeHost(ptr)
	}
}

// Close frees all pooled buffers.
func (p *PinnedHostPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	for size, bufs := range p.buffers {
		for _, ptr := range bufs {
			_ = FreeHost(ptr)
		}
		delete(p.buffers, size)
	}
	return nil
}

