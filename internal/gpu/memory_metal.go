//go:build gpu && darwin && arm64

package gpu

/*
#cgo CFLAGS: -x objective-c -fobjc-arc
#cgo LDFLAGS: -framework Metal -framework Foundation

#import <Foundation/Foundation.h>
#import <Metal/Metal.h>

// Cache line size for optimal memory access
#define CACHE_LINE_SIZE 64

// Aligned allocation helper
void* metalMallocAligned(id<MTLDevice> device, size_t size, size_t alignment) {
    @autoreleasepool {
        // Metal buffers are naturally aligned, but ensure size is multiple of alignment
        size_t alignedSize = (size + alignment - 1) & ~(alignment - 1);
        id<MTLBuffer> buffer = [device newBufferWithLength:alignedSize
                                                    options:MTLResourceStorageModeShared];
        if (!buffer) {
            return NULL;
        }
        return (__bridge_retained void*)buffer;
    }
}

// Allocate Metal buffer with cache-line alignment
void* metalMalloc(id<MTLDevice> device, size_t size) {
    @autoreleasepool {
        // Use 64-byte alignment for cache line optimal access
        size_t alignedSize = (size + CACHE_LINE_SIZE - 1) & ~(CACHE_LINE_SIZE - 1);
        id<MTLBuffer> buffer = [device newBufferWithLength:alignedSize
                                                    options:MTLResourceStorageModeShared];
        if (!buffer) {
            return NULL;
        }
        return (__bridge_retained void*)buffer;
    }
}

// Get optimal buffer length with alignment
size_t metalAlignedSize(size_t size) {
    return (size + CACHE_LINE_SIZE - 1) & ~(CACHE_LINE_SIZE - 1);
}

// Free Metal buffer
void metalFree(void* buffer) {
    @autoreleasepool {
        if (buffer) {
            CFRelease(buffer);
        }
    }
}

// Get Metal device default
id<MTLDevice> metalGetDefaultDevice() {
    @autoreleasepool {
        return MTLCreateSystemDefaultDevice();
    }
}

// Copy to Metal buffer
void metalMemcpyToBuffer(id<MTLBuffer> buffer, void* src, size_t size) {
    @autoreleasepool {
        memcpy([buffer contents], src, size);
    }
}

// Copy from Metal buffer
void metalMemcpyFromBuffer(void* dst, id<MTLBuffer> buffer, size_t size) {
    @autoreleasepool {
        memcpy(dst, [buffer contents], size);
    }
}

// Get buffer contents pointer for zero-copy access
void* metalBufferContents(void* buffer) {
    @autoreleasepool {
        id<MTLBuffer> mtlBuffer = (__bridge id<MTLBuffer>)buffer;
        return [mtlBuffer contents];
    }
}
*/
import "C"
import (
	"fmt"
	"unsafe"
)

const (
	cacheLineSize = 64
)

// AlignSize rounds up size to nearest cache line boundary
func AlignSize(size int) int {
	return (size + cacheLineSize - 1) & ^(cacheLineSize - 1)
}

// GetBufferContents returns direct pointer to Metal buffer contents for zero-copy
func GetBufferContents(buffer unsafe.Pointer) unsafe.Pointer {
	return C.metalBufferContents(buffer)
}

// allocateMetalMemory allocates memory using Metal
func (p *GPUMemPool) allocateMetalMemory(size int64) (unsafe.Pointer, error) {
	device := C.metalGetDefaultDevice()
	if device == nil {
		return nil, fmt.Errorf("failed to get Metal device")
	}

	ptr := C.metalMalloc(device, C.size_t(size))
	if ptr == nil {
		return nil, fmt.Errorf("failed to allocate %d bytes on Metal", size)
	}

	p.allocations[ptr] = size
	p.usedBytes += size

	return ptr, nil
}

// freeMetalMemory frees Metal memory
func (p *GPUMemPool) freeMetalMemory(ptr unsafe.Pointer) error {
	C.metalFree(ptr)
	return nil
}

// metalMemcpyHostToDevice copies data from host to Metal buffer
func (p *GPUMemPool) metalMemcpyHostToDevice(hostPtr, devicePtr unsafe.Pointer, size int64) error {
	C.metalMemcpyToBuffer(devicePtr, hostPtr, C.size_t(size))
	return nil
}

// metalMemcpyDeviceToHost copies data from Metal buffer to host
func (p *GPUMemPool) metalMemcpyDeviceToHost(devicePtr, hostPtr unsafe.Pointer, size int64) error {
	C.metalMemcpyFromBuffer(hostPtr, devicePtr, C.size_t(size))
	return nil
}
