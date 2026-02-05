//go:build gpu && darwin && arm64

package gpu

/*
#cgo CFLAGS: -x objective-c -fobjc-arc
#cgo LDFLAGS: -framework Metal -framework Foundation

#import <Foundation/Foundation.h>
#import <Metal/Metal.h>

// Allocate Metal buffer
void* metalMalloc(id<MTLDevice> device, size_t size) {
    @autoreleasepool {
        id<MTLBuffer> buffer = [device newBufferWithLength:size
                                                    options:MTLResourceStorageModeShared];
        if (!buffer) {
            return NULL;
        }
        return (__bridge_retained void*)buffer;
    }
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
*/
import "C"
import (
	"fmt"
	"unsafe"
)

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
