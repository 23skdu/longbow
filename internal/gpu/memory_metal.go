//go:build gpu && darwin && arm64

package gpu

/*
#cgo CFLAGS: -x objective-c -fobjc-arc
#cgo LDFLAGS: -framework Metal -framework Foundation

#import <Foundation/Foundation.h>
#import <Metal/Metal.h>

typedef void* MetalDevicePtr;
typedef void* MetalBufferPtr;

#define CACHE_LINE_SIZE 64

MetalBufferPtr metalMalloc(MetalDevicePtr device, size_t size) {
    @autoreleasepool {
        id<MTLDevice> dev = (__bridge id<MTLDevice>)device;
        size_t alignedSize = (size + CACHE_LINE_SIZE - 1) & ~(CACHE_LINE_SIZE - 1);
        id<MTLBuffer> buffer = [dev newBufferWithLength:alignedSize
                                                    options:MTLResourceStorageModeShared];
        if (!buffer) {
            return NULL;
        }
        return (__bridge_retained void*)buffer;
    }
}

void metalFree(MetalBufferPtr buffer) {
    @autoreleasepool {
        if (buffer) {
            CFRelease(buffer);
        }
    }
}

MetalDevicePtr metalGetDefaultDevice() {
    @autoreleasepool {
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        return (__bridge_retained void*)device;
    }
}

void metalMemcpyToBuffer(MetalBufferPtr buffer, void* src, size_t size) {
    @autoreleasepool {
        id<MTLBuffer> buf = (__bridge id<MTLBuffer>)buffer;
        memcpy([buf contents], src, size);
    }
}

void metalMemcpyFromBuffer(void* dst, MetalBufferPtr buffer, size_t size) {
    @autoreleasepool {
        id<MTLBuffer> buf = (__bridge id<MTLBuffer>)buffer;
        memcpy(dst, [buf contents], size);
    }
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

const cacheLineSize = 64

func (p *GPUMemPool) allocateMetalMemoryImpl(size int64) (unsafe.Pointer, error) {
	device := C.metalGetDefaultDevice()
	if device == nil {
		return nil, fmt.Errorf("failed to get Metal device")
	}

	ptr := C.metalMalloc(device, C.size_t(size))
	if ptr == nil {
		return nil, fmt.Errorf("failed to allocate %d bytes on Metal", size)
	}

	p.allocations[unsafe.Pointer(ptr)] = size
	p.usedBytes += size

	return unsafe.Pointer(ptr), nil
}

func (p *GPUMemPool) freeMetalMemoryImpl(ptr unsafe.Pointer) {
	C.metalFree(C.MetalBufferPtr(ptr))
}

func (p *GPUMemPool) metalMemcpyHostToDeviceImpl(hostPtr, devicePtr unsafe.Pointer, size int64) error {
	C.metalMemcpyToBuffer(C.MetalBufferPtr(devicePtr), hostPtr, C.size_t(size))
	return nil
}

func (p *GPUMemPool) metalMemcpyDeviceToHostImpl(devicePtr, hostPtr unsafe.Pointer, size int64) error {
	C.metalMemcpyFromBuffer(hostPtr, C.MetalBufferPtr(devicePtr), C.size_t(size))
	return nil
}
