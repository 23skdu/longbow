//go:build darwin
package gpu

/*
#cgo CFLAGS: -x objective-c -fobjc-arc
#cgo LDFLAGS: -framework Metal -framework Foundation
#import <Metal/Metal.h>
#import <Foundation/Foundation.h>

long get_metal_memory() {
    @autoreleasepool {
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        if (!device) return 0;
        if (@available(macOS 10.12, *)) {
            return (long)device.recommendedMaxWorkingSetSize;
        }
        return 0;
    }
}

char* get_metal_device_name() {
    @autoreleasepool {
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        if (!device) return NULL;
        return strdup([device.name UTF8String]);
    }
}
*/
import "C"
import (
	"runtime"
	"unsafe"
)

func detectMetalGPULive() []GPUInfo {
	if runtime.GOOS != "darwin" {
		return nil
	}

	mem := int64(C.get_metal_memory())
	if mem == 0 {
		return nil
	}

	cName := C.get_metal_device_name()
	defer C.free(unsafe.Pointer(cName))
	name := C.GoString(cName)

	return []GPUInfo{
		{
			Backend:  BackendMetal,
			Name:     name,
			DeviceID: 0,
			MemoryMB: mem / (1024 * 1024),
		},
	}
}
