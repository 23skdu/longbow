//go:build gpu && darwin && arm64

package metal

import (
	"fmt"
	"sync"
	"unsafe"
)

/*
#cgo CFLAGS: -x objective-c -fobjc-arc
#cgo LDFLAGS: -framework Metal -framework Foundation

#import <Metal/Metal.h>
#import <Foundation/Foundation.h>

typedef struct {
    void* device;
    void* commandQueue;
    void* library;
} MetalContextHandle;

MetalContextHandle* init_metal_context(const void* libData, int libLen) {
    @autoreleasepool {
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        if (!device) return NULL;

        id<MTLCommandQueue> queue = [device newCommandQueue];
        if (!queue) return NULL;

        NSError* error = nil;
        dispatch_data_t data = dispatch_data_create(libData, libLen, nil, DISPATCH_DATA_DESTRUCTOR_DEFAULT);
        id<MTLLibrary> library = [device newLibraryWithData:data error:&error];
        
        if (!library) {
            NSLog(@"Failed to load Metal library: %@", error);
            return NULL;
        }

        MetalContextHandle* handle = (MetalContextHandle*)malloc(sizeof(MetalContextHandle));
        handle->device = (__bridge_retained void*)device;
        handle->commandQueue = (__bridge_retained void*)queue;
        handle->library = (__bridge_retained void*)library;
        return handle;
    }
}

void* get_metal_function(MetalContextHandle* handle, const char* name) {
    @autoreleasepool {
        id<MTLLibrary> library = (__bridge id<MTLLibrary>)handle->library;
        id<MTLFunction> function = [library newFunctionWithName:[NSString stringWithUTF8String:name]];
        return (__bridge_retained void*)function;
    }
}

void* create_pipeline_state(MetalContextHandle* handle, void* function) {
    @autoreleasepool {
        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLFunction> func = (__bridge id<MTLFunction>)function;
        NSError* error = nil;
        id<MTLComputePipelineState> pipeline = [device newComputePipelineStateWithFunction:func error:&error];
        if (!pipeline) {
            NSLog(@"Failed to create pipeline state: %@", error);
            return NULL;
        }
        return (__bridge_retained void*)pipeline;
    }
}
*/
import "C"

type MetalContext struct {
	handle *C.MetalContextHandle
	mu     sync.RWMutex
	pipelines map[string]*C.void
}

var (
	globalContext *MetalContext
	contextOnce   sync.Once
)

// Initialize the global Metal context from embedded library data
func InitGlobalContext(libData []byte) error {
	var err error
	contextOnce.Do(func() {
		ptr := unsafe.Pointer(&libData[0])
		handle := C.init_metal_context(ptr, C.int(len(libData)))
		if handle == nil {
			err = fmt.Errorf("failed to initialize Metal context")
			return
		}
		globalContext = &MetalContext{
			handle:    handle,
			pipelines: make(map[string]*C.void),
		}
	})
	return err
}

func GetContext() *MetalContext {
	return globalContext
}

func (c *MetalContext) GetDevice() unsafe.Pointer {
	return c.handle.device
}

func (c *MetalContext) GetCommandQueue() unsafe.Pointer {
	return c.handle.commandQueue
}

func (c *MetalContext) GetPipelineState(kernelName string) (unsafe.Pointer, error) {
	c.mu.RLock()
	if p, ok := c.pipelines[kernelName]; ok {
		c.mu.RUnlock()
		return unsafe.Pointer(p), nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double check
	if p, ok := c.pipelines[kernelName]; ok {
		return unsafe.Pointer(p), nil
	}

	cKernelName := C.CString(kernelName)
	defer C.free(unsafe.Pointer(cKernelName))

	fn := C.get_metal_function(c.handle, cKernelName)
	if fn == nil {
		return nil, fmt.Errorf("kernel %s not found in library", kernelName)
	}
	defer C.CFRelease(fn)

	pipeline := C.create_pipeline_state(c.handle, fn)
	if pipeline == nil {
		return nil, fmt.Errorf("failed to create pipeline for %s", kernelName)
	}

	c.pipelines[kernelName] = (*C.void)(pipeline)
	return unsafe.Pointer(pipeline), nil
}
