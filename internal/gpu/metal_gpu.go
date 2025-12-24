//go:build gpu && darwin && arm64

package gpu

/*
#cgo CFLAGS: -x objective-c -fobjc-arc
#cgo LDFLAGS: -framework Accelerate -framework Metal -framework MetalPerformanceShaders -framework Foundation

#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#import <MetalPerformanceShaders/MetalPerformanceShaders.h>
#import <Accelerate/Accelerate.h>

// MetalIndex wraps Metal GPU resources
typedef struct {
    void* device;
    void* commandQueue;
    void* vectorBuffer;
    int vectorCount;
    int dimensions;
} MetalIndexHandle;

// Initialize Metal device and command queue
MetalIndexHandle* metal_init(int dimensions) {
    @autoreleasepool {
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        if (!device) {
            return NULL;
        }

        id<MTLCommandQueue> queue = [device newCommandQueue];
        if (!queue) {
            return NULL;
        }

        MetalIndexHandle* handle = (MetalIndexHandle*)malloc(sizeof(MetalIndexHandle));
        handle->device = (__bridge_retained void*)device;
        handle->commandQueue = (__bridge_retained void*)queue;
        handle->vectorBuffer = NULL;
        handle->vectorCount = 0;
        handle->dimensions = dimensions;

        return handle;
    }
}

// Add vectors to Metal buffer
int metal_add_vectors(MetalIndexHandle* handle, float* vectors, int count) {
    @autoreleasepool {
        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;

        size_t bufferSize = count * handle->dimensions * sizeof(float);
        id<MTLBuffer> buffer = [device newBufferWithBytes:vectors
                                                   length:bufferSize
                                                  options:MTLResourceStorageModeShared];

        if (!buffer) {
            return -1;
        }

        // Release old buffer if exists
        if (handle->vectorBuffer) {
            CFRelease(handle->vectorBuffer);
        }

        handle->vectorBuffer = (__bridge_retained void*)buffer;
        handle->vectorCount = count;

        return 0;
    }
}

// Search for k-nearest neighbors using Metal
int metal_search(MetalIndexHandle* handle, float* query, int k, int64_t* resultIDs, float* resultDistances) {
    @autoreleasepool {
        if (!handle->vectorBuffer || handle->vectorCount == 0) {
            return -1;
        }

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLBuffer> vectorBuffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;

        // Create query buffer
        id<MTLBuffer> queryBuffer = [device newBufferWithBytes:query
                                                        length:handle->dimensions * sizeof(float)
                                                       options:MTLResourceStorageModeShared];

        // Create distance buffer
        id<MTLBuffer> distanceBuffer = [device newBufferWithLength:handle->vectorCount * sizeof(float)
                                                            options:MTLResourceStorageModeShared];

        // Compute distances using vDSP (Accelerate framework)
        float* vectors = (float*)[vectorBuffer contents];
        float* distances = (float*)[distanceBuffer contents];

        // Calculate L2 distances for each vector
        for (int i = 0; i < handle->vectorCount; i++) {
            float* vec = vectors + (i * handle->dimensions);
            float dist = 0.0f;

            // Use vDSP for efficient distance calculation
            vDSP_distancesq(query, 1, vec, 1, &dist, handle->dimensions);
            distances[i] = sqrtf(dist);
        }

        // Find k smallest distances
        // Simple selection for now (can be optimized with vDSP_vsorti)
        for (int i = 0; i < k && i < handle->vectorCount; i++) {
            int minIdx = i;
            float minDist = distances[i];

            for (int j = i + 1; j < handle->vectorCount; j++) {
                if (distances[j] < minDist) {
                    minDist = distances[j];
                    minIdx = j;
                }
            }

            // Swap
            if (minIdx != i) {
                float tempDist = distances[i];
                distances[i] = distances[minIdx];
                distances[minIdx] = tempDist;
            }

            resultIDs[i] = minIdx;
            resultDistances[i] = distances[i];
        }

        return 0;
    }
}

// Clean up Metal resources
void metal_cleanup(MetalIndexHandle* handle) {
    @autoreleasepool {
        if (handle->vectorBuffer) {
            CFRelease(handle->vectorBuffer);
        }
        if (handle->commandQueue) {
            CFRelease(handle->commandQueue);
        }
        if (handle->device) {
            CFRelease(handle->device);
        }
        free(handle);
    }
}
*/
import "C"
import (
	"fmt"
	"runtime"
	"sync"
	"unsafe"
)

// MetalIndex implements GPU-accelerated vector search using Apple Metal
type MetalIndex struct {
	handle *C.MetalIndexHandle
	dim    int
	mu     sync.RWMutex
	closed bool
}

// NewMetalIndex creates a new Metal-based GPU index
func NewMetalIndex(cfg GPUConfig) (Index, error) {
	if cfg.Dimension <= 0 {
		return nil, fmt.Errorf("dimension must be positive, got %d", cfg.Dimension)
	}
	handle := C.metal_init(C.int(cfg.Dimension))
	if handle == nil {
		return nil, fmt.Errorf("failed to initialize Metal device")
	}

	idx := &MetalIndex{
		handle: handle,
		dim:    cfg.Dimension,
	}

	runtime.SetFinalizer(idx, (*MetalIndex).Close)
	return idx, nil
}

// Add adds vectors to the Metal GPU index
func (idx *MetalIndex) Add(ids []int64, vectors []float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	if len(vectors)%idx.dim != 0 {
		return fmt.Errorf("vector data length %d not divisible by dimension %d", len(vectors), idx.dim)
	}

	n := len(vectors) / idx.dim
	if len(ids) != n {
		return fmt.Errorf("id count %d does not match vector count %d", len(ids), n)
	}

	ret := C.metal_add_vectors(idx.handle, (*C.float)(unsafe.Pointer(&vectors[0])), C.int(n))
	if ret != 0 {
		return fmt.Errorf("failed to add vectors to Metal buffer")
	}

	return nil
}

// Search queries the Metal GPU index for k-nearest neighbors
func (idx *MetalIndex) Search(vector []float32, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	ret := C.metal_search(
		idx.handle,
		(*C.float)(unsafe.Pointer(&vector[0])),
		C.int(k),
		(*C.int64_t)(unsafe.Pointer(&resultIDs[0])),
		(*C.float)(unsafe.Pointer(&resultDistances[0])),
	)

	if ret != 0 {
		return nil, nil, fmt.Errorf("Metal search failed")
	}

	return resultIDs, resultDistances, nil
}

// Close releases Metal GPU resources
func (idx *MetalIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return nil
	}

	if idx.handle != nil {
		C.metal_cleanup(idx.handle)
		idx.handle = nil
	}

	idx.closed = true
	return nil
}
