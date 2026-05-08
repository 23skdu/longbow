//go:build darwin && arm64

package amx

/*
#cgo LDFLAGS: -framework Accelerate
#include <Accelerate/Accelerate.h>

float dot_amx(float* a, float* b, int n) {
    float result = 0;
    vDSP_dotpr(a, 1, b, 1, &result, n);
    return result;
}

float l2_amx(float* a, float* b, int n) {
    // L2 = sum((a-b)^2)
    // We can use vDSP_vsub and then vDSP_dotpr
    // However, for best performance we might want to use a temporary buffer
    // or a single-pass implementation if available in Accelerate.
    // vDSP_distancesq is available in newer versions of Accelerate.
    float result = 0;
    float* diff = (float*)malloc(n * sizeof(float));
    vDSP_vsub(b, 1, a, 1, diff, 1, n);
    vDSP_dotpr(diff, 1, diff, 1, &result, n);
    free(diff);
    return result;
}
*/
import "C"
import (
	"fmt"
	"math"
	"unsafe"
)

func DotAMX(a, b []float32) (float32, error) {
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) > math.MaxInt32 { // #nosec G115
		return 0, fmt.Errorf("vector length %d exceeds MaxInt32 for AMX", len(a))
	}
	return float32(C.dot_amx((*C.float)(unsafe.Pointer(&a[0])), (*C.float)(unsafe.Pointer(&b[0])), C.int(len(a)))), nil // #nosec G115
}

func L2AMX(a, b []float32) (float32, error) {
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) > math.MaxInt32 { // #nosec G115
		return 0, fmt.Errorf("vector length %d exceeds MaxInt32 for AMX", len(a))
	}
	// Note: malloc/free in every call is slow. 
	// For high-dim, we should use a pre-allocated scratch buffer per worker.
	return float32(C.l2_amx((*C.float)(unsafe.Pointer(&a[0])), (*C.float)(unsafe.Pointer(&b[0])), C.int(len(a)))), nil // #nosec G115
}
