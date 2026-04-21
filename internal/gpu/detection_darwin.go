//go:build darwin
package gpu

/*
#cgo LDFLAGS: -framework Metal -framework Foundation
#include "metal_info_darwin.h"
#include <stdlib.h>
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
	if cName == nil {
		return nil
	}
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
