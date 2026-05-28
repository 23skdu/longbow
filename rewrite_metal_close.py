import re
import sys

with open('internal/gpu/metal/metal_gpu_optimized.go', 'r') as f:
    content = f.read()

new_close = """func (idx *MetalIndexOptimized) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return nil
	}

	if idx.syncTicker != nil {
		idx.syncTicker.Stop()
		close(idx.stopSync)
	}

	_ = idx.Flush()

	if idx.pager != nil {
		idx.pager.Close()
	}

	if idx.memPool != nil {
		idx.memPool.Close()
	}

	if idx.handle != nil {
		C.metal_cleanup_optimized(idx.handle)
		idx.handle = nil
	}

	idx.closed = true
	return nil
}"""

content = re.sub(r'// Close releases GPU resources\nfunc \(idx \*MetalIndexOptimized\) Close\(\) error \{.*?\n\}\n', new_close + '\n\n', content, flags=re.DOTALL)

with open('internal/gpu/metal/metal_gpu_optimized.go', 'w') as f:
    f.write(content)
