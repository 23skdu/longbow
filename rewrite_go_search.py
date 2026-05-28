import re

with open('internal/gpu/metal/metal_gpu_optimized.go', 'r') as f:
    content = f.read()

new_search = """// Search queries the optimized Metal GPU index using compute shaders
func (idx *MetalIndexOptimized) Search(vector []float32, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}

	if err := idx.Flush(); err != nil {
		return nil, nil, err
	}

	if idx.pager == nil {
		return nil, nil, fmt.Errorf("GPU pager not initialized")
	}

	n := idx.vectorCount
	if n == 0 {
		return nil, nil, nil
	}

	if k > 2147483647 {
		return nil, nil, fmt.Errorf("k too large")
	}
	if k > n {
		k = n
	}

	start := time.Now()

	numChunks := (n + vectorsPerPage - 1) / vectorsPerPage

	type pageEntry struct {
		ptr   unsafe.Pointer
		nvecs int
	}
	pages := make([]pageEntry, 0, numChunks)
	for chunk := 0; chunk < numChunks; chunk++ {
		pid := idx.pageIDFor(0, chunk)
		pi := idx.pager.PageInfo(pid)
		if pi == nil {
			continue
		}
		if err := idx.pager.Promote(pi); err != nil {
			continue
		}
		gpuPtr := idx.pager.GetGPUAddr(pi)
		if gpuPtr == nil {
			continue
		}
		vecsInChunk := n - chunk*vectorsPerPage
		if vecsInChunk > vectorsPerPage {
			vecsInChunk = vectorsPerPage
		}
		pages = append(pages, pageEntry{ptr: gpuPtr, nvecs: vecsInChunk})
	}

	if len(pages) == 0 {
		return nil, nil, fmt.Errorf("no resident pages available for search")
	}

	numPages := len(pages)
	hPageStarts := make([]C.int, numPages+1)
	hPagePtrs := make([]unsafe.Pointer, numPages)
	for i, p := range pages {
		hPagePtrs[i] = p.ptr
		hPageStarts[i+1] = hPageStarts[i] + C.int(p.nvecs)
	}
	totalVecs := int(hPageStarts[numPages])

	ids := make([]int64, k)
	distances := make([]float32, k)

	ret := C.metal_search_optimized(
		idx.handle,
		(*C.float)(unsafe.Pointer(&vector[0])),
		(*unsafe.Pointer)(unsafe.Pointer(&hPagePtrs[0])),
		(*C.int)(unsafe.Pointer(&hPageStarts[0])),
		C.int(numPages),
		C.int(totalVecs),
		C.int(k),
		(*C.int64_t)(unsafe.Pointer(&ids[0])),
		(*C.float)(unsafe.Pointer(&distances[0])),
	)

	if ret != 0 {
		return nil, nil, fmt.Errorf("failed to search optimized Metal buffer")
	}

	// Because we replaced idBuffer logic directly into idList in Go (like CUDA), 
	// the C returned IDs are actually LOCAL offsets within the totalVecs. 
	// So we must remap local offsets to global IDs using idx.idList.
	// Wait! I will just use idx.idList in Go!
	for i := 0; i < k; i++ {
		localIdx := int(ids[i])
		if localIdx >= 0 && localIdx < len(idx.idList) {
			ids[i] = idx.idList[localIdx]
		}
	}

	metrics.GPUComputeDurationSeconds.WithLabelValues("Apple Silicon GPU (Optimized)", "search").Observe(time.Since(start).Seconds())

	return ids, distances, nil
}"""

content = re.sub(r'// Search queries the optimized Metal GPU index using compute shaders\nfunc \(idx \*MetalIndexOptimized\) Search\(vector \[\]float32, k int\) \(\[\]int64, \[\]float32, error\) \{.*?\n\}\n', new_search + '\n\n', content, flags=re.DOTALL)

with open('internal/gpu/metal/metal_gpu_optimized.go', 'w') as f:
    f.write(content)
