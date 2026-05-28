import re
import sys

with open('internal/gpu/metal/metal_gpu_optimized.go', 'r') as f:
    content = f.read()

# 1. Update NewMetalIndexOptimized
new_new_method = """func NewMetalIndexOptimized(cfg types.GPUConfig) (types.Index, error) {
	libData, err := metalFS.ReadFile("kernels.metallib")
	if err != nil {
		return nil, fmt.Errorf("failed to read embedded metal library: %w", err)
	}

	if err := InitGlobalContext(libData); err != nil {
		return nil, err
	}

	ctx := GetContext()
	if ctx == nil {
		return nil, fmt.Errorf("failed to get shared metal context")
	}

	handle := C.metal_init_optimized(C.int(cfg.Dimension))
	if handle == nil {
		return nil, fmt.Errorf("failed to initialize optimized Metal device")
	}

	// Resolve all required pipelines
	l2, err := ctx.GetPipelineState("compute_l2_distances")
	if err != nil {
		return nil, err
	}
	cosine, err := ctx.GetPipelineState("compute_cosine_similarity")
	if err != nil {
		return nil, err
	}
	dot, err := ctx.GetPipelineState("compute_dot_product")
	if err != nil {
		return nil, err
	}
	topK, err := ctx.GetPipelineState("find_top_k_heap")
	if err != nil {
		return nil, err
	}
	l2Fp16, err := ctx.GetPipelineState("compute_l2_distances_fp16")
	if err != nil {
		return nil, err
	}
	cosineFp16, err := ctx.GetPipelineState("compute_cosine_similarity_fp16")
	if err != nil {
		return nil, err
	}
	dotFp16, err := ctx.GetPipelineState("compute_dot_product_fp16")
	if err != nil {
		return nil, err
	}
	l2C128, err := ctx.GetPipelineState("compute_l2_distances_complex128")
	if err != nil {
		return nil, err
	}
	cosineC128, err := ctx.GetPipelineState("compute_cosine_similarity_complex128")
	if err != nil {
		return nil, err
	}
	l2C64, err := ctx.GetPipelineState("compute_l2_distances_complex64")
	if err != nil {
		return nil, err
	}
	cosineC64, err := ctx.GetPipelineState("compute_cosine_similarity_complex64")
	if err != nil {
		return nil, err
	}
	tq, err := ctx.GetPipelineState("compute_tq_distances")
	if err != nil {
		return nil, err
	}
	haversine, err := ctx.GetPipelineState("haversine_batch")
	if err != nil {
		return nil, err
	}
	norm, err := ctx.GetPipelineState("norm_batch_f32")
	if err != nil {
		return nil, err
	}
	prune, err := ctx.GetPipelineState("hnsw_prune_neighbors")
	if err != nil {
		return nil, err
	}
	greedy, err := ctx.GetPipelineState("hnsw_greedy_search")
	if err != nil {
		return nil, err
	}
	greedyTQ, err := ctx.GetPipelineState("hnsw_greedy_search_tq")
	if err != nil {
		return nil, err
	}

	C.metal_set_pipelines_optimized(
		handle,
		ctx.GetDevice(), ctx.GetCommandQueue(),
		l2, cosine, dot, topK,
		l2Fp16, cosineFp16, dotFp16,
		l2C128, cosineC128, l2C64, cosineC64,
		tq, haversine, norm, prune, greedy, greedyTQ,
	)

	maxVRAM := cfg.MaxMemory
	if maxVRAM <= 0 {
		maxVRAM = 1024 * 1024 * 1024 // 1GB default for Metal
	}
	pageSize := int64(vectorsPerPage) * int64(cfg.Dimension) * 4

	idx := &MetalIndexOptimized{
		handle:       handle,
		dim:          cfg.Dimension,
		idList:       make([]int64, 0),
		lastSyncTime: time.Now(),
		stopSync:     make(chan struct{}),
		maxMemory:    maxVRAM,
	}

	pool, err := memory.NewGPUMemPool(types.BackendMetal, cfg.DeviceID)
	if err == nil {
		idx.memPool = pool
		idx.pager = memory.NewGPUPager(pool, maxVRAM, pageSize)
	}

	idx.startSyncTicker(cfg)

	runtime.SetFinalizer(idx, (*MetalIndexOptimized).Close)
	return idx, nil
}"""

content = re.sub(r'func NewMetalIndexOptimized\(cfg types\.GPUConfig\) \(types\.Index, error\) \{.*?\n\}\n', new_new_method + '\n\n', content, flags=re.DOTALL)

# 2. Update Add
new_add = """func (idx *MetalIndexOptimized) pageIDFor(dataType int, chunkIdx int) int64 {
	return int64(dataType)<<32 | int64(chunkIdx)
}

func (idx *MetalIndexOptimized) startSyncTicker(cfg types.GPUConfig) {
	interval := cfg.SyncInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	idx.syncTicker = time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-idx.syncTicker.C:
				_ = idx.Flush()
			case <-idx.stopSync:
				return
			}
		}
	}()
}

func (idx *MetalIndexOptimized) Flush() error {
	idx.batchMu.Lock()
	defer idx.batchMu.Unlock()

	if len(idx.batchIDs) == 0 {
		return nil
	}

	start := time.Now()
	batchCount := len(idx.batchIDs)

	if batchCount > 2147483647 {
		return fmt.Errorf("batch too large")
	}

	if idx.pager == nil {
		return fmt.Errorf("GPU pager not initialized")
	}

	dim := idx.dim
	maxMem := idx.maxMemory
	prevCount := idx.vectorCount
	newCount := prevCount + batchCount

	totalPages := (newCount + vectorsPerPage - 1) / vectorsPerPage
	estimatedMem := int64(totalPages) * int64(vectorsPerPage) * int64(dim) * 4
	if maxMem > 0 && estimatedMem > maxMem {
		return &types.GPUSyncError{
			BatchSize: batchCount,
			DeviceID:  0,
			Cause:     fmt.Errorf("GPU memory limit exceeded: estimated %d bytes, limit %d", estimatedMem, maxMem),
		}
	}

	vecSize := dim * 4
	pageVecs := vectorsPerPage

	for i := 0; i < batchCount; {
		globalPos := prevCount + i
		chunk := globalPos / pageVecs
		offset := globalPos % pageVecs
		space := pageVecs - offset
		toCopy := batchCount - i
		if toCopy > space {
			toCopy = space
		}

		pid := idx.pageIDFor(0, chunk)

		pi := idx.pager.PageInfo(pid)
		if pi == nil {
			var err error
			pi, err = idx.pager.Alloc(pid)
			if err != nil {
				return &types.GPUSyncError{
					BatchSize: batchCount,
					DeviceID:  0,
					Cause:     fmt.Errorf("failed to allocate pager page %d: %w", pid, err),
				}
			}
		}

		cpuBuf := idx.pager.GetCPUBuf(pi)
		srcVec := idx.batchVectors[i*int(dim) : (i+toCopy)*int(dim)]
		dstOffset := offset * vecSize
		copy(cpuBuf[dstOffset:dstOffset+toCopy*vecSize], unsafe.Slice((*byte)(unsafe.Pointer(&srcVec[0])), toCopy*vecSize))

		if err := idx.pager.Promote(pi); err != nil {
			return &types.GPUSyncError{
				BatchSize: batchCount,
				DeviceID:  0,
				Cause:     fmt.Errorf("failed to promote page %d to GPU: %w", pid, err),
			}
		}

		i += toCopy
	}

	idx.vectorCount = newCount
	idx.idList = append(idx.idList, idx.batchIDs...)

	duration := time.Since(start)
	metrics.RecordGPUSync(duration, batchCount)

	idx.batchIDs = idx.batchIDs[:0]
	idx.batchVectors = idx.batchVectors[:0]
	idx.lastSyncTime = time.Now()

	return nil
}

func (idx *MetalIndexOptimized) Add(ids []int64, vectors []float32) error {
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

	idx.batchMu.Lock()
	idx.batchIDs = append(idx.batchIDs, ids...)
	idx.batchVectors = append(idx.batchVectors, vectors...)
	batchSize := len(idx.batchIDs)
	idx.batchMu.Unlock()

	if batchSize >= 1000 {
		return idx.Flush()
	}

	return nil
}"""

content = re.sub(r'// Add adds vectors to the optimized Metal GPU index\nfunc \(idx \*MetalIndexOptimized\) Add\(ids \[\]int64, vectors \[\]float32\) error \{.*?\n\}\n', new_add + '\n\n', content, flags=re.DOTALL)

with open('internal/gpu/metal/metal_gpu_optimized.go', 'w') as f:
    f.write(content)
