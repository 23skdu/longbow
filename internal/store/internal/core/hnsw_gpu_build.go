package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/gpu"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/rs/zerolog"
)

type GPUBatchBuildConfig struct {
	BatchSize       int
	ParallelSearch  int
	EnablePipeline  bool
	SyncInterval    time.Duration
	MaxGPUQueueSize int
}

func DefaultGPUBatchBuildConfig() GPUBatchBuildConfig {
	return GPUBatchBuildConfig{
		BatchSize:       1000,
		ParallelSearch:  4,
		EnablePipeline:  true,
		SyncInterval:    100 * time.Millisecond,
		MaxGPUQueueSize: 4,
	}
}

type GPUBatchBuilder struct {
	index     *ArrowHNSW
	config    GPUBatchBuildConfig
	gpuIndex  gpu.Index
	gpuMu     sync.RWMutex
	isRunning bool
	stopCh    chan struct{}

	buildQueue chan buildTask
	resultCh   chan buildResult
}

type buildTask struct {
	vectors [][]float32
	ids     []uint32
	level   int
}

type buildResult struct {
	distances []float32
	ids       []uint32
	err       error
}

func NewGPUBatchBuilder(index *ArrowHNSW, config GPUBatchBuildConfig, logger zerolog.Logger) (*GPUBatchBuilder, error) {
	if index == nil {
		return nil, fmt.Errorf("index cannot be nil")
	}

	builder := &GPUBatchBuilder{
		index:      index,
		config:     config,
		buildQueue: make(chan buildTask, config.MaxGPUQueueSize),
		resultCh:   make(chan buildResult, config.MaxGPUQueueSize),
		stopCh:     make(chan struct{}),
	}

	if index.gpuEnabled && index.gpuIndex != nil {
		builder.gpuIndex = index.gpuIndex
		if logger.GetLevel() != zerolog.Disabled {
			logger.Info().
				Int("batch_size", config.BatchSize).
				Int("parallel_search", config.ParallelSearch).
				Msg("GPU batch builder initialized")
		}
	} else {
		if logger.GetLevel() != zerolog.Disabled {
			logger.Warn().
				Msg("GPU not available, using CPU fallback")
		}
	}

	return builder, nil
}

func (b *GPUBatchBuilder) StartPipeline(ctx context.Context) error {
	b.gpuMu.Lock()
	defer b.gpuMu.Unlock()

	if b.isRunning {
		return fmt.Errorf("pipeline already running")
	}

	b.isRunning = true

	if b.config.EnablePipeline && b.gpuIndex != nil {
		go b.runGPUPipeline(ctx)
	}

	return nil
}

func (b *GPUBatchBuilder) StopPipeline() {
	b.gpuMu.Lock()
	defer b.gpuMu.Unlock()

	if !b.isRunning {
		return
	}

	close(b.stopCh)
	b.isRunning = false
}

func (b *GPUBatchBuilder) runGPUPipeline(ctx context.Context) {
	ticker := time.NewTicker(b.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-b.stopCh:
			return
		case task := <-b.buildQueue:
			b.processTask(task)
		case <-ticker.C:
		}
	}
}

func (b *GPUBatchBuilder) processTask(task buildTask) {
	if b.gpuIndex == nil {
		b.resultCh <- buildResult{err: fmt.Errorf("GPU index not available")}
		return
	}

	start := time.Now()

	nVectors := len(task.vectors)
	if nVectors == 0 {
		b.resultCh <- buildResult{ids: task.ids}
		return
	}

	dims := len(task.vectors[0])
	flatVectors := make([]float32, nVectors*dims)
	for i, vec := range task.vectors {
		copy(flatVectors[i*dims:(i+1)*dims], vec)
	}

	var distances []float32
	var searchErr error

	for i := 0; i < nVectors; i++ {
		query := flatVectors[i*dims : (i+1)*dims]
		_, dists, err := b.gpuIndex.Search(query, 10)
		if err != nil {
			searchErr = err
			break
		}
		distances = append(distances, dists...)
	}

	duration := time.Since(start).Seconds()
	metrics.GPUSearchDurationSeconds.WithLabelValues("batch_build").Observe(duration)
	metrics.GPUOperationsTotal.WithLabelValues("batch_build", "vectors").Add(float64(nVectors))

	b.resultCh <- buildResult{
		distances: distances,
		ids:       task.ids,
		err:       searchErr,
	}
}

func (h *ArrowHNSW) BatchInsertWithGPU(ctx context.Context, ids []uint32, vectors [][]float32, level int) error {
	start := time.Now()

	if !h.gpuEnabled || h.gpuIndex == nil {
		return h.batchInsertCPU(ids, vectors, level)
	}

	nVectors := len(vectors)
	if nVectors == 0 {
		return nil
	}

	dims := len(vectors[0])
	if dims == 0 {
		return fmt.Errorf("empty vectors")
	}

	defer func() {
		duration := time.Since(start).Seconds()
		metrics.GPUSearchDurationSeconds.WithLabelValues("gpu_batch_insert").Observe(duration)
		metrics.GPUOperationsTotal.WithLabelValues("batch_insert", "total").Inc()
	}()

	flatVectors := make([]float32, nVectors*dims)
	for i, vec := range vectors {
		if len(vec) != dims {
			return fmt.Errorf("vector %d has %d dims, expected %d", i, len(vec), dims)
		}
		copy(flatVectors[i*dims:(i+1)*dims], vec)
	}

	candidateCount := h.config.M * 5
	if candidateCount > h.Len() {
		candidateCount = h.Len()
	}

	var allCandidates [][]types.Candidate
	for i := 0; i < nVectors; i++ {
		query := flatVectors[i*dims : (i+1)*dims]
		gpuIDs, distances, err := h.gpuIndex.Search(query, candidateCount)
		if err != nil {
			metrics.GPUFallbackTotal.WithLabelValues("batch_insert_gpu_error").Inc()
			return h.batchInsertCPU(ids, vectors, level)
		}

		candidates := make([]types.Candidate, len(gpuIDs))
		for j := range gpuIDs {
			candidates[j] = types.Candidate{
				ID:   uint32(gpuIDs[j]),
				Dist: distances[j],
			}
		}
		allCandidates = append(allCandidates, candidates)
	}

	for i, vec := range vectors {
		id := ids[i]
		candidates := allCandidates[i]

		if err := h.insertWithGPUCandidates(id, vec, level, candidates); err != nil {
			return fmt.Errorf("failed to insert vector %d: %w", id, err)
		}
	}

	return nil
}

func (h *ArrowHNSW) insertWithGPUCandidates(id uint32, vec any, level int, gpuCandidates []types.Candidate) error {
	data := h.data.Load()
	if data == nil {
		return fmt.Errorf("index data not initialized")
	}

	dims := int(h.dims.Load())
	if dims == 0 {
		return fmt.Errorf("index dimensions not set")
	}

	if err := data.SetVector(id, vec); err != nil {
		return err
	}

	cID := types.ChunkID(id)
	cOff := types.ChunkOffset(id)
	levelsChunk := data.GetLevelsChunk(cID)
	if levelsChunk != nil {
		levelsChunk[cOff] = uint8(level)
	}

	var bestCandidate types.Candidate
	bestFound := false
	for _, c := range gpuCandidates {
		loc, ok := h.GetLocation(c.ID)
		if !ok {
			continue
		}
		locTyped, _ := loc.(types.Location)
		if locTyped.BatchIdx == -1 {
			continue
		}

		if !bestFound || c.Dist < bestCandidate.Dist {
			bestCandidate = c
			bestFound = true
		}
	}

	ep := h.entryPoint.Load()
	if bestFound {
		epLoc, ok := h.GetLocation(bestCandidate.ID)
		if ok {
			if epLocTyped, ok := epLoc.(types.Location); ok && epLocTyped.BatchIdx != -1 {
				ep = bestCandidate.ID
			}
		}
	}

	maxL := int(h.maxLevel.Load())

	for l := maxL; l > level; l-- {
		neighbors, err := h.searchLayerForInsert(context.Background(), nil, vec, ep, 1, l, data)
		if err != nil {
			return err
		}
		if len(neighbors) > 0 {
			ep = neighbors[0].ID
		}
	}

	if maxL < 0 {
		h.maxLevel.Store(int32(level))
		h.entryPoint.Store(id)
	} else {
		for l := min(level, maxL); l >= 0; l-- {
			ef := int(h.efConstruction.Load())
			neighbors, err := h.searchLayerForInsert(context.Background(), nil, vec, ep, ef, l, data)
			if err != nil {
				return err
			}

			m := h.m
			if l == 0 {
				m = h.mMax0
			}
			selected := selectNeighborsSimple(neighbors, m)

			searchCtx := h.searchPool.Get()
			maxConn := h.mMax
			if l == 0 {
				maxConn = h.mMax0
			}

			for _, n := range selected {
				data = h.AddConnection(searchCtx, data, id, n.ID, l, maxConn, n.Dist)
				data = h.AddConnection(searchCtx, data, n.ID, id, l, maxConn, n.Dist)
			}

			h.searchPool.Put(searchCtx)

			if len(selected) > 0 {
				ep = selected[0].ID
			}
		}
	}

	h.nodeCount.Add(1)
	return nil
}

func (h *ArrowHNSW) batchInsertCPU(ids []uint32, vectors [][]float32, level int) error {
	for i, vec := range vectors {
		id := ids[i]
		if err := h.InsertWithVector(id, vec, level); err != nil {
			return fmt.Errorf("failed to insert vector %d: %w", id, err)
		}
	}
	return nil
}

func selectNeighborsSimple(candidates []types.Candidate, m int) []types.Candidate {
	if len(candidates) <= m {
		return candidates
	}

	for i := 0; i < len(candidates)-1; i++ {
		for j := i + 1; j < len(candidates); j++ {
			if candidates[j].Dist < candidates[i].Dist {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			}
		}
	}

	return candidates[:m]
}

func (h *ArrowHNSW) BuildIndexWithGPU(ctx context.Context, vectors [][]float32, ids []uint32, config GPUBatchBuildConfig, logger zerolog.Logger) error {
	start := time.Now()

	builder, err := NewGPUBatchBuilder(h, config, logger)
	if err != nil {
		return fmt.Errorf("failed to create GPU batch builder: %w", err)
	}

	if err := builder.StartPipeline(ctx); err != nil {
		return fmt.Errorf("failed to start GPU pipeline: %w", err)
	}
	defer builder.StopPipeline()

	nVectors := len(vectors)
	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	for i := 0; i < nVectors; i += batchSize {
		end := i + batchSize
		if end > nVectors {
			end = nVectors
		}

		batchVectors := vectors[i:end]
		batchIDs := ids[i:end]

		builder.buildQueue <- buildTask{
			vectors: batchVectors,
			ids:     batchIDs,
			level:   -1,
		}

		select {
		case result := <-builder.resultCh:
			if result.err != nil {
				logger.Warn().Err(result.err).Int("batch_start", i).Msg("GPU batch failed, falling back to CPU")
				if err := h.batchInsertCPU(batchIDs, batchVectors, -1); err != nil {
					return err
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}

		if logger.GetLevel() != zerolog.Disabled && (i%10000 == 0 || end == nVectors) {
			logger.Info().
				Int("processed", end).
				Int("total", nVectors).
				Float64("progress", float64(end)/float64(nVectors)*100).
				Msg("GPU batch build progress")
		}
	}

	duration := time.Since(start).Seconds()
	metrics.GPUSearchDurationSeconds.WithLabelValues("full_build").Observe(duration)
	metrics.GPUOperationsTotal.WithLabelValues("full_build", "vectors").Add(float64(nVectors))

	if logger.GetLevel() != zerolog.Disabled {
		logger.Info().
			Int("vectors", nVectors).
			Dur("duration", time.Since(start)).
			Float64("vectors_per_sec", float64(nVectors)/duration).
			Msg("GPU batch build completed")
	}

	return nil
}
