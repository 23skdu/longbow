package gpu

import (
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/gpu/memory"
	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/rs/zerolog"
)

type MultiGPUConfig struct {
	DeviceIDs       []int
	Strategy        MultiGPUStrategy
	MaxMemoryPerGPU int64
	EnableMigration bool
	BalanceInterval time.Duration
}

type MultiGPUStrategy int

const (
	StrategyRoundRobin MultiGPUStrategy = iota
	StrategyLoadBalance
	StrategyAffinity
	StrategyMemoryAware
)

func (s MultiGPUStrategy) String() string {
	switch s {
	case StrategyRoundRobin:
		return "round_robin"
	case StrategyLoadBalance:
		return "load_balance"
	case StrategyAffinity:
		return "affinity"
	case StrategyMemoryAware:
		return "memory_aware"
	default:
		return "unknown"
	}
}

type GPUDevice struct {
	ID         int
	Index      types.Index
	MemPool    *memory.GPUMemPool
	Info       *types.GPUInfo
	QueryCount atomic.Int64
	ErrorCount atomic.Int64
	LastUsed   atomic.Int64
}

type MultiGPUManager struct {
	config   MultiGPUConfig
	devices  []*GPUDevice
	deviceMu sync.RWMutex
	rrIndex  atomic.Int32
	logger   zerolog.Logger
	closed   atomic.Bool
}

func NewMultiGPUManager(config MultiGPUConfig, logger zerolog.Logger) (*MultiGPUManager, error) {
	if len(config.DeviceIDs) == 0 {
		return nil, fmt.Errorf("no GPU devices specified")
	}

	mgr := &MultiGPUManager{
		config:  config,
		devices: make([]*GPUDevice, 0, len(config.DeviceIDs)),
		logger:  logger,
	}

	for _, id := range config.DeviceIDs {
		device, err := mgr.initializeDevice(id)
		if err != nil {
			logger.Warn().
				Err(err).
				Int("device_id", id).
				Msg("Failed to initialize GPU device")
			continue
		}
		mgr.devices = append(mgr.devices, device)
	}

	if len(mgr.devices) == 0 {
		return nil, fmt.Errorf("no GPU devices could be initialized")
	}

	logger.Info().
		Int("device_count", len(mgr.devices)).
		Str("strategy", config.Strategy.String()).
		Msg("Multi-GPU manager initialized")

	mgr.startMetricsCollector()

	return mgr, nil
}

func (m *MultiGPUManager) startMetricsCollector() {
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for range ticker.C {
			if m.closed.Load() {
				ticker.Stop()
				return
			}
			m.deviceMu.RLock()
			for _, d := range m.devices {
				if d.Index != nil {
					metrics.UpdateDeviceMetrics(d.Index)
				}
			}
			m.deviceMu.RUnlock()
		}
	}()
}

func (m *MultiGPUManager) initializeDevice(deviceID int) (*GPUDevice, error) {
	gpuConfig := types.GPUConfig{
		Backend:  types.BackendCUDA,
		DeviceID: deviceID,
		Enabled:  true,
	}

	idx, err := NewIndexWithBackend(gpuConfig, types.BackendCUDA)
	if err != nil {
		return nil, fmt.Errorf("failed to create GPU index for device %d: %w", deviceID, err)
	}

	memPool, err := memory.NewGPUMemPool(types.BackendCUDA, deviceID)
	if err != nil {
		idx.Close()
		return nil, fmt.Errorf("failed to create memory pool for device %d: %w", deviceID, err)
	}

	info, err := idx.GetDeviceInfo()
	if err != nil {
		memPool.Close()
		idx.Close()
		return nil, fmt.Errorf("failed to get device info for device %d: %w", deviceID, err)
	}

	device := &GPUDevice{
		ID:      deviceID,
		Index:   idx,
		MemPool: memPool,
		Info:    info,
	}

	m.logger.Info().
		Int("device_id", deviceID).
		Str("name", info.Name).
		Int64("memory_mb", info.MemoryMB).
		Msg("GPU device initialized")

	return device, nil
}

func (m *MultiGPUManager) GetDevice(deviceID int) (*GPUDevice, error) {
	m.deviceMu.RLock()
	defer m.deviceMu.RUnlock()

	for _, d := range m.devices {
		if d.ID == deviceID {
			return d, nil
		}
	}
	return nil, fmt.Errorf("device %d not found", deviceID)
}

func (m *MultiGPUManager) SelectDevice() *GPUDevice {
	m.deviceMu.RLock()
	defer m.deviceMu.RUnlock()

	if len(m.devices) == 0 {
		return nil
	}

	switch m.config.Strategy {
	case StrategyLoadBalance:
		return m.selectLoadBalance()
	case StrategyMemoryAware:
		return m.selectMemoryAware()
	default:
		return m.selectRoundRobin()
	}
}

func (m *MultiGPUManager) selectRoundRobin() *GPUDevice {
	idx := m.rrIndex.Add(1) - 1
	return m.devices[int(idx)%len(m.devices)]
}

func (m *MultiGPUManager) selectLoadBalance() *GPUDevice {
	var best *GPUDevice
	var bestScore int64 = -1

	for _, d := range m.devices {
		queries := d.QueryCount.Load()
		errors := d.ErrorCount.Load()
		score := queries - errors*10

		if best == nil || score < bestScore {
			best = d
			bestScore = score
		}
	}

	return best
}

func (m *MultiGPUManager) selectMemoryAware() *GPUDevice {
	var best *GPUDevice
	var bestFree int64 = -1

	for _, d := range m.devices {
		free := d.MemPool.GetAvailableMemory()
		if best == nil || free > bestFree {
			best = d
			bestFree = free
		}
	}

	if best == nil {
		return m.selectRoundRobin()
	}

	return best
}

func (m *MultiGPUManager) AddVectors(ids []int64, vectors []float32) error {
	device := m.SelectDevice()
	if device == nil {
		return fmt.Errorf("no GPU device available")
	}

	device.QueryCount.Add(1)
	device.LastUsed.Store(time.Now().UnixNano())

	err := device.Index.Add(ids, vectors)
	if err != nil {
		device.ErrorCount.Add(1)
		return err
	}

	return nil
}

func (m *MultiGPUManager) Search(query []float32, k int) ([]int64, []float32, error) {
	device := m.SelectDevice()
	if device == nil {
		return nil, nil, fmt.Errorf("no GPU device available")
	}

	device.QueryCount.Add(1)
	device.LastUsed.Store(time.Now().UnixNano())

	ids, distances, err := device.Index.Search(query, k)
	if err != nil {
		device.ErrorCount.Add(1)
		return nil, nil, err
	}

	return ids, distances, nil
}

func (m *MultiGPUManager) SearchPQ(lookupTable []float32, mSub int, k int) ([]int64, []float32, error) {
	device := m.SelectDevice()
	if device == nil {
		return nil, nil, fmt.Errorf("no GPU device available")
	}

	device.QueryCount.Add(1)
	device.LastUsed.Store(time.Now().UnixNano())

	ids, distances, err := device.Index.SearchPQ(lookupTable, mSub, k)
	if err != nil {
		device.ErrorCount.Add(1)
		return nil, nil, err
	}

	return ids, distances, nil
}

func (m *MultiGPUManager) TrainPQ(vectors []float32, mSub, kCentroids int) error {
	device := m.SelectDevice()
	if device == nil {
		return fmt.Errorf("no GPU device available")
	}
	start := time.Now()
	err := device.Index.TrainPQ(vectors, mSub, kCentroids)
	duration := time.Since(start)

	status := "success"
	if err != nil {
		status = "error"
	}

	metrics.PQTrainingDuration.WithLabelValues("multi_gpu", fmt.Sprintf("%d", len(vectors)/mSub), fmt.Sprintf("%d", mSub)).Observe(duration.Seconds())
	metrics.PQOperationsTotal.WithLabelValues("multi_gpu", "train", status).Inc()

	return err
}

func (m *MultiGPUManager) EncodePQ(vectors []float32) ([]byte, error) {
	device := m.SelectDevice()
	if device == nil {
		return nil, fmt.Errorf("no GPU device available")
	}
	start := time.Now()
	codes, err := device.Index.EncodePQ(vectors)
	duration := time.Since(start)

	status := "success"
	if err != nil {
		status = "error"
	}

	metrics.PQEncodingDuration.WithLabelValues("multi_gpu", fmt.Sprintf("%d", len(vectors)/int(device.Info.MemoryMB))).Observe(duration.Seconds())
	metrics.PQOperationsTotal.WithLabelValues("multi_gpu", "encode", status).Inc()

	return codes, err
}

func (m *MultiGPUManager) SearchMerged(query []float32, k int) ([]int64, []float32, error) {
	allIDs, allDistances, allErrors := m.SearchAllDevices(query, k)

	// Check for errors
	for _, err := range allErrors {
		if err != nil {
			return nil, nil, err
		}
	}

	return m.mergeResults(allIDs, allDistances, k)
}

func (m *MultiGPUManager) SearchMergedPQ(lookupTable []float32, mSub int, k int) ([]int64, []float32, error) {
	m.deviceMu.RLock()
	defer m.deviceMu.RUnlock()

	allIDs := make([][]int64, len(m.devices))
	allDistances := make([][]float32, len(m.devices))
	allErrors := make([]error, len(m.devices))

	var wg sync.WaitGroup
	for i, device := range m.devices {
		wg.Add(1)
		go func(idx int, d *GPUDevice) {
			defer wg.Done()
			d.QueryCount.Add(1)
			d.LastUsed.Store(time.Now().UnixNano())
			ids, distances, err := d.Index.SearchPQ(lookupTable, mSub, k)
			if err != nil {
				d.ErrorCount.Add(1)
				allErrors[idx] = err
				return
			}
			allIDs[idx] = ids
			allDistances[idx] = distances
		}(i, device)
	}
	wg.Wait()

	for _, err := range allErrors {
		if err != nil {
			return nil, nil, err
		}
	}

	return m.mergeResults(allIDs, allDistances, k)
}

func (m *MultiGPUManager) SearchAllDevices(query []float32, k int) ([][]int64, [][]float32, []error) {
	m.deviceMu.RLock()
	defer m.deviceMu.RUnlock()

	allIDs := make([][]int64, len(m.devices))
	allDistances := make([][]float32, len(m.devices))
	allErrors := make([]error, len(m.devices))

	var wg sync.WaitGroup
	for i, device := range m.devices {
		wg.Add(1)
		go func(idx int, d *GPUDevice) {
			defer wg.Done()
			d.QueryCount.Add(1)
			d.LastUsed.Store(time.Now().UnixNano())
			ids, distances, err := d.Index.Search(query, k)
			if err != nil {
				d.ErrorCount.Add(1)
				allErrors[idx] = err
				return
			}
			allIDs[idx] = ids
			allDistances[idx] = distances
		}(i, device)
	}
	wg.Wait()

	return allIDs, allDistances, allErrors
}

func (m *MultiGPUManager) mergeResults(allIDs [][]int64, allDistances [][]float32, k int) ([]int64, []float32, error) {
	numDevices := len(allIDs)
	if numDevices == 0 {
		return nil, nil, nil
	}
	if numDevices == 1 {
		resIDs := allIDs[0]
		resDistances := allDistances[0]
		if len(resIDs) > k {
			resIDs = resIDs[:k]
			resDistances = resDistances[:k]
		}
		return resIDs, resDistances, nil
	}

	// Classic merge of sorted lists
	indices := make([]int, numDevices)
	mergedIDs := make([]int64, 0, k)
	mergedDistances := make([]float32, 0, k)

	for len(mergedIDs) < k {
		bestDevice := -1
		var minDistance float32 = 1e38

		for i := 0; i < numDevices; i++ {
			if indices[i] < len(allDistances[i]) {
				dist := allDistances[i][indices[i]]
				if bestDevice == -1 || dist < minDistance {
					minDistance = dist
					bestDevice = i
				}
			}
		}

		if bestDevice == -1 {
			break
		}

		mergedIDs = append(mergedIDs, allIDs[bestDevice][indices[bestDevice]])
		mergedDistances = append(mergedDistances, allDistances[bestDevice][indices[bestDevice]])
		indices[bestDevice]++
	}

	return mergedIDs, mergedDistances, nil
}

func (m *MultiGPUManager) GetStats() MultiGPUStats {
	m.deviceMu.RLock()
	defer m.deviceMu.RUnlock()

	stats := MultiGPUStats{
		DeviceCount: len(m.devices),
		Strategy:    m.config.Strategy.String(),
	}

	for _, d := range m.devices {
		total := d.MemPool.GetTotalMemory()
		used := d.MemPool.GetUsedMemory()

		deviceStats := DeviceStats{
			ID:          d.ID,
			Name:        d.Info.Name,
			TotalMemory: total,
			UsedMemory:  used,
			QueryCount:  d.QueryCount.Load(),
			ErrorCount:  d.ErrorCount.Load(),
			LastUsed:    time.Unix(0, d.LastUsed.Load()),
		}

		stats.Devices = append(stats.Devices, deviceStats)
		stats.TotalMemory += total
		stats.TotalQueries += deviceStats.QueryCount
	}

	return stats
}

type MultiGPUStats struct {
	DeviceCount  int
	Strategy     string
	TotalMemory  int64
	TotalQueries int64
	Devices      []DeviceStats
}

type DeviceStats struct {
	ID          int
	Name        string
	TotalMemory int64
	UsedMemory  int64
	QueryCount  int64
	ErrorCount  int64
	LastUsed    time.Time
}

func (m *MultiGPUManager) Close() error {
	if m.closed.Swap(true) {
		return nil
	}

	m.deviceMu.Lock()
	defer m.deviceMu.Unlock()

	var lastErr error
	for _, d := range m.devices {
		if d.Index != nil {
			if err := d.Index.Close(); err != nil {
				lastErr = err
			}
		}
		if d.MemPool != nil {
			if err := d.MemPool.Close(); err != nil {
				lastErr = err
			}
		}
	}

	m.devices = nil
	return lastErr
}

func (m *MultiGPUManager) DeviceCount() int {
	m.deviceMu.RLock()
	defer m.deviceMu.RUnlock()
	return len(m.devices)
}

func (m *MultiGPUManager) ReplicateToAll(ids []int64, vectors []float32) error {
	m.deviceMu.RLock()
	defer m.deviceMu.RUnlock()

	var wg sync.WaitGroup
	errCh := make(chan error, len(m.devices))

	for _, device := range m.devices {
		wg.Add(1)
		go func(d *GPUDevice) {
			defer wg.Done()
			if err := d.Index.Add(ids, vectors); err != nil {
				errCh <- fmt.Errorf("device %d: %w", d.ID, err)
			}
		}(device)
	}

	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("replication failed on %d devices: %v", len(errs), errs[0])
	}

	return nil
}

func DefaultMultiGPUConfig() MultiGPUConfig {
	return MultiGPUConfig{
		DeviceIDs:       []int{0},
		Strategy:        StrategyRoundRobin,
		MaxMemoryPerGPU: 0,
		EnableMigration: false,
		BalanceInterval: 30 * time.Second,
	}
}

func DetectAvailableDevices() ([]int, error) {
	count := GetDeviceCount()
	if count == 0 {
		return nil, fmt.Errorf("no GPU devices detected")
	}

	devices := make([]int, count)
	for i := 0; i < count; i++ {
		devices[i] = i
	}

	return devices, nil
}

func (m *MultiGPUManager) shardVectorID(id int64) int {
	if len(m.devices) == 1 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte{byte(id), byte(id >> 8), byte(id >> 16), byte(id >> 24), byte(id >> 32), byte(id >> 40), byte(id >> 48), byte(id >> 56)})
	return int(h.Sum64() % uint64(len(m.devices)))
}

func (m *MultiGPUManager) ShardDevice(vectorID int64) *GPUDevice {
	m.deviceMu.RLock()
	defer m.deviceMu.RUnlock()

	if len(m.devices) == 0 {
		return nil
	}

	shardIdx := m.shardVectorID(vectorID)
	return m.devices[shardIdx]
}

func (m *MultiGPUManager) AddVectorsSharded(ids []int64, vectors []float32) error {
	if len(ids) == 0 {
		return nil
	}

	dimension := len(vectors) / len(ids)
	if len(ids)*dimension != len(vectors) {
		return fmt.Errorf("ids and vectors length mismatch: %d ids, %d floats (dim=%d)", len(ids), len(vectors), dimension)
	}

	m.deviceMu.RLock()
	defer m.deviceMu.RUnlock()

	batches := make(map[int][]int64)

	for _, id := range ids {
		shardIdx := m.shardVectorID(id)
		batches[shardIdx] = append(batches[shardIdx], id)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(batches))

	for shardIdx, batchIDs := range batches {
		wg.Add(1)
		go func(idx int, batchIDs []int64) {
			defer wg.Done()
			device := m.devices[idx]
			vecBatch := make([]float32, len(batchIDs)*dimension)
			for i, id := range batchIDs {
				srcStart := 0
				for j, sid := range ids {
					if sid == id {
						srcStart = j * dimension
						break
					}
				}
				copy(vecBatch[i*dimension:], vectors[srcStart:srcStart+dimension])
			}
			if err := device.Index.Add(batchIDs, vecBatch); err != nil {
				errCh <- fmt.Errorf("device %d: %w", idx, err)
			}
		}(shardIdx, batchIDs)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *MultiGPUManager) GetDeviceByVectorID(id int64) (*GPUDevice, error) {
	device := m.ShardDevice(id)
	if device == nil {
		return nil, fmt.Errorf("no device found for vector ID %d", id)
	}
	return device, nil
}
