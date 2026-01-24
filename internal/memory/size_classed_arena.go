package memory

import (
	"fmt"
	"sort"
	"sync"
	"unsafe"
)

type SizeClass struct {
	MinSize   int
	MaxSize   int
	SlabSize  int
	AllocName string
}

var SizeClasses = []SizeClass{
	{MinSize: 0, MaxSize: 64, SlabSize: 1 << 20, AllocName: "tiny"},
	{MinSize: 65, MaxSize: 256, SlabSize: 2 << 20, AllocName: "small"},
	{MinSize: 257, MaxSize: 1024, SlabSize: 4 << 20, AllocName: "medium"},
	{MinSize: 1025, MaxSize: 4096, SlabSize: 8 << 20, AllocName: "large"},
	{MinSize: 4097, MaxSize: 16384, SlabSize: 16 << 20, AllocName: "xlarge"},
	{MinSize: 16385, MaxSize: 65536, SlabSize: 32 << 20, AllocName: "huge"},
}

type SizeClassArena[T any] struct {
	mu       sync.RWMutex
	arenas   map[string]*TypedArena[T]
	classMap map[int]string
}

func NewSizeClassArena[T any]() *SizeClassArena[T] {
	sca := &SizeClassArena[T]{
		arenas:   make(map[string]*TypedArena[T]),
		classMap: make(map[int]string),
	}

	for _, class := range SizeClasses {
		arena := NewTypedArena[T](NewSlabArena(class.SlabSize))
		sca.arenas[class.AllocName] = arena
	}

	sca.buildSizeMap()
	return sca
}

func (sca *SizeClassArena[T]) buildSizeMap() {
	sca.classMap = make(map[int]string)
	for size := 0; size <= 65536; size++ {
		for _, class := range SizeClasses {
			if size >= class.MinSize && size <= class.MaxSize {
				sca.classMap[size] = class.AllocName
				break
			}
		}
	}
}

func (sca *SizeClassArena[T]) getArenaForSize(size int) *TypedArena[T] {
	if className, ok := sca.classMap[size]; ok {
		return sca.arenas[className]
	}

	return sca.arenas["huge"]
}

func (sca *SizeClassArena[T]) AllocSlice(count int) (SliceRef, string, error) {
	if count <= 0 {
		return SliceRef{}, "", fmt.Errorf("count must be positive")
	}

	var zero T
	elemSize := int(unsafe.Sizeof(zero))
	totalSize := count * elemSize

	sca.mu.RLock()
	arena := sca.getArenaForSize(totalSize)
	className := sca.classMap[totalSize]
	if className == "" {
		className = "huge"
	}
	sca.mu.RUnlock()

	ref, err := arena.AllocSlice(count)
	return ref, className, err
}

func (sca *SizeClassArena[T]) AllocSliceDirty(count int) (SliceRef, string, error) {
	if count <= 0 {
		return SliceRef{}, "", fmt.Errorf("count must be positive")
	}

	var zero T
	elemSize := int(unsafe.Sizeof(zero))
	totalSize := count * elemSize

	sca.mu.RLock()
	arena := sca.getArenaForSize(totalSize)
	className := sca.classMap[totalSize]
	if className == "" {
		className = "huge"
	}
	sca.mu.RUnlock()

	ref, err := arena.AllocSliceDirty(count)
	return ref, className, err
}

func (sca *SizeClassArena[T]) Get(ref SliceRef) []T {
	sca.mu.RLock()
	defer sca.mu.RUnlock()

	for _, arena := range sca.arenas {
		data := arena.Get(ref)
		if data != nil {
			return data
		}
	}
	return nil
}

func (sca *SizeClassArena[T]) Stats() map[string]ArenaStats {
	sca.mu.RLock()
	defer sca.mu.RUnlock()

	stats := make(map[string]ArenaStats)
	for name, arena := range sca.arenas {
		stats[name] = arena.arena.Stats()
	}
	return stats
}

func (sca *SizeClassArena[T]) Compact(liveRefs map[string][]SliceRef) (map[string]*CompactionStats, error) {
	sca.mu.Lock()
	defer sca.mu.Unlock()

	compactStats := make(map[string]*CompactionStats)

	for className, refs := range liveRefs {
		arena, ok := sca.arenas[className]
		if !ok {
			return nil, fmt.Errorf("unknown size class: %s", className)
		}

		stats, err := arena.Compact(refs)
		if err != nil {
			return nil, fmt.Errorf("compaction failed for class %s: %w", className, err)
		}
		compactStats[className] = stats
	}

	return compactStats, nil
}

func (sca *SizeClassArena[T]) MemoryUsage() MemoryUsageSummary {
	sca.mu.RLock()
	defer sca.mu.RUnlock()

	summary := MemoryUsageSummary{
		ByClass: make(map[string]MemoryClassUsage),
	}

	for className, arena := range sca.arenas {
		stats := arena.arena.Stats()
		classUsage := MemoryClassUsage{
			TotalCapacity: stats.TotalCapacity,
			UsedBytes:     stats.UsedBytes,
			Utilization:   float64(stats.UsedBytes) / float64(stats.TotalCapacity) * 100,
		}

		summary.ByClass[className] = classUsage
		summary.TotalCapacity += classUsage.TotalCapacity
		summary.TotalUsed += classUsage.UsedBytes
	}

	if summary.TotalCapacity > 0 {
		summary.OverallUtilization = float64(summary.TotalUsed) / float64(summary.TotalCapacity) * 100
	}

	sortedClasses := make([]string, 0, len(summary.ByClass))
	for className := range summary.ByClass {
		sortedClasses = append(sortedClasses, className)
	}
	sort.Slice(sortedClasses, func(i, j int) bool {
		return summary.ByClass[sortedClasses[i]].UsedBytes > summary.ByClass[sortedClasses[j]].UsedBytes
	})
	summary.SortedClasses = sortedClasses

	return summary
}

type MemoryUsageSummary struct {
	TotalCapacity      int64
	TotalUsed          int64
	OverallUtilization float64
	ByClass            map[string]MemoryClassUsage
	SortedClasses      []string
}

type MemoryClassUsage struct {
	TotalCapacity int64
	UsedBytes     int64
	Utilization   float64
}

func AllocateOptimized[T any](sca *SizeClassArena[T], data []T) (SliceRef, string, error) {
	return sca.AllocSlice(len(data))
}

func CopyBetweenArenas[T any](srcArena, dstArena *TypedArena[T], srcRef SliceRef) (SliceRef, error) {
	srcData := srcArena.Get(srcRef)
	if srcData == nil {
		return SliceRef{}, fmt.Errorf("source reference is invalid")
	}

	dstRef, err := dstArena.AllocSliceDirty(len(srcData))
	if err != nil {
		return SliceRef{}, err
	}

	dstData := dstArena.Get(dstRef)
	copy(dstData, srcData)

	return dstRef, nil
}
