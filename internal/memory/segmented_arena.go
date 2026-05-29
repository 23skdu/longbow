package memory

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// SegmentTier represents the heat tier of an arena segment.
type SegmentTier int

const (
	TierHot SegmentTier = iota
	TierWarm
	TierCold
)

// SegmentConfig configures each tier in the segmented arena.
type SegmentConfig struct {
	Tier        SegmentTier
	SlabSize    int   // power-of-2 slab size for this tier
	TargetBytes int64 // max total bytes for this tier (0 = unlimited)
	AccessThres int64 // min access count to be promoted to this tier
}

// SegmentedArena provides three-tier vector storage with LRU-aware promotion/demotion.
// Hot tier: small slabs, fast allocation, frequently accessed vectors.
// Warm tier: medium slabs, moderate access frequency.
// Cold tier: large slabs, mmap-backed, infrequently accessed vectors.
type SegmentedArena struct {
	tiers [3]*TierState

	// accessLog stores per-allocation access counters indexed by offset.
	accessLog sync.Map

	stopCh    chan struct{}
	promoteCh chan struct{}

	promotions atomic.Int64
	evictions  atomic.Int64
}

// TierState holds per-tier state.
type TierState struct {
	config SegmentConfig
	arena  *SlabArena

	mu       sync.RWMutex
	used     int64
	capacity int64
}

// AccessInfo tracks access frequency for a single allocation.
type AccessInfo struct {
	count   atomic.Int64
	lastHit atomic.Int64 // unix nanos
}

// NewSegmentedArena creates a three-tier segmented arena.
func NewSegmentedArena(cfgs [3]SegmentConfig) *SegmentedArena {
	sa := &SegmentedArena{
		stopCh:    make(chan struct{}),
		promoteCh: make(chan struct{}, 1),
	}
	for i, cfg := range cfgs {
		sa.tiers[i] = &TierState{
			config: cfg,
			arena:  NewSlabArena(cfg.SlabSize),
		}
	}
	go sa.promotionLoop()
	return sa
}

// allocWithAccess allocates bytes in the specified tier and records access info.
func (sa *SegmentedArena) allocWithAccess(tier SegmentTier, size int) (uint64, error) {
	ts := sa.tiers[tier]
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.capacity > 0 && ts.used+int64(size) > ts.capacity {
		return 0, ErrOutOfMemory
	}

	aligned := (size + 7) & ^7
	offset, err := ts.arena.Alloc(aligned)
	if err != nil {
		return 0, err
	}

	ts.used += int64(aligned)

	ai := &AccessInfo{}
	ai.count.Store(1)
	ai.lastHit.Store(time.Now().UnixNano())
	sa.accessLog.Store(offset, ai)

	return offset, nil
}

// RecordAccess records a vector access for LRU tracking.
func (sa *SegmentedArena) RecordAccess(offset uint64) {
	val, ok := sa.accessLog.Load(offset)
	if !ok {
		return
	}
	ai := val.(*AccessInfo)
	ai.count.Add(1)
	ai.lastHit.Store(time.Now().UnixNano())
}

// GetAccessInfo returns access statistics for a given offset.
func (sa *SegmentedArena) GetAccessInfo(offset uint64) (hits int64, lastAccess time.Time) {
	val, ok := sa.accessLog.Load(offset)
	if !ok {
		return 0, time.Time{}
	}
	ai := val.(*AccessInfo)
	return ai.count.Load(), time.Unix(0, ai.lastHit.Load())
}

// GetTier returns the SlabArena for a given tier.
func (sa *SegmentedArena) GetTier(tier SegmentTier) *SlabArena {
	return sa.tiers[tier].arena
}

// ShouldPromote checks whether a ref's access pattern justifies promotion to the next tier.
func (sa *SegmentedArena) ShouldPromote(offset uint64, currentTier SegmentTier) bool {
	if currentTier >= TierCold {
		return false
	}
	nextTier := currentTier + 1
	threshold := sa.tiers[nextTier].config.AccessThres
	if threshold <= 0 {
		return false
	}
	val, ok := sa.accessLog.Load(offset)
	if !ok {
		return false
	}
	ai := val.(*AccessInfo)
	return ai.count.Load() >= threshold
}

// AllocHot allocates into the hot tier.
func (sa *SegmentedArena) AllocHot(size int) (uint64, error) {
	return sa.allocWithAccess(TierHot, size)
}

// AllocWarm allocates into the warm tier.
func (sa *SegmentedArena) AllocWarm(size int) (uint64, error) {
	return sa.allocWithAccess(TierWarm, size)
}

// AllocCold allocates into the cold tier.
func (sa *SegmentedArena) AllocCold(size int) (uint64, error) {
	return sa.allocWithAccess(TierCold, size)
}

// Stats returns aggregate statistics for all tiers.
func (sa *SegmentedArena) Stats() SegmentedArenaStats {
	var s SegmentedArenaStats
	for i, ts := range sa.tiers {
		slabs := *ts.arena.slabs.Load()
		usedBytes := int64(0)
		for _, slab := range slabs {
			usedBytes += int64(slab.offset)
		}
		capBytes := int64(0)
		for _, slab := range slabs {
			capBytes += int64(len(slab.data))
		}
		switch SegmentTier(i) {
		case TierHot:
			s.HotSlabs = len(slabs)
			s.HotUsed = usedBytes
			s.HotCapacity = capBytes
		case TierWarm:
			s.WarmSlabs = len(slabs)
			s.WarmUsed = usedBytes
			s.WarmCapacity = capBytes
		case TierCold:
			s.ColdSlabs = len(slabs)
			s.ColdUsed = usedBytes
			s.ColdCapacity = capBytes
		}
	}
	s.TotalPromotions = sa.promotions.Load()
	s.TotalEvictions = sa.evictions.Load()
	return s
}

// SegmentedArenaStats exposes metrics for monitoring.
type SegmentedArenaStats struct {
	HotSlabs        int
	HotUsed         int64
	HotCapacity     int64
	WarmSlabs       int
	WarmUsed        int64
	WarmCapacity    int64
	ColdSlabs       int
	ColdUsed        int64
	ColdCapacity    int64
	TotalPromotions int64
	TotalEvictions  int64
}

// promotionLoop periodically scans access logs and promotes hot vectors.
func (sa *SegmentedArena) promotionLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-sa.stopCh:
			return
		case <-ticker.C:
			sa.runPromotionCycle()
		}
	}
}

func (sa *SegmentedArena) runPromotionCycle() {
	const maxPromotionsPerCycle = 1024
	var promoted int

	sa.accessLog.Range(func(key, value any) bool {
		if promoted >= maxPromotionsPerCycle {
			return false
		}
		offset := key.(uint64)
		ai := value.(*AccessInfo)

		hits := ai.count.Load()
		tier := sa.locateTier(offset)
		if tier == -1 {
			return true
		}

		nextTier := tier + 1
		if nextTier > TierCold {
			return true
		}
		threshold := sa.tiers[nextTier].config.AccessThres
		if threshold > 0 && hits >= threshold {
			sz := sa.tiers[tier].arena.Get(offset, 0)
			if sz == nil {
				return true
			}
			size := len(sz)
			if size > math.MaxUint32 {
				return true
			}
			dstOffset, err := sa.allocWithAccess(nextTier, size)
			if err != nil {
				return true
			}
			dst := sa.tiers[nextTier].arena.Get(dstOffset, uint32(size))
			if dst == nil {
				return true
			}
			copy(dst, sz)

			// Transfer access info to new location.
			newAI := &AccessInfo{}
			newAI.count.Store(ai.count.Load())
			newAI.lastHit.Store(ai.lastHit.Load())
			sa.accessLog.Store(dstOffset, newAI)

			promoted++
			sa.promotions.Add(1)
		}
		return true
	})

	_ = promoted
}

// locateTier determines which tier contains a given slab offset.
// offset encodes slabIdx * slabCap + localOffset.
func (sa *SegmentedArena) locateTier(offset uint64) SegmentTier {
	for i, ts := range sa.tiers {
		slabCap := uint64(ts.arena.slabCap)
		slabIdx := offset / slabCap
		slabs := *ts.arena.slabs.Load()
		if slabIdx < uint64(len(slabs)) {
			return SegmentTier(i)
		}
	}
	return -1
}

func (sa *SegmentedArena) Close() {
	close(sa.stopCh)
	for _, ts := range sa.tiers {
		ts.arena.Free()
	}
}

// DefaultSegmentConfigs returns sensible defaults for vector storage.
func DefaultSegmentConfigs() [3]SegmentConfig {
	return [3]SegmentConfig{
		{
			Tier:        TierHot,
			SlabSize:    4 * 1024 * 1024, // 4MB slabs
			TargetBytes: 0,
			AccessThres: 100,
		},
		{
			Tier:        TierWarm,
			SlabSize:    16 * 1024 * 1024, // 16MB slabs
			TargetBytes: 0,
			AccessThres: 10,
		},
		{
			Tier:        TierCold,
			SlabSize:    64 * 1024 * 1024, // 64MB slabs
			TargetBytes: 0,
			AccessThres: 0,
		},
	}
}

// ErrOutOfMemory is returned when no arena tier has capacity.
var ErrOutOfMemory = &SlabAllocError{msg: "segmented arena out of memory"}

// SlabAllocError is returned when arena allocation fails.
type SlabAllocError struct {
	msg string
}

func (e *SlabAllocError) Error() string { return e.msg }
