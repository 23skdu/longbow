package store

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVersionHistory_New(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	cfg := DefaultVersionHistoryConfig()
	vh := NewVersionHistory(cfg)

	assert.NotNil(t, vh)
	assert.Equal(t, 10, vh.maxVersions)
}

func TestVersionHistory_Add(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	vh := NewVersionHistory(DefaultVersionHistoryConfig())

	vector := []float32{1.0, 2.0, 3.0}
	now := time.Now().UnixNano()
	vh.Add(1, vector, 0.0, now, nil)

	history := vh.GetHistory(1)
	assert.Len(t, history, 1)
	assert.Equal(t, 1, history[0].Version)
}

func TestVersionHistory_GetLatestVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	vh := NewVersionHistory(DefaultVersionHistoryConfig())

	now := time.Now().UnixNano()
	vh.Add(1, []float32{1.0}, 0.0, now, nil)
	vh.Add(1, []float32{2.0}, 0.0, now+1000, nil)

	latest, err := vh.GetLatestVersion(1)
	assert.NoError(t, err)
	assert.Equal(t, 2, latest.Version)
}

func TestVersionHistory_GetVersionAt(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	vh := NewVersionHistory(DefaultVersionHistoryConfig())

	now := time.Now().UnixNano()
	vh.Add(1, []float32{1.0}, 0.0, now-1000, nil)
	vh.Add(1, []float32{2.0}, 0.0, now, nil)
	vh.Add(1, []float32{3.0}, 0.0, now+1000, nil)

	version, err := vh.GetVersionAt(1, now)
	assert.NoError(t, err)
	assert.Equal(t, []float32{2.0}, version.Vector)
}

func TestVersionHistory_Prune(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	vh := NewVersionHistory(DefaultVersionHistoryConfig())

	now := time.Now().UnixNano()
	vh.Add(1, []float32{1.0}, 0.0, now-2000, nil)
	vh.Add(1, []float32{2.0}, 0.0, now-1000, nil)
	vh.Add(1, []float32{3.0}, 0.0, now, nil)

	pruned := vh.Prune(context.Background(), now-500)

	assert.Equal(t, 2, pruned)
	history := vh.GetHistory(1)
	assert.Len(t, history, 1)
}

func TestVersionHistory_MaxVersions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	cfg := VersionHistoryConfig{
		MaxVersions:     3,
		RetentionPeriod: 7 * 24 * time.Hour,
	}
	vh := NewVersionHistory(cfg)

	now := time.Now().UnixNano()
	for i := 0; i < 5; i++ {
		vh.Add(1, []float32{float32(i)}, 0.0, now+int64(i)*1000, nil)
	}

	history := vh.GetHistory(1)
	assert.Len(t, history, 3)
	assert.Equal(t, 5, history[2].Version)
}

func TestVersionHistory_GetVersionsAtBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	vh := NewVersionHistory(DefaultVersionHistoryConfig())

	now := time.Now().UnixNano()
	// id=1: two versions; id=2: one version; id=3: only future versions.
	vh.Add(1, []float32{1.0}, 0.0, now-2000, nil)
	vh.Add(1, []float32{2.0}, 0.0, now-1000, nil)
	vh.Add(2, []float32{9.0}, 0.0, now-500, nil)
	vh.Add(3, []float32{7.0}, 0.0, now+5000, nil)

	ids := []uint64{1, 2, 3, 99}
	out := make(map[uint64]VersionedVector, len(ids))
	vh.GetVersionsAtBatch(ids, now, out)

	require.Len(t, out, 2)
	assert.Equal(t, []float32{2.0}, out[1].Vector)
	assert.Equal(t, []float32{9.0}, out[2].Vector)
	_, has3 := out[3]
	assert.False(t, has3, "id 3 has no version at or before timestamp")
	_, has99 := out[99]
	assert.False(t, has99)
}

// legacyVersionHistory is the pre-index implementation of every method the
// index replaced, copied verbatim. It is the parity reference: for a given
// insert sequence it is the definition of what GetVersionAt and
// GetVersionsAtBatch must return, including the tie-break between versions
// that share a timestamp and the omission of ids with nothing at or before
// the query.
type legacyVersionHistory struct {
	maxVersions int
	retention   time.Duration
	history     map[uint64][]VersionedVector
}

func newLegacyVersionHistory(maxVersions int) *legacyVersionHistory {
	return &legacyVersionHistory{
		maxVersions: maxVersions,
		retention:   7 * 24 * time.Hour,
		history:     make(map[uint64][]VersionedVector),
	}
}

func (l *legacyVersionHistory) Add(id uint64, vector []float32, norm float32, timestamp int64, metadata []byte) {
	existing := l.history[id]
	newVersion := 1
	if len(existing) > 0 {
		newVersion = existing[len(existing)-1].Version + 1
	}

	versioned := VersionedVector{
		ID:        id,
		Vector:    vector,
		Norm:      norm,
		Timestamp: timestamp,
		Metadata:  metadata,
		Version:   newVersion,
	}

	l.history[id] = append(l.history[id], versioned)

	if len(l.history[id]) > l.maxVersions {
		l.history[id] = l.history[id][len(l.history[id])-l.maxVersions:]
	}
}

func (l *legacyVersionHistory) GetVersion(id uint64, version int) (*VersionedVector, error) {
	versions, ok := l.history[id]
	if !ok || len(versions) == 0 {
		return nil, fmt.Errorf("vector %d not found", id)
	}

	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].Version == version {
			return &versions[i], nil
		}
	}

	return nil, fmt.Errorf("version %d not found for vector %d", version, id)
}

func (l *legacyVersionHistory) GetVersionAt(id uint64, timestamp int64) (*VersionedVector, error) {
	versions, ok := l.history[id]
	if !ok || len(versions) == 0 {
		return nil, fmt.Errorf("vector %d not found", id)
	}

	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].Timestamp <= timestamp {
			return &versions[i], nil
		}
	}

	return nil, fmt.Errorf("no version found at or before timestamp %d for vector %d", timestamp, id)
}

// legacyBatch is the pre-index batch read: a map lookup per id followed by the
// reverse scan over that id's insertion order.
func legacyBatch(history map[uint64][]VersionedVector, ids []uint64, timestamp int64, out map[uint64]VersionedVector) {
	for _, id := range ids {
		versions, ok := history[id]
		if !ok || len(versions) == 0 {
			continue
		}

		for i := len(versions) - 1; i >= 0; i-- {
			if versions[i].Timestamp <= timestamp {
				out[id] = versions[i]
				break
			}
		}
	}
}

func (l *legacyVersionHistory) GetVersionsAtBatch(ids []uint64, timestamp int64, out map[uint64]VersionedVector) {
	legacyBatch(l.history, ids, timestamp, out)
}

func (l *legacyVersionHistory) GetHistory(id uint64) []VersionedVector {
	versions, ok := l.history[id]
	if !ok {
		return nil
	}

	result := make([]VersionedVector, len(versions))
	copy(result, versions)
	return result
}

func (l *legacyVersionHistory) GetLatestVersion(id uint64) (*VersionedVector, error) {
	versions, ok := l.history[id]
	if !ok || len(versions) == 0 {
		return nil, fmt.Errorf("vector %d not found", id)
	}

	return &versions[len(versions)-1], nil
}

func (l *legacyVersionHistory) Prune(ctx context.Context, beforeTimestamp int64) int {
	pruned := 0
	for id, versions := range l.history {
		filtered := make([]VersionedVector, 0)
		for _, v := range versions {
			if v.Timestamp > beforeTimestamp {
				filtered = append(filtered, v)
			} else {
				pruned++
			}
		}
		if len(filtered) == 0 {
			delete(l.history, id)
		} else {
			l.history[id] = filtered
		}
	}

	return pruned
}

func (l *legacyVersionHistory) Size() int { return len(l.history) }

func (l *legacyVersionHistory) TotalVersions() int {
	total := 0
	for _, versions := range l.history {
		total += len(versions)
	}
	return total
}

// requireBatchParity asserts that both the entity fallback path and the
// published index path answer exactly like the legacy implementation, for
// every id (including the ones the history never saw) and every timestamp.
func requireBatchParity(t *testing.T, vh *VersionHistory, legacy *legacyVersionHistory, ids []uint64, timestamps []int64, label string) {
	t.Helper()

	check := func(path string) {
		for _, ts := range timestamps {
			want := make(map[uint64]VersionedVector, len(ids))
			legacy.GetVersionsAtBatch(ids, ts, want)
			got := make(map[uint64]VersionedVector, len(ids))
			vh.GetVersionsAtBatch(ids, ts, got)
			require.Equal(t, want, got, "%s: %s batch at timestamp %d", label, path, ts)

			for _, id := range ids {
				wantVec, wantErr := legacy.GetVersionAt(id, ts)
				gotVec, gotErr := vh.GetVersionAt(id, ts)
				if wantErr != nil {
					require.EqualError(t, gotErr, wantErr.Error(), "%s: %s GetVersionAt(%d, %d)", label, path, id, ts)
					require.Nil(t, gotVec)
					continue
				}
				require.NoError(t, gotErr, "%s: %s GetVersionAt(%d, %d)", label, path, id, ts)
				require.Equal(t, *wantVec, *gotVec, "%s: %s GetVersionAt(%d, %d)", label, path, id, ts)
			}
		}
	}

	check("entities")

	idx := vh.rebuildVersionIndex()
	require.NotNil(t, idx, "%s: expected a published index", label)
	check("index")
}

// parityTimestamps returns query timestamps that sit on every interesting side
// of every stored timestamp: the int64 extremes, the epoch, negatives, and
// the values stored by the tests that use them.
func parityTimestamps(extra ...int64) []int64 {
	stamps := []int64{
		math.MinInt64,
		math.MinInt64 + 1,
		-1 << 62,
		-1000,
		-1,
		0,
		1,
		1000,
		1 << 40,
		math.MaxInt64 - 1,
		math.MaxInt64,
	}
	return append(stamps, extra...)
}

func TestVersionHistory_ActiveAtTimestampIsUpperBound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	vh := NewVersionHistory(DefaultVersionHistoryConfig())
	legacy := newLegacyVersionHistory(10)

	for v, ts := range []int64{10, 20, 30} {
		vh.Add(7, []float32{float32(v)}, float32(v), ts, nil)
		legacy.Add(7, []float32{float32(v)}, float32(v), ts, nil)
	}

	for _, tc := range []struct {
		query   int64
		want    int
		wantErr string
	}{
		{query: 0, wantErr: "no version found at or before timestamp 0 for vector 7"},
		{query: 9, wantErr: "no version found at or before timestamp 9 for vector 7"},
		{query: 10, want: 1},
		{query: 19, want: 1},
		{query: 20, want: 2},
		{query: 30, want: 3},
		{query: math.MaxInt64, want: 3},
	} {
		got, err := vh.GetVersionAt(7, tc.query)
		if tc.wantErr != "" {
			require.EqualError(t, err, tc.wantErr)
			continue
		}
		require.NoError(t, err, "query %d", tc.query)
		assert.Equal(t, tc.want, got.Version, "query %d", tc.query)
	}

	// A lower bound would have returned the version *after* the query, so the
	// batch is pinned on the same rule.
	out := make(map[uint64]VersionedVector)
	vh.GetVersionsAtBatch([]uint64{7}, 25, out)
	require.Len(t, out, 1)
	assert.Equal(t, 2, out[7].Version)
}

func TestVersionHistory_DuplicateTimestampsTakeLastInserted(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	vh := NewVersionHistory(DefaultVersionHistoryConfig())
	legacy := newLegacyVersionHistory(10)

	for v, ts := range []int64{5, 5, 5, 9} {
		vec := []float32{float32(v)}
		vh.Add(1, vec, float32(v), ts, nil)
		legacy.Add(1, vec, float32(v), ts, nil)
	}

	// The three versions at 5 tie, and the tie goes to the last one inserted,
	// which is the rule the reverse scan implemented.
	got, err := vh.GetVersionAt(1, 5)
	require.NoError(t, err)
	assert.Equal(t, 3, got.Version)

	ts := int64(0)
	requireBatchParity(t, vh, legacy, []uint64{1, 0, 1 << 40}, parityTimestamps(ts, 5, 9, 6), "duplicate timestamps")
}

func TestVersionHistory_ParityCorners(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	cases := []struct {
		name        string
		maxVersions int
		stamps      []int64
	}{
		{name: "single version", maxVersions: 4, stamps: []int64{0}},
		{name: "many versions", maxVersions: 128, stamps: []int64{-40, -30, -20, -10, 0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100}},
		{name: "duplicate timestamps", maxVersions: 16, stamps: []int64{7, 7, 7, 7, 8, 8, 9, 7}},
		{name: "negative timestamps", maxVersions: 8, stamps: []int64{-900, -800, -700, -600, -500}},
		{name: "epoch only", maxVersions: 8, stamps: []int64{0, 0, 0}},
		{name: "int64 extremes", maxVersions: 8, stamps: []int64{math.MinInt64, math.MinInt64, -1, 0, math.MaxInt64 - 1, math.MaxInt64}},
		{name: "zero retention", maxVersions: 0, stamps: []int64{1, 2, 3}},
		{name: "single version retained", maxVersions: 1, stamps: []int64{1, 2, 3}},
		{name: "unbounded versions", maxVersions: 1 << 20, stamps: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vh := NewVersionHistory(VersionHistoryConfig{MaxVersions: tc.maxVersions, RetentionPeriod: time.Hour})
			legacy := newLegacyVersionHistory(tc.maxVersions)

			// id 1 carries the case's stamps, id 2 a single version far in the
			// future, and 3 and 4 are never inserted.
			for v, ts := range tc.stamps {
				vec := []float32{float32(v)}
				vh.Add(1, vec, float32(v), ts, nil)
				legacy.Add(1, vec, float32(v), ts, nil)
			}
			vec := []float32{42}
			vh.Add(2, vec, 42, math.MaxInt64, nil)
			legacy.Add(2, vec, 42, math.MaxInt64, nil)

			ids := []uint64{0, 1, 2, 3, 4, 1 << 40, math.MaxUint64}
			requireBatchParity(t, vh, legacy, ids, parityTimestamps(tc.stamps...), tc.name)

			require.Equal(t, legacy.Size(), vh.Size(), "%s: Size", tc.name)
			require.Equal(t, legacy.TotalVersions(), vh.TotalVersions(), "%s: TotalVersions", tc.name)
			for _, id := range ids {
				require.Equal(t, legacy.GetHistory(id), vh.GetHistory(id), "%s: GetHistory(%d)", tc.name, id)

				wantLatest, wantErr := legacy.GetLatestVersion(id)
				gotLatest, gotErr := vh.GetLatestVersion(id)
				if wantErr != nil {
					require.EqualError(t, gotErr, wantErr.Error(), "%s: GetLatestVersion(%d)", tc.name, id)
					continue
				}
				require.NoError(t, gotErr)
				require.Equal(t, *wantLatest, *gotLatest)

				for version := 0; version <= len(tc.stamps)+1; version++ {
					wantVec, wantErr := legacy.GetVersion(id, version)
					gotVec, gotErr := vh.GetVersion(id, version)
					if wantErr != nil {
						require.EqualError(t, gotErr, wantErr.Error(), "%s: GetVersion(%d, %d)", tc.name, id, version)
						continue
					}
					require.NoError(t, gotErr)
					require.Equal(t, *wantVec, *gotVec)
				}
			}
		})
	}
}

func TestVersionHistory_ParityRandomizedInserts(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	for _, order := range []string{"monotonic", "shuffled", "tied"} {
		t.Run(order, func(t *testing.T) {
			const nIDs = 64
			rng := rand.New(rand.NewSource(0x5eed)) // #nosec G404 -- deterministic parity fixture
			vh := NewVersionHistory(VersionHistoryConfig{MaxVersions: 8, RetentionPeriod: time.Hour})
			legacy := newLegacyVersionHistory(8)

			base := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC).UnixNano()
			stamps := []int64{base, base + 1, base + 1, base + 2, base + 5, base + 5, base + 5, base + 9, -1, 0, 1}
			ids := make([]uint64, 0, nIDs+8)
			for i := uint64(0); i < nIDs; i++ {
				ids = append(ids, i)
			}
			// Ids the history never sees, including the extremes of the key space.
			ids = append(ids, nIDs, nIDs+1, 1<<20, math.MaxUint64-1, math.MaxUint64)

			for step := range 2000 {
				id := uint64(rng.Intn(nIDs))
				var ts int64
				switch order {
				case "monotonic":
					ts = base + int64(step)
				case "shuffled":
					ts = stamps[rng.Intn(len(stamps))]
				default:
					ts = base + int64(rng.Intn(3))
				}
				vec := []float32{float32(step)}
				vh.Add(id, vec, float32(step), ts, nil)
				legacy.Add(id, vec, float32(step), ts, nil)
			}

			query := append(parityTimestamps(stamps...), base+1, base+5, base+9, base+2)
			requireBatchParity(t, vh, legacy, ids, query, order)
			require.Equal(t, legacy.Size(), vh.Size())
			require.Equal(t, legacy.TotalVersions(), vh.TotalVersions())
		})
	}
}

func TestVersionHistory_ParityPrune(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ctx := context.Background()

	rng := rand.New(rand.NewSource(7)) // #nosec G404 -- deterministic parity fixture
	vh := NewVersionHistory(VersionHistoryConfig{MaxVersions: 8, RetentionPeriod: time.Hour})
	legacy := newLegacyVersionHistory(8)

	ids := make([]uint64, 0, 40)
	for i := uint64(0); i < 32; i++ {
		ids = append(ids, i)
	}
	for step := range 800 {
		id := ids[rng.Intn(len(ids))]
		ts := int64(rng.Intn(1000)) - 500
		vec := []float32{float32(step)}
		vh.Add(id, vec, float32(step), ts, nil)
		legacy.Add(id, vec, float32(step), ts, nil)
	}

	for range 4 {
		before := int64(rng.Intn(1000)) - 500
		require.Equal(t, legacy.Prune(ctx, before), vh.Prune(ctx, before), "prune before %d", before)
		requireBatchParity(t, vh, legacy, ids, parityTimestamps(before-1, before, before+1), "after prune")
		require.Equal(t, legacy.Size(), vh.Size())
		require.Equal(t, legacy.TotalVersions(), vh.TotalVersions())
	}
}

func TestVersionHistory_ParityIndexMatchesEntities(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	vh := NewVersionHistory(VersionHistoryConfig{MaxVersions: 6, RetentionPeriod: time.Hour})
	rng := rand.New(rand.NewSource(11)) // #nosec G404 -- deterministic parity fixture

	sortedIDs := make([]uint64, 0, 64)
	for i := uint64(0); i < 64; i++ {
		sortedIDs = append(sortedIDs, i*3+7)
		for v := range 6 {
			ts := int64(v*10) + int64(i)
			vh.Add(sortedIDs[len(sortedIDs)-1], []float32{float32(v)}, float32(v), ts, nil)
		}
	}
	// These ids get their versions in a random order, so their timestamp
	// column is dropped and the index must fall back for them.
	for i := uint64(0); i < 32; i++ {
		id := 1000 + i
		for range 5 {
			vh.Add(id, []float32{1}, 1, int64(rng.Intn(50)), nil)
		}
	}

	idx := vh.rebuildVersionIndex()
	require.NotNil(t, idx)
	require.Equal(t, len(vh.entities), len(idx.slot))

	queried := append(parityTimestamps(), make([]int64, 0, 64)...)
	for i := 0; i < 64; i++ {
		queried = append(queried, int64(i))
	}

	for id, ent := range vh.entities {
		slot, ok := idx.slotOf(id)
		require.True(t, ok, "id %d missing from the index", id)
		require.Equal(t, ent.sorted, idx.ordered[slot/64]&(1<<(slot%64)) != 0, "id %d ordered bit", id)
		lo := int(idx.entOff[slot])
		hi := int(idx.entOff[slot+1])
		require.Equal(t, ent.vers, idx.vers[lo:hi], "id %d records", id)
		require.Equal(t, ent.timestamps(), idx.ts[lo:hi], "id %d timestamps", id)

		for _, ts := range queried {
			want := ent.active(ts)
			pos, ok := idx.activeSlot(slot, ts)
			if !ok {
				require.False(t, ent.sorted, "id %d: unsorted groups must not answer from the index", id)
				continue
			}
			if want < 0 {
				require.Negative(t, pos, "id %d at %d has no version", id, ts)
				continue
			}
			require.Equal(t, want, pos-lo, "id %d active at %d", id, ts)
		}
	}
}

func TestVersionHistory_IndexDenseColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	t.Run("contiguous ids", func(t *testing.T) {
		vh := NewVersionHistory(DefaultVersionHistoryConfig())
		for id := uint64(10); id < 20; id++ {
			if id == 15 {
				continue
			}
			vh.Add(id, []float32{1}, 1, int64(id), nil)
		}
		idx := vh.rebuildVersionIndex()
		require.NotNil(t, idx.dense)
		assert.Equal(t, uint64(10), idx.denseBase)

		for id := uint64(10); id < 20; id++ {
			if id == 15 {
				continue
			}
			slot, ok := idx.slotOf(id)
			require.True(t, ok, "id %d", id)
			assert.Equal(t, idx.slot[id], slot)
		}
		// Inside the dense window but untracked resolves to absent, and so
		// does anything outside it.
		_, ok := idx.slotOf(15)
		assert.False(t, ok)
		_, ok = idx.slotOf(5)
		assert.False(t, ok)
		_, ok = idx.slotOf(1 << 40)
		assert.False(t, ok)
	})

	t.Run("sparse ids", func(t *testing.T) {
		vh := NewVersionHistory(DefaultVersionHistoryConfig())
		for i := uint64(0); i < 32; i++ {
			vh.Add(1+i<<20, []float32{1}, 1, int64(i), nil)
		}
		idx := vh.rebuildVersionIndex()
		assert.Nil(t, idx.dense, "a sparse id space must not get a dense column")

		for i := uint64(0); i < 32; i++ {
			slot, ok := idx.slotOf(1 + i<<20)
			require.True(t, ok)
			assert.Equal(t, idx.slot[1+i<<20], slot)
		}
		_, ok := idx.slotOf(1 << 40)
		assert.False(t, ok)
	})
}

func TestVersionHistory_ErrorsPreserved(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	vh := NewVersionHistory(DefaultVersionHistoryConfig())
	vh.Add(5, []float32{1}, 1, 100, nil)

	_, err := vh.GetVersionAt(5, 99)
	require.EqualError(t, err, "no version found at or before timestamp 99 for vector 5")
	_, err = vh.GetVersionAt(6, 99)
	require.EqualError(t, err, "vector 6 not found")
	_, err = vh.GetVersion(5, 4)
	require.EqualError(t, err, "version 4 not found for vector 5")
	_, err = vh.GetVersion(6, 1)
	require.EqualError(t, err, "vector 6 not found")
	_, err = vh.GetLatestVersion(6)
	require.EqualError(t, err, "vector 6 not found")

	// An id that only ever gets pruned away keeps reporting as missing.
	pruned := NewVersionHistory(VersionHistoryConfig{MaxVersions: 4, RetentionPeriod: time.Hour})
	pruned.Add(9, []float32{1}, 1, 10, nil)
	require.Equal(t, 1, pruned.Prune(context.Background(), 10))
	_, err = pruned.GetVersionAt(9, 100)
	require.EqualError(t, err, "vector 9 not found")
	assert.Equal(t, 0, pruned.Size())
	assert.Equal(t, 0, pruned.TotalVersions())
}

func TestVersionHistory_ConcurrentReadsAndWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	vh := NewVersionHistory(VersionHistoryConfig{MaxVersions: 8, RetentionPeriod: time.Hour})

	const nIDs = 64
	ids := make([]uint64, nIDs)
	for i := range ids {
		ids[i] = uint64(i)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	const writers = 4
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			own := ids[w*len(ids)/writers : (w+1)*len(ids)/writers]
			for step := 0; ; step++ {
				select {
				case <-stop:
					return
				default:
				}
				vh.Add(own[step%len(own)], []float32{float32(step)}, float32(step), int64(step*(w+1)), nil)
				if step%128 == 0 {
					vh.Prune(context.Background(), int64(step*(w+1))-16)
				}
			}
		}(w)
	}

	for r := range 4 {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			out := make(map[uint64]VersionedVector, nIDs)
			for i := range 500 {
				vh.GetVersionsAtBatch(ids, int64(i*97), out)
				for _, id := range ids {
					_, _ = vh.GetVersionAt(id, int64(i*97))
					_ = vh.GetHistory(id)
					_, _ = vh.GetLatestVersion(id)
				}
				vh.Size()
				vh.TotalVersions()
				if r%2 == 0 {
					vh.rebuildVersionIndex()
				}
			}
		}(r)
	}

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()

	// The writers are done, so the batch has to agree with the insertion order
	// each id ended up with, on both the fallback and the indexed path. A
	// legacy reference cannot be compared here: an unrelated writer's Prune
	// can land between a writer's insert into the history and the same insert
	// into the reference, which is a property of the test harness and not of
	// the history.
	vh.rebuildVersionIndex()
	idx := vh.index.Load()
	require.NotNil(t, idx)

	total := 0
	for i := range ids {
		id := ids[i]
		history := vh.GetHistory(id)
		total += len(history)
		for _, ts := range parityTimestamps(0, 1, 2, 3, 97) {
			want, wantOK := legacyBatchHistory(history, ts)
			got, err := vh.GetVersionAt(id, ts)
			if wantOK {
				require.NoError(t, err, "id %d at %d", id, ts)
				require.Equal(t, want, *got, "id %d at %d", id, ts)
				continue
			}
			if len(history) == 0 {
				require.EqualError(t, err, fmt.Sprintf("vector %d not found", id))
				continue
			}
			require.EqualError(t, err, fmt.Sprintf("no version found at or before timestamp %d for vector %d", ts, id))
		}
	}
	require.Equal(t, total, vh.TotalVersions())
}

func legacyBatchHistory(versions []VersionedVector, timestamp int64) (VersionedVector, bool) {
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].Timestamp <= timestamp {
			return versions[i], true
		}
	}
	return VersionedVector{}, false
}

func TestUpperBoundScalar(t *testing.T) {
	rng := rand.New(rand.NewSource(3)) // #nosec G404 -- deterministic search fixture
	for _, n := range []int{0, 1, 2, 3, 4, 5, 7, 8, 15, 16, 31, 32, 100, 1000} {
		ts := make([]int64, n)
		for i := range ts {
			// A small value range so duplicates are common.
			ts[i] = int64(rng.Intn(n/2 + 1))
		}
		slices.Sort(ts)

		probes := []int64{math.MinInt64, 0, math.MaxInt64}
		if n > 0 {
			probes = append(probes, ts[0]-1)
			for _, v := range ts {
				probes = append(probes, v-1, v, v+1)
			}
		}
		for _, x := range probes {
			want := sortUpperBound(ts, x)
			assert.Equal(t, want, upperBoundScalar(ts, x), "n=%d x=%d", n, x)
		}
	}
}

func sortUpperBound(ts []int64, x int64) int {
	return sortSearch(len(ts), func(i int) bool { return ts[i] > x })
}

func sortSearch(n int, f func(int) bool) int {
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1)
		if !f(h) {
			i = h + 1
		} else {
			j = h
		}
	}
	return i
}

func BenchmarkVersionHistory_GetVersionsAtBatch(b *testing.B) {
	vh := NewVersionHistory(DefaultVersionHistoryConfig())
	now := time.Now().UnixNano()

	const nIDs = 1000
	ids := make([]uint64, nIDs)
	for id := uint64(0); id < nIDs; id++ {
		ids[id] = id
		for v := 0; v < 5; v++ {
			vh.Add(id, []float32{float32(v)}, 0.0, now+int64(v)*1000, nil)
		}
	}

	out := make(map[uint64]VersionedVector, nIDs)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		clear(out)
		vh.GetVersionsAtBatch(ids, now, out)
	}
}

// versionHistoryBench is a fixture shared by the paired benchmarks below: the
// same inserts drive the indexed implementation and the legacy one inside a
// single process, so the comparison cannot be moved by machine drift between
// two separate runs.
type versionHistoryBench struct {
	ids    []uint64
	query  int64
	vh     *VersionHistory
	legacy *legacyVersionHistory
	groups [][]VersionedVector
	idx    *versionHistoryIndex
}

func newVersionHistoryBench(nIDs, versionsPerID int, idStride uint64) *versionHistoryBench {
	vec := []float32{0.5, 0.25, 0.125}
	vh := NewVersionHistory(VersionHistoryConfig{MaxVersions: versionsPerID + 1, RetentionPeriod: time.Hour})
	legacy := newLegacyVersionHistory(versionsPerID + 1)

	ids := make([]uint64, nIDs)
	for i := range ids {
		ids[i] = uint64(i) * idStride
	}

	query := int64(0)
	for i, id := range ids {
		for v := 0; v < versionsPerID; v++ {
			ts := int64(v)*int64(nIDs)*int64(idStride+1) + int64(i)*int64(idStride)
			vh.Add(id, vec, float32(v), ts, nil)
			legacy.Add(id, vec, float32(v), ts, nil)
			if v == versionsPerID/2 {
				query = ts
			}
		}
	}

	f := &versionHistoryBench{ids: ids, query: query, vh: vh, legacy: legacy}
	f.idx = vh.rebuildVersionIndex()
	f.groups = make([][]VersionedVector, nIDs)
	for i, id := range ids {
		f.groups[i] = legacy.history[id]
	}
	return f
}

// legacyLookup resolves an id through the published columns the way the old
// map did and then applies the old reverse scan.
func (f *versionHistoryBench) legacyLookup(ids []uint64, timestamp int64, out map[uint64]VersionedVector) {
	for _, id := range ids {
		var versions []VersionedVector
		if d := id - f.idx.denseBase; f.idx.dense != nil && d < uint64(len(f.idx.dense)) {
			if slot := f.idx.dense[d]; slot != versionSlotAbsent {
				lo := int(f.idx.entOff[slot])
				versions = f.idx.vers[lo:int(f.idx.entOff[slot+1])]
			}
		} else if slot, ok := f.idx.slot[id]; ok {
			lo := int(f.idx.entOff[slot])
			versions = f.idx.vers[lo:int(f.idx.entOff[slot+1])]
		}
		if len(versions) == 0 {
			continue
		}
		for i := len(versions) - 1; i >= 0; i-- {
			if versions[i].Timestamp <= timestamp {
				out[id] = versions[i]
				break
			}
		}
	}
}

func (f *versionHistoryBench) report(b *testing.B) {
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(len(f.ids)), "ns/id")
}

var versionHistoryBenchIDCounts = []int{1000, 10000, 100000}
var versionHistoryBenchVersions = []int{1, 10, 100}

func BenchmarkVersionHistory_GetVersionsAtBatchMatrix(b *testing.B) {
	for _, nIDs := range versionHistoryBenchIDCounts {
		for _, versions := range versionHistoryBenchVersions {
			f := newVersionHistoryBench(nIDs, versions, 1)
			name := fmt.Sprintf("ids=%d/versions=%d", nIDs, versions)

			b.Run(name+"/indexed", func(b *testing.B) {
				out := make(map[uint64]VersionedVector, nIDs)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					clear(out)
					f.vh.GetVersionsAtBatch(f.ids, f.query, out)
				}
				f.report(b)
			})

			b.Run(name+"/legacy-scan", func(b *testing.B) {
				out := make(map[uint64]VersionedVector, nIDs)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					clear(out)
					f.legacy.GetVersionsAtBatch(f.ids, f.query, out)
				}
				f.report(b)
			})
		}
	}
}

// BenchmarkVersionHistory_GetVersionsAtBatchCrossover sweeps the number of
// versions per id against both implementations, which is where the linear scan
// stops winning.
func BenchmarkVersionHistory_GetVersionsAtBatchCrossover(b *testing.B) {
	const nIDs = 20000
	for _, versions := range []int{1, 2, 3, 4, 6, 8, 12, 16, 24, 32, 48, 64, 96, 128, 256} {
		f := newVersionHistoryBench(nIDs, versions, 1)
		name := fmt.Sprintf("versions=%d", versions)

		b.Run(name+"/indexed", func(b *testing.B) {
			out := make(map[uint64]VersionedVector, nIDs)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				clear(out)
				f.vh.GetVersionsAtBatch(f.ids, f.query, out)
			}
			f.report(b)
		})

		b.Run(name+"/legacy-scan", func(b *testing.B) {
			out := make(map[uint64]VersionedVector, nIDs)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				clear(out)
				f.legacy.GetVersionsAtBatch(f.ids, f.query, out)
			}
			f.report(b)
		})

		// Same id resolution as the indexed path, old reverse scan instead of
		// the binary search: the isolated crossover.
		b.Run(name+"/linear-over-columns", func(b *testing.B) {
			out := make(map[uint64]VersionedVector, nIDs)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				clear(out)
				f.legacyLookup(f.ids, f.query, out)
			}
			f.report(b)
		})
	}
}

// BenchmarkVersionHistory_BatchResolution isolates the batch fast path: the
// same columns and the same ids, resolved once through the dense id column and
// once through the map, so the difference is the id resolution alone.
func BenchmarkVersionHistory_BatchResolution(b *testing.B) {
	const nIDs, versions = 100000, 10
	f := newVersionHistoryBench(nIDs, versions, 1)
	require.NotNil(b, f.idx.dense, "the contiguous fixture must build a dense column")

	out := make(map[uint64]VersionedVector, nIDs)

	b.Run("dense-column", func(b *testing.B) {
		f.vh.index.Store(f.idx)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			clear(out)
			f.vh.GetVersionsAtBatch(f.ids, f.query, out)
		}
		f.report(b)
	})

	b.Run("map-lookup", func(b *testing.B) {
		noDense := *f.idx
		noDense.dense = nil
		f.vh.index.Store(&noDense)
		b.Cleanup(func() { f.vh.index.Store(f.idx) })
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			clear(out)
			f.vh.GetVersionsAtBatch(f.ids, f.query, out)
		}
		f.report(b)
	})

	b.Run("legacy-map-lookup-and-scan", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			clear(out)
			f.legacy.GetVersionsAtBatch(f.ids, f.query, out)
		}
		f.report(b)
	})
}
