package store

import (
	"errors"
	"sync"
	"time"
)

// RecordVersion represents a specific version of a record or dataset.
type RecordVersion struct {
	VersionID   int64
	Timestamp   time.Time
	CommitID    string
	ParentID    int64
	IsTombstone bool
}

// VersionedDataset manages versions and branches for a single dataset.
type VersionedDataset struct {
	Name     string
	Versions map[int64]RecordVersion
	Branches map[string]BranchConfig
	mu       sync.RWMutex
}

// BranchConfig defines the configuration for a dataset branch.
type BranchConfig struct {
	Name         string
	BaseVersion  int64
	CreatedAt    time.Time
	LastCommitID string
}

// NewVersionedDataset creates a new versioned dataset instance.
func NewVersionedDataset(name string) *VersionedDataset {
	return &VersionedDataset{
		Name:     name,
		Versions: make(map[int64]RecordVersion),
		Branches: make(map[string]BranchConfig),
	}
}

// AddVersion adds a new version to the dataset.
func (vd *VersionedDataset) AddVersion(version RecordVersion) {
	vd.mu.Lock()
	defer vd.mu.Unlock()
	vd.Versions[version.VersionID] = version
}

// GetVersion retrieves a specific version by its ID.
func (vd *VersionedDataset) GetVersion(versionID int64) (RecordVersion, bool) {
	vd.mu.RLock()
	defer vd.mu.RUnlock()
	v, ok := vd.Versions[versionID]
	return v, ok
}

// ListVersions returns all versions in the dataset.
func (vd *VersionedDataset) ListVersions() []RecordVersion {
	vd.mu.RLock()
	defer vd.mu.RUnlock()

	versions := make([]RecordVersion, 0, len(vd.Versions))
	for _, v := range vd.Versions {
		versions = append(versions, v)
	}
	return versions
}

// CreateBranch creates a new branch starting from the specified version.
func (vd *VersionedDataset) CreateBranch(name string, baseVersionID int64) error {
	vd.mu.Lock()
	defer vd.mu.Unlock()

	if _, exists := vd.Branches[name]; exists {
		return errors.New("branch already exists: " + name)
	}

	vd.Branches[name] = BranchConfig{
		Name:        name,
		BaseVersion: baseVersionID,
		CreatedAt:   time.Now(),
	}
	return nil
}

// GetBranch retrieves the configuration for a branch by its name.
func (vd *VersionedDataset) GetBranch(name string) (BranchConfig, bool) {
	vd.mu.RLock()
	defer vd.mu.RUnlock()
	b, ok := vd.Branches[name]
	return b, ok
}

// ListBranches returns all branches in the dataset.
func (vd *VersionedDataset) ListBranches() []BranchConfig {
	vd.mu.RLock()
	defer vd.mu.RUnlock()

	branches := make([]BranchConfig, 0, len(vd.Branches))
	for _, b := range vd.Branches {
		branches = append(branches, b)
	}
	return branches
}

// DeleteBranch removes a branch by its name.
func (vd *VersionedDataset) DeleteBranch(name string) error {
	vd.mu.Lock()
	defer vd.mu.Unlock()

	if _, exists := vd.Branches[name]; !exists {
		return errors.New("branch not found: " + name)
	}

	delete(vd.Branches, name)
	return nil
}

// VersionRetentionPolicy defines the rules for retaining dataset versions.
type VersionRetentionPolicy struct {
	MaxVersions    int
	MaxAge         time.Duration
	KeepTombstones bool
}

// ApplyRetentionPolicy removes versions that exceed the retention criteria.
func (vd *VersionedDataset) ApplyRetentionPolicy(policy VersionRetentionPolicy) (int, error) {
	vd.mu.Lock()
	defer vd.mu.Unlock()

	now := time.Now()
	removed := 0

	var toRemove []int64
	for id, v := range vd.Versions {
		if policy.MaxAge > 0 && now.Sub(v.Timestamp) > policy.MaxAge {
			if !policy.KeepTombstones || !v.IsTombstone {
				toRemove = append(toRemove, id)
			}
		}
	}

	if policy.MaxVersions > 0 && len(vd.Versions)-len(toRemove) > policy.MaxVersions {
		versions := make([]int64, 0, len(vd.Versions))
		for id := range vd.Versions {
			versions = append(versions, id)
		}
		sortVersionsDesc(versions)

		for i := policy.MaxVersions; i < len(versions); i++ {
			v := vd.Versions[versions[i]]
			if !policy.KeepTombstones || !v.IsTombstone {
				toRemove = append(toRemove, versions[i])
			}
		}
	}

	for _, id := range toRemove {
		delete(vd.Versions, id)
		removed++
	}

	return removed, nil
}

func sortVersionsDesc(versions []int64) {
	for i := 0; i < len(versions); i++ {
		for j := i + 1; j < len(versions); j++ {
			if versions[j] > versions[i] {
				versions[i], versions[j] = versions[j], versions[i]
			}
		}
	}
}

// VersionQuery defines the criteria for querying a specific version.
type VersionQuery struct {
	VersionID *int64
	AsOfTime  *time.Time
	Branch    string
}

// QueryVersion retrieves a version based on the specified query criteria.
func (vd *VersionedDataset) QueryVersion(q VersionQuery) (RecordVersion, error) {
	vd.mu.RLock()
	defer vd.mu.RUnlock()

	if q.VersionID != nil {
		v, ok := vd.Versions[*q.VersionID]
		if !ok {
			return RecordVersion{}, errors.New("version not found")
		}
		return v, nil
	}

	if q.AsOfTime != nil {
		var best RecordVersion
		found := false
		for _, v := range vd.Versions {
			if v.Timestamp.Before(*q.AsOfTime) {
				if !found || v.Timestamp.After(best.Timestamp) {
					best = v
					found = true
				}
			}
		}
		if !found {
			return RecordVersion{}, errors.New("no version found before specified time")
		}
		return best, nil
	}

	if q.Branch != "" {
		branch, ok := vd.Branches[q.Branch]
		if !ok {
			return RecordVersion{}, errors.New("branch not found")
		}
		v, ok := vd.Versions[branch.BaseVersion]
		if !ok {
			return RecordVersion{}, errors.New("base version not found for branch")
		}
		return v, nil
	}

	return RecordVersion{}, errors.New("must specify version ID, as-of time, or branch")
}

// VersionManager coordinates versioned datasets across the system.
type VersionManager struct {
	datasets map[string]*VersionedDataset
	mu       sync.RWMutex
}

// NewVersionManager creates a new VersionManager instance.
func NewVersionManager() *VersionManager {
	return &VersionManager{
		datasets: make(map[string]*VersionedDataset),
	}
}

// GetOrCreateDataset retrieves a versioned dataset or creates one if it doesn't exist.
func (vm *VersionManager) GetOrCreateDataset(name string) *VersionedDataset {
	vm.mu.RLock()
	vd, ok := vm.datasets[name]
	vm.mu.RUnlock()

	if ok {
		return vd
	}

	vm.mu.Lock()
	defer vm.mu.Unlock()

	if vd, ok = vm.datasets[name]; ok {
		return vd
	}

	vd = NewVersionedDataset(name)
	vm.datasets[name] = vd
	return vd
}

// GetDataset retrieves a versioned dataset by name.
func (vm *VersionManager) GetDataset(name string) (*VersionedDataset, bool) {
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	vd, ok := vm.datasets[name]
	return vd, ok
}

// DeleteDataset removes a versioned dataset by name.
func (vm *VersionManager) DeleteDataset(name string) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	delete(vm.datasets, name)
}
