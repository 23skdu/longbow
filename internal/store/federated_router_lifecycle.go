package store

// This file contains the lifecycle operations for FederatedQueryRouter.
// Membership changes publish immutable maps so searches can keep using an
// older snapshot while a refresh/removal is in progress.

// snapshotSearchers returns the currently published searcher map. Callers must
// treat it as read-only; replaceSearcher publishes a new map instead of
// mutating it.
func (fqr *FederatedQueryRouter) snapshotSearchers() map[string]VectorSearcher {
	current := fqr.searchers.Load()
	if current == nil {
		return nil
	}
	return *current
}

// replaceSearcher atomically publishes a searcher map with name added,
// replaced, or (when searcher is nil) removed. The copy is done outside any
// exclusive router lock, so in-flight searches are never serialized by a
// membership refresh.
func (fqr *FederatedQueryRouter) replaceSearcher(name string, searcher VectorSearcher) {
	for {
		current := fqr.searchers.Load()
		capacity := 1
		if current != nil {
			capacity = len(*current) + 1
		}
		next := make(map[string]VectorSearcher, capacity)
		if current != nil {
			for col, existing := range *current {
				next[col] = existing
			}
		}
		if searcher == nil {
			delete(next, name)
		} else {
			next[name] = searcher
		}
		if fqr.searchers.CompareAndSwap(current, &next) {
			return
		}
	}
}

// searcherFor returns a published searcher without taking a lock.
func (fqr *FederatedQueryRouter) searcherFor(name string) (VectorSearcher, bool) {
	searcher, ok := fqr.snapshotSearchers()[name]
	return searcher, ok
}

// RemoveCollection drops a collection searcher, its metadata, and any routing
// rules pointing at it. In-flight searches keep their immutable map snapshot.
// The return value reports whether anything was registered for the name.
func (fqr *FederatedQueryRouter) RemoveCollection(name string) bool {
	_, hasSearcher := fqr.searcherFor(name)
	_, hasInfo := fqr.registry.GetCollection(name)
	if !hasSearcher && !hasInfo {
		return false
	}
	if hasSearcher {
		fqr.replaceSearcher(name, nil)
	}
	_ = fqr.registry.DeleteCollection(name)
	fqr.registry.RemoveRoutingRulesForCollection(name)
	return true
}

// RefreshCollection replaces a collection searcher and refreshes its metrics
// without taking an exclusive lock around the whole operation. Dimension is
// read before publishing the new searcher because GetDimension may wait on a
// dataset lock. Descriptive metadata is preserved when info omits it.
func (fqr *FederatedQueryRouter) RefreshCollection(name string, searcher VectorSearcher, info *CollectionInfo) {
	if name == "" || searcher == nil {
		return
	}
	fresh := cloneCollectionInfo(info)
	if fresh == nil {
		fresh = &CollectionInfo{}
	}
	fresh.Name = name
	fresh.Dimension = searcher.GetDimension()
	fqr.registry.UpsertCollectionInfo(fresh)
	fqr.replaceSearcher(name, searcher)
}

// hasMissingCollection reports whether any requested dataset lacks a
// registered searcher. It lets the store resync after dataset recreation even
// when the dirty flag was already consumed by an earlier search.
func (fqr *FederatedQueryRouter) hasMissingCollection(names []string) bool {
	for _, name := range names {
		if _, ok := fqr.searcherFor(name); !ok {
			return true
		}
	}
	return false
}

// federatedRouterIfInitialized returns the router without creating one, so
// dataset teardown does not allocate a feature suite it never needed.
func (s *VectorStore) federatedRouterIfInitialized() *FederatedQueryRouter {
	s.featuresMu.Lock()
	defer s.featuresMu.Unlock()
	if s.features == nil {
		return nil
	}
	return s.features.federated
}
