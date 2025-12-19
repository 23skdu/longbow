
# Test Coverage Audit & Implementation Plan
## Longbow Vector Store - Feature Branch: feature/test-coverage-audit

---

## Executive Summary

| Metric | Value |
|--------|-------|
| **Overall Coverage** | 74.6% |
| **Zero Coverage Functions** | 112 |
| **Low Coverage (<50%)** | 19 |
| **Medium Coverage (50-80%)** | 90 |
| **Estimated New Tests** | 76 |

---

## Package Coverage Summary

| Package | Coverage | Status |
|---------|----------|--------|
| `internal/store` | 75.4% | 🟡 Needs improvement |
| `internal/simd` | 79.6% | 🟢 Good |
| `internal/logging` | 61.9% | 🟡 Needs improvement |
| `cmd/longbow` | 0.0% | 🔴 Critical (main.go) |

---

## Top 10 Priority Areas

### Priority #1: S3 Backend Persistence
- **File**: `s3_backend.go`
- **Coverage**: ~60% (CRUD ops 0%)
- **Criticality**: 🔴 HIGH
- **Untested**: `WriteSnapshot`, `ReadSnapshot`, `ListSnapshots`, `DeleteSnapshot`
- **Tests Needed**: 8
- **Rationale**: Cloud persistence critical for production deployments

### Priority #2: Hybrid Search Pipeline
- **File**: `hybrid_pipeline.go`
- **Coverage**: ~70% (main Search 0%)
- **Criticality**: 🔴 HIGH
- **Untested**: `Search`, `applyExactFilters`
- **Tests Needed**: 6
- **Rationale**: Main user-facing hybrid search completely untested

### Priority #3: DoPut Sharding
- **File**: `doput_sharding.go`
- **Coverage**: ~40%
- **Criticality**: 🔴 HIGH
- **Untested**: `AddByLocation`, `SearchVectors`, `SearchDataset`, `MigrateToShardedIndex`
- **Tests Needed**: 10
- **Rationale**: Sharding critical for large dataset scalability

### Priority #4: Peer Replication
- **File**: `peer_integration.go`
- **Coverage**: ~65%
- **Criticality**: 🔴 HIGH
- **Untested**: `RecordSuccess`, `DoPutToPeer`, `DoGetFromPeers`
- **Tests Needed**: 8
- **Rationale**: Data replication essential for high availability

### Priority #5: Fast Path Ordered Filters
- **File**: `fast_path_ordered.go`
- **Coverage**: ~22%
- **Criticality**: 🟠 MEDIUM-HIGH
- **Untested**: `FastPathLessEqual`, `FastPathGreaterEqual`, 10+ type funcs
- **Tests Needed**: 12
- **Rationale**: Performance-critical filter operations

### Priority #6: NUMA Allocator
- **File**: `numa_allocator.go`
- **Coverage**: ~35%
- **Criticality**: 🟡 MEDIUM
- **Untested**: `NumNodes`, `NextNode`, `AllocBytesOnNode`, `setCPUAffinity`
- **Tests Needed**: 8
- **Rationale**: NUMA-aware memory allocation for performance

### Priority #7: Namespace Management
- **File**: `namespace.go`
- **Coverage**: ~70%
- **Criticality**: 🟠 MEDIUM-HIGH
- **Untested**: `AddDataset`, `RemoveDataset`, `HasDataset`, `GetNamespace`
- **Tests Needed**: 6
- **Rationale**: Multi-tenancy support critical for production

### Priority #8: Sharded Dataset Operations
- **File**: `sharded_dataset.go`
- **Coverage**: ~65%
- **Criticality**: 🟡 MEDIUM
- **Untested**: `Index`, `SetIndex`, `Clear`, `ReplaceShardRecords`
- **Tests Needed**: 8
- **Rationale**: Sharded data management operations

### Priority #9: DoGet Pipeline Integration
- **File**: `doget_pipeline_integration.go`
- **Coverage**: ~75%
- **Criticality**: 🟡 MEDIUM
- **Untested**: `DefaultDoGetPipelineConfig`, `incrementPipelineErrors`
- **Tests Needed**: 4
- **Rationale**: Pipeline error handling untested

### Priority #10: Zero Alloc Parser
- **File**: `zero_alloc_parser.go`
- **Coverage**: ~70%
- **Criticality**: 🟡 MEDIUM
- **Untested**: `skipValue`, `skipObject`, `skipArray`, `skipLiteral`, `skipNumber`
- **Tests Needed**: 6
- **Rationale**: JSON parsing edge cases

---

## Step-by-Step Implementation Plan

### Phase 1: S3 Backend Tests (Priority #1)
```
File: internal/store/s3_backend_test.go
Tests:
  1. TestWriteSnapshot_Success
  2. TestWriteSnapshot_BucketError  
  3. TestReadSnapshot_Success
  4. TestReadSnapshot_NotFound
  5. TestListSnapshots_Empty
  6. TestListSnapshots_WithData
  7. TestDeleteSnapshot_Success
  8. TestDeleteSnapshot_NotFound

Approach: Use minio test container or mock S3 client
```

### Phase 2: Hybrid Search Pipeline Tests (Priority #2)
```
File: internal/store/hybrid_pipeline_search_test.go
Tests:
  1. TestHybridPipeline_Search_PureVector
  2. TestHybridPipeline_Search_PureKeyword
  3. TestHybridPipeline_Search_Combined
  4. TestHybridPipeline_ApplyExactFilters_Match
  5. TestHybridPipeline_ApplyExactFilters_NoMatch
  6. TestHybridPipeline_Search_EmptyIndex

Approach: Create test fixtures with known vectors/keywords
```

### Phase 3: DoPut Sharding Tests (Priority #3)
```
File: internal/store/doput_sharding_test.go
Tests:
  1. TestShardedIndex_AddByLocation
  2. TestShardedIndex_SearchVectors_SingleShard
  3. TestShardedIndex_SearchVectors_MultipleShard
  4. TestShardedIndex_SearchDataset
  5. TestShardedIndex_MigrateToSharded_SmallDataset
  6. TestShardedIndex_MigrateToSharded_LargeDataset
  7. TestShardedIndex_GetVectorFromDataset
  8. TestShardedIndex_ExtractVectorFromCol
  9. TestShardedIndex_AddToIndex
  10. TestShardedIndex_Concurrent

Approach: Test shard distribution and cross-shard search
```

### Phase 4: Peer Replication Tests (Priority #4)
```
File: internal/store/peer_replication_test.go
Tests:
  1. TestPeerManager_RecordSuccess
  2. TestPeerManager_DoPutToPeer_Success
  3. TestPeerManager_DoPutToPeer_Failure
  4. TestPeerManager_DoGetFromPeers_Success
  5. TestPeerManager_DoGetFromPeers_PartialFailure
  6. TestPeerManager_DoGetFromPeers_AllFail
  7. TestPeerStreaming_Concurrent
  8. TestPeerManager_HealthTracking

Approach: Mock Flight clients for peer simulation
```

### Phase 5: Fast Path Ordered Tests (Priority #5)
```
File: internal/store/fast_path_ordered_test.go
Tests:
  1. TestFastPathLessEqual_Int64
  2. TestFastPathLessEqual_Int32
  3. TestFastPathLessEqual_Float64
  4. TestFastPathGreaterEqual_Int64
  5. TestFastPathGreaterEqual_Int32
  6. TestFastPathGreaterEqual_Float64
  7. TestFastPathOrdered_Int8
  8. TestFastPathOrdered_Uint64
  9. TestFastPathOrdered_Uint32
  10. TestFastPathOrdered_Uint16
  11. TestFastPathOrdered_Uint8
  12. TestFastPathOrdered_EdgeCases

Approach: Parameterized tests with boundary values
```

### Phase 6: NUMA Allocator Tests (Priority #6)
```
File: internal/store/numa_allocator_ext_test.go
Tests:
  1. TestNUMATopology_NumNodes
  2. TestNUMATopology_NumCPUs
  3. TestNUMATopology_GetNodeCPUs
  4. TestNUMATopology_GetCPUNode
  5. TestNUMAAllocator_NextNode
  6. TestNUMAAllocator_AllocBytesOnNode
  7. TestNUMAAllocator_ParseCPUList
  8. TestCPUAffinity_Set (build-tagged)

Approach: Skip on non-NUMA systems, mock topology
```

### Phase 7: Namespace Tests (Priority #7)
```
File: internal/store/namespace_ext_test.go
Tests:
  1. TestNamespace_AddDataset
  2. TestNamespace_RemoveDataset
  3. TestNamespace_HasDataset
  4. TestNamespaceManager_GetNamespace
  5. TestNamespaceManager_BuildNamespacedPath
  6. TestNamespace_DatasetCount

Approach: Test CRUD operations on namespace datasets
```

### Phase 8: Sharded Dataset Tests (Priority #8)
```
File: internal/store/sharded_dataset_ext_test.go
Tests:
  1. TestShardedDataset_Index
  2. TestShardedDataset_SetIndex
  3. TestShardedDataset_Clear
  4. TestShardedDataset_ReplaceShardRecords
  5. TestShardedDataset_LastAccess
  6. TestShardedDataset_Version
  7. TestShardedDataset_ForEachInShard
  8. TestShardedDataset_IncrementVersion

Approach: Test mutation operations with validation
```

### Phase 9: DoGet Pipeline Tests (Priority #9)
```
File: internal/store/doget_pipeline_ext_test.go  
Tests:
  1. TestDoGetPipeline_DefaultConfig
  2. TestDoGetPipeline_IncrementErrors
  3. TestDoGetPipeline_ErrorRecovery
  4. TestDoGetPipeline_ConfigValidation

Approach: Test error paths and config defaults
```

### Phase 10: Zero Alloc Parser Tests (Priority #10)
```
File: internal/store/zero_alloc_parser_ext_test.go
Tests:
  1. TestZeroAllocParser_SkipValue_String
  2. TestZeroAllocParser_SkipValue_Number
  3. TestZeroAllocParser_SkipObject
  4. TestZeroAllocParser_SkipArray
  5. TestZeroAllocParser_SkipLiteral
  6. TestZeroAllocParser_SkipNumber

Approach: Test skip functions with complex JSON
```

---

## Execution Timeline

| Phase | Area | Tests | Est. Time |
|-------|------|-------|----------|
| 1 | S3 Backend | 8 | 2 hours |
| 2 | Hybrid Search | 6 | 1.5 hours |
| 3 | DoPut Sharding | 10 | 2.5 hours |
| 4 | Peer Replication | 8 | 2 hours |
| 5 | Fast Path Ordered | 12 | 2 hours |
| 6 | NUMA Allocator | 8 | 1.5 hours |
| 7 | Namespace | 6 | 1 hour |
| 8 | Sharded Dataset | 8 | 1.5 hours |
| 9 | DoGet Pipeline | 4 | 1 hour |
| 10 | Zero Alloc Parser | 6 | 1 hour |
| **Total** | | **76** | **~16 hours** |

---

## Success Criteria

- [ ] All 76 new tests passing
- [ ] Overall coverage increased to >80%
- [ ] Zero critical (HIGH) areas with <70% coverage
- [ ] Zero lint issues
- [ ] All existing tests still passing

---

## Files to Create/Modify

### New Test Files
1. `internal/store/s3_backend_ext_test.go`
2. `internal/store/hybrid_pipeline_search_test.go`
3. `internal/store/doput_sharding_ext_test.go`
4. `internal/store/peer_replication_ext_test.go`
5. `internal/store/fast_path_ordered_ext_test.go`
6. `internal/store/numa_allocator_ext_test.go`
7. `internal/store/namespace_ext_test.go`
8. `internal/store/sharded_dataset_ext_test.go`
9. `internal/store/doget_pipeline_ext_test.go`
10. `internal/store/zero_alloc_parser_ext_test.go`

---

*Generated: 2025-12-18*
*Branch: feature/test-coverage-audit*
