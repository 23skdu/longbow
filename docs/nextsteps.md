# Longbow Next Steps — Feature Roadmap 2026

**Last Updated**: 2026-04-12

---

## 🎯 REMAINING WORK

| # | Feature | Status | Notes |
|---|---------|--------|-------|
| 1 | Subqueries/CTE | ⏳ NOT STARTED | Advanced SQL - parser updates needed |
| 2 | ONNX Benchmarks | ⏳ NOT STARTED | `internal/onnx/benchmarks_test.go` missing |

---

## Architecture Notes

### Build Tags - Expected Stubs (NOT Issues)

- `internal/gpu/memory/memory_metal_stub.go`
- `internal/gpu/memory/memory_cuda_stub.go`
- `internal/simd/simd_stubs*.go`
- `internal/storage/wal_backend_arrow_iouring_stub.go`