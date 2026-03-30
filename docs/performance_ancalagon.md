# Performance Validation Matrix — Ancalagon (Linux Server)

**Generated**: 2026-03-29
**Platform**: Linux x86_64 (Remote Server)
**Memory**: 12GB allocated
**Test Tool**: Longbow Unified Benchmark Script
**Version**: 0.1.8-rc1

## Test Configuration
- Dimensions: 128, 384, 768, 1536, 3072
- Vector Counts: 10000, 25000, 5000
- Data Types: complex64, float32, float64, int16, int32, int8, turboquant
- Duration: 15s per test

## Ingest Performance (10000 vectors - vec/s)

| DType | Dim=128 | Dim=384 | Dim=768 | Dim=1536 | Dim=3072 |
|-------|---------|---------|---------|----------|----------|
| complex64 | - | - | - | - | - |
| float32 | 592,566 | 196,942 | - | - | - |
| float64 | 348,249 | - | - | - | - |
| int16 | 1,073,776 | - | - | - | - |
| int32 | 758,175 | - | - | - | - |
| int8 | 1,782,229 | - | - | - | - |
| turboquant | 708,824 | 167,772 | - | - | - |

## Search Performance - Dense QPS (10000 vectors)

| DType | Dim=128 | Dim=384 | Dim=768 | Dim=1536 | Dim=3072 |
|-------|---------|---------|---------|----------|----------|
| complex64 | - | - | - | - | - |
| float32 | 2,052 | 3,612 | - | - | - |
| float64 | 2,418 | - | - | - | - |
| int16 | 5,766 | - | - | - | - |
| int32 | 6,728 | - | - | - | - |
| int8 | 2,646 | - | - | - | - |
| turboquant | 279 | 226 | - | - | - |

## Ingest Performance (25000 vectors - vec/s)

| DType | Dim=128 | Dim=384 | Dim=768 | Dim=1536 | Dim=3072 |
|-------|---------|---------|---------|----------|----------|
| complex64 | - | 5,258 | - | - | - |
| float32 | 726,766 | - | - | - | - |
| float64 | 330,895 | - | - | - | - |
| int16 | 1,215,407 | - | - | - | - |
| int32 | - | 13,037 | - | - | - |
| int8 | 2,057,960 | - | - | - | - |
| turboquant | 696,568 | 162,445 | - | - | - |

## Search Performance - Dense QPS (25000 vectors)

| DType | Dim=128 | Dim=384 | Dim=768 | Dim=1536 | Dim=3072 |
|-------|---------|---------|---------|----------|----------|
| complex64 | - | 1,012 | - | - | - |
| float32 | 1,769 | - | - | - | - |
| float64 | 2,302 | - | - | - | - |
| int16 | 6,028 | - | - | - | - |
| int32 | - | 114 | - | - | - |
| int8 | 2,603 | - | - | - | - |
| turboquant | 270 | 285 | - | - | - |

## Ingest Performance (5000 vectors - vec/s)

| DType | Dim=128 | Dim=384 | Dim=768 | Dim=1536 | Dim=3072 |
|-------|---------|---------|---------|----------|----------|
| complex64 | - | - | - | - | - |
| float32 | 594,534 | - | - | - | - |
| float64 | 342,970 | - | - | - | - |
| int16 | 903,410 | - | - | - | - |
| int32 | 560,256 | - | - | - | - |
| int8 | 178,489 | 46,415 | - | - | - |
| turboquant | 493,157 | 175,646 | - | - | - |

## Search Performance - Dense QPS (5000 vectors)

| DType | Dim=128 | Dim=384 | Dim=768 | Dim=1536 | Dim=3072 |
|-------|---------|---------|---------|----------|----------|
| complex64 | - | - | - | - | - |
| float32 | 1,662 | - | - | - | - |
| float64 | 2,207 | - | - | - | - |
| int16 | 5,809 | - | - | - | - |
| int32 | 1,826 | - | - | - | - |
| int8 | 2,278 | 86 | - | - | - |
| turboquant | 205 | 234 | - | - | - |

## Notes
- Tests run via SSH on ancalagon server
- Memory: 12GB allocated per test (LONGBOW_MAX_MEMORY=12884901888)
- Server restarted between each test configuration for clean state
- Complex types (complex64, turboquant) tested on this run
