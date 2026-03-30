# Performance Validation Matrix — Apple M3 Pro Metal GPU

**Generated**: 2026-03-29
**Platform**: Darwin arm64 (macOS Apple Silicon)
**Memory**: 18GB allocated
**Test Tool**: Longbow Unified Benchmark Script
**Version**: 0.1.8-rc1

## Test Configuration
- Dimensions: 128, 384, 768, 1536, 3072
- Vector Counts: 1000 (partial - representative sample)
- Data Types: float32, float64, int8, int16, int32, int64, uint8, uint16
- Duration: 15s per test

## Ingest Performance (vectors/second)

| DType | Dim=128 | Dim=384 | Dim=768 | Dim=1536 | Dim=3072 |
|-------|---------|---------|---------|----------|----------|
| float32 | 539,896 | 260,821 | 256,254 | 139,923 | 91,992 |
| float64 | 449,522 | 282,214 | 159,696 | 96,273 | 52,082 |
| int8 | 879,927 | 678,349 | 499,407 | 409,647 | 250,755 |
| int16 | 794,360 | 578,299 | 393,256 | 261,963 | 160,680 |
| int32 | 592,783 | 396,675 | 246,561 | 152,083 | 80,831 |
| int64 | 458,269 | 265,819 | 165,615 | 95,251 | 44,130 |
| uint8 | 965,523 | 659,631 | 504,032 | 437,892 | 280,961 |
| uint16 | 740,786 | 516,762 | 444,041 | 260,053 | 137,907 |

## Search Performance - Dense QPS

| DType | Dim=128 | Dim=384 | Dim=768 | Dim=1536 | Dim=3072 |
|-------|---------|---------|---------|----------|----------|
| float32 | 3,077 | 2,196 | 1,546 | 1,079 | 606 |
| float64 | 4,298 | 3,373 | 2,579 | 1,858 | 1,134 |
| int8 | 3,134 | 3,305 | 2,443 | 1,768 | 1,164 |
| int16 | 10,805 | 7,923 | 5,646 | 3,640 | 1,945 |
| int32 | 4,921 | 3,258 | 2,513 | 1,813 | 1,081 |
| int64 | 10,850 | 7,923 | 5,677 | 3,618 | 1,937 |
| uint8 | 4,733 | 3,321 | 2,403 | 1,737 | 1,169 |
| uint16 | 10,845 | 7,929 | 5,706 | 3,641 | 1,950 |

## Search Latency - P50 (ms)

| DType | Dim=128 | Dim=384 | Dim=768 | Dim=1536 | Dim=3072 |
|-------|---------|---------|---------|----------|----------|
| float32 | 0.322 | 0.450 | 0.639 | 0.919 | 1.641 |
| float64 | 0.223 | 0.294 | 0.383 | 0.536 | 0.879 |
| int8 | 0.245 | 0.296 | 0.402 | 0.563 | 0.856 |
| int16 | 0.086 | 0.120 | 0.172 | 0.270 | 0.509 |
| int32 | 0.196 | 0.304 | 0.393 | 0.548 | 0.922 |
| int64 | 0.086 | 0.121 | 0.172 | 0.272 | 0.512 |
| uint8 | 0.203 | 0.297 | 0.410 | 0.571 | 0.851 |
| uint16 | 0.085 | 0.120 | 0.170 | 0.270 | 0.509 |

## Notes
- float16: FAILED (Metal not supported for float16)
- complex64, complex128, turboquant: Not tested in this run
- Tests run with 1000 vectors per configuration
- Memory: 18GB allocated per test (LONGBOW_MAX_MEMORY=19327352832)
- Server restarted between each test configuration for clean state
