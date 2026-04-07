# Performance Documentation

**Generated**: 2026-03-31
**Platform**: Darwin arm64 (Apple Silicon)
**Test Tool**: Longbow Unified Benchmark Script

---

## Test Configuration

| Parameter | Value |
|-----------|-------|
| Dimensions | 128, 384, 768, 1536, 3072 |
| Batch Sizes | 1,000, 3,000, 5,000, 7,000, 10,000, 15,000 |
| Data Types | float32, float64, float16, int8, int16, int32, int64, uint8, uint16, uint32, uint64, complex64, complex128, turboquant |
| Build Modes | CPU (CGO_ENABLED=0), Metal GPU (CGO_ENABLED=1) |
| Queries per Test | 30 |
| Duration per Test | 3 seconds |

---
## CPU Build - Ingest Performance (Vectors/Second)

### 1,000 Vectors

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | 384,289 | 213,919 | 156,248 | 83,536 |
| **float64** | N/A | 270,060 | 152,964 | 69,752 | 44,816 |
| **float16** | N/A | 566,332 | 442,519 | 252,924 | 164,625 |
| **int8** | N/A | 605,144 | 470,634 | 416,587 | 257,505 |
| **int16** | N/A | 454,856 | 400,267 | 265,666 | 149,725 |
| **int32** | N/A | 399,694 | 272,873 | 159,970 | 89,070 |
| **int64** | N/A | 271,456 | 168,944 | 77,090 | 45,447 |
| **uint8** | N/A | 640,222 | 480,384 | 362,768 | 265,173 |
| **uint16** | N/A | 491,592 | 426,560 | 230,397 | 166,883 |
| **uint32** | N/A | 446,296 | 265,963 | 162,645 | 90,085 |
| **uint64** | N/A | 264,180 | 161,467 | 91,820 | 45,463 |
| **complex64** | N/A | 287,099 | 151,906 | 95,737 | 46,998 |
| **complex128** | N/A | 161,753 | 94,648 | 51,144 | N/A |
| **turboquant** | 481,406 | N/A | 240,132 | N/A | N/A |

### 3,000 Vectors

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 82,922 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 162,427 |
| **int8** | N/A | N/A | N/A | N/A | 258,256 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 656,311 | N/A | 220,066 | N/A | N/A |

### 5,000 Vectors

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 84,027 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 158,103 |
| **int8** | N/A | N/A | N/A | N/A | 272,520 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 588,091 | N/A | 277,720 | N/A | N/A |

### 7,000 Vectors

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 87,770 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 121,611 |
| **int8** | N/A | N/A | N/A | N/A | 278,807 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 628,453 | N/A | 254,785 | N/A | N/A |

### 10,000 Vectors

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 94,114 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 138,458 |
| **int8** | N/A | N/A | N/A | N/A | 263,322 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 15,000 Vectors

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 80,783 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 147,487 |
| **int8** | N/A | N/A | N/A | N/A | 237,624 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

## CPU Build - Ingest Performance (MB/Second)

### 1,000 Vectors - MB/s (Ingest)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | 562.9 | 626.7 | 915.5 | 978.9 |
| **float64** | N/A | 791.2 | 896.3 | 817.4 | 1,050.4 |
| **float16** | N/A | 414.8 | 648.2 | 741.0 | 964.6 |
| **int8** | N/A | 221.6 | 344.7 | 610.2 | 754.4 |
| **int16** | N/A | 333.1 | 586.3 | 778.3 | 877.3 |
| **int32** | N/A | 585.5 | 799.4 | 937.3 | 1,043.8 |
| **int64** | N/A | 795.3 | 989.9 | 903.4 | 1,065.2 |
| **uint8** | N/A | 234.5 | 351.8 | 531.4 | 776.9 |
| **uint16** | N/A | 360.1 | 624.8 | 675.0 | 977.8 |
| **uint32** | N/A | 653.8 | 779.2 | 953.0 | 1,055.7 |
| **uint64** | N/A | 774.0 | 946.1 | 1,076.0 | 1,065.5 |
| **complex64** | N/A | 841.1 | 890.1 | 1,121.9 | 1,101.5 |
| **complex128** | N/A | 947.8 | 1,109.2 | 1,198.7 | N/A |
| **turboquant** | 22.5 | N/A | 66.2 | N/A | N/A |

### 3,000 Vectors - MB/s (Ingest)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 971.7 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 951.7 |
| **int8** | N/A | N/A | N/A | N/A | 756.6 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 30.7 | N/A | 60.7 | N/A | N/A |

### 5,000 Vectors - MB/s (Ingest)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 984.7 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 926.4 |
| **int8** | N/A | N/A | N/A | N/A | 798.4 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 27.5 | N/A | 76.5 | N/A | N/A |

### 7,000 Vectors - MB/s (Ingest)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 1,028.6 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 712.6 |
| **int8** | N/A | N/A | N/A | N/A | 816.8 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 29.4 | N/A | 70.2 | N/A | N/A |

### 10,000 Vectors - MB/s (Ingest)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 1,102.9 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 811.3 |
| **int8** | N/A | N/A | N/A | N/A | 771.5 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 15,000 Vectors - MB/s (Ingest)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 946.7 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 864.2 |
| **int8** | N/A | N/A | N/A | N/A | 696.2 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

## CPU Build - Retrieve Performance (MB/Second)

### 1,000 Vectors - MB/s (Retrieve)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | 665.2 | 884.4 | 1,018.3 | 935.3 |
| **float64** | N/A | 638.3 | 705.7 | 806.4 | 851.9 |
| **float16** | N/A | 450.7 | 590.4 | 655.9 | 782.0 |
| **int8** | N/A | 352.5 | 458.4 | 605.8 | 903.2 |
| **int16** | N/A | 604.0 | 788.3 | 800.8 | 902.5 |
| **int32** | N/A | 713.3 | 611.4 | 812.9 | 808.0 |
| **int64** | N/A | 865.3 | 1,032.2 | 1,021.0 | 1,046.3 |
| **uint8** | N/A | 323.8 | 367.5 | 660.3 | 603.3 |
| **uint16** | N/A | 325.6 | 771.1 | 930.5 | 956.1 |
| **uint32** | N/A | 740.1 | 689.5 | 781.3 | 890.7 |
| **uint64** | N/A | 942.8 | 1,132.5 | 1,107.8 | 1,049.2 |
| **complex64** | N/A | 855.4 | 928.4 | 1,055.6 | 1,098.0 |
| **complex128** | N/A | 964.5 | 938.5 | 805.9 | N/A |
| **turboquant** | 90.8 | N/A | 194.9 | N/A | N/A |

### 3,000 Vectors - MB/s (Retrieve)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 910.0 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 1,020.7 |
| **int8** | N/A | N/A | N/A | N/A | 546.1 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 91.4 | N/A | 223.8 | N/A | N/A |

### 5,000 Vectors - MB/s (Retrieve)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 671.6 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 669.1 |
| **int8** | N/A | N/A | N/A | N/A | 636.0 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 76.0 | N/A | 208.1 | N/A | N/A |

### 7,000 Vectors - MB/s (Retrieve)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 1,049.3 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 650.3 |
| **int8** | N/A | N/A | N/A | N/A | 794.5 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 76.5 | N/A | 182.4 | N/A | N/A |

### 10,000 Vectors - MB/s (Retrieve)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 973.3 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 750.2 |
| **int8** | N/A | N/A | N/A | N/A | 683.3 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 15,000 Vectors - MB/s (Retrieve)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 1,057.3 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 713.4 |
| **int8** | N/A | N/A | N/A | N/A | 864.7 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

## CPU Build - Search Performance (QPS)

### 1,000 Vectors - Dense Search QPS

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | 2,007 | 1,577 | 964 | 584 |
| **float64** | N/A | 2,763 | 2,327 | 1,750 | 1,004 |
| **float16** | N/A | 2,637 | 2,212 | 1,597 | 987 |
| **int8** | N/A | 2,897 | 2,285 | 1,634 | 1,108 |
| **int16** | N/A | 5,977 | 4,877 | 2,927 | 1,796 |
| **int32** | N/A | 3,046 | 2,275 | 1,642 | 1,052 |
| **int64** | N/A | 6,047 | 5,052 | 2,953 | 1,801 |
| **uint8** | N/A | 2,907 | 2,357 | 1,681 | 1,085 |
| **uint16** | N/A | 6,107 | 4,423 | 3,147 | 1,847 |
| **uint32** | N/A | 2,764 | 2,158 | 1,597 | 1,113 |
| **uint64** | N/A | 5,693 | 4,257 | 2,945 | 1,789 |
| **complex64** | N/A | 1,448 | 987 | 572 | 339 |
| **complex128** | N/A | 2,398 | 1,632 | 982 | N/A |
| **turboquant** | 3,071 | N/A | 1,340 | N/A | N/A |

### 3,000 Vectors - Dense Search QPS

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 625 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 955 |
| **int8** | N/A | N/A | N/A | N/A | 1,058 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 3,104 | N/A | 1,639 | N/A | N/A |

### 5,000 Vectors - Dense Search QPS

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 618 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 1,007 |
| **int8** | N/A | N/A | N/A | N/A | 1,111 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 3,142 | N/A | 1,628 | N/A | N/A |

### 7,000 Vectors - Dense Search QPS

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 620 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 1,032 |
| **int8** | N/A | N/A | N/A | N/A | 1,061 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 3,102 | N/A | 1,631 | N/A | N/A |

### 10,000 Vectors - Dense Search QPS

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 609 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 972 |
| **int8** | N/A | N/A | N/A | N/A | 1,031 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 15,000 Vectors - Dense Search QPS

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 550 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 1,012 |
| **int8** | N/A | N/A | N/A | N/A | 1,149 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

## CPU Build - Search Latency (P50 ms)

### 1,000 Vectors - Dense Search Pp50 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | 0.472 | 0.604 | 1.005 | 1.677 |
| **float64** | N/A | 0.344 | 0.414 | 0.557 | 0.980 |
| **float16** | N/A | 0.363 | 0.440 | 0.607 | 0.987 |
| **int8** | N/A | 0.335 | 0.421 | 0.587 | 0.870 |
| **int16** | N/A | 0.159 | 0.197 | 0.337 | 0.545 |
| **int32** | N/A | 0.308 | 0.426 | 0.583 | 0.944 |
| **int64** | N/A | 0.151 | 0.184 | 0.326 | 0.541 |
| **uint8** | N/A | 0.330 | 0.415 | 0.579 | 0.909 |
| **uint16** | N/A | 0.156 | 0.222 | 0.307 | 0.531 |
| **uint32** | N/A | 0.344 | 0.451 | 0.604 | 0.870 |
| **uint64** | N/A | 0.167 | 0.220 | 0.327 | 0.553 |
| **complex64** | N/A | 0.659 | 0.995 | 1.725 | 2.951 |
| **complex128** | N/A | 0.400 | 0.598 | 0.991 | N/A |
| **turboquant** | 0.323 | N/A | 0.646 | N/A | N/A |

### 3,000 Vectors - Dense Search Pp50 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 1.588 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 1.039 |
| **int8** | N/A | N/A | N/A | N/A | 0.937 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 0.318 | N/A | 0.605 | N/A | N/A |

### 5,000 Vectors - Dense Search Pp50 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 1.611 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 0.982 |
| **int8** | N/A | N/A | N/A | N/A | 0.882 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 0.315 | N/A | 0.610 | N/A | N/A |

### 7,000 Vectors - Dense Search Pp50 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 1.598 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 0.962 |
| **int8** | N/A | N/A | N/A | N/A | 0.934 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 0.318 | N/A | 0.608 | N/A | N/A |

### 10,000 Vectors - Dense Search Pp50 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 1.624 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 1.015 |
| **int8** | N/A | N/A | N/A | N/A | 0.956 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 15,000 Vectors - Dense Search Pp50 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 1.763 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 0.977 |
| **int8** | N/A | N/A | N/A | N/A | 0.854 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

## CPU Build - Search Latency (P95 ms)

### 1,000 Vectors - Dense Search Pp95 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | 0.648 | 0.757 | 1.169 | 1.887 |
| **float64** | N/A | 0.481 | 0.565 | 0.704 | 1.166 |
| **float16** | N/A | 0.487 | 0.548 | 0.733 | 1.099 |
| **int8** | N/A | 0.453 | 0.547 | 0.815 | 1.028 |
| **int16** | N/A | 0.229 | 0.336 | 0.385 | 0.638 |
| **int32** | N/A | 0.424 | 0.535 | 0.722 | 1.072 |
| **int64** | N/A | 0.226 | 0.256 | 0.452 | 0.659 |
| **uint8** | N/A | 0.440 | 0.516 | 0.740 | 1.043 |
| **uint16** | N/A | 0.208 | 0.278 | 0.394 | 0.611 |
| **uint32** | N/A | 0.477 | 0.544 | 0.877 | 1.016 |
| **uint64** | N/A | 0.225 | 0.289 | 0.423 | 0.636 |
| **complex64** | N/A | 0.832 | 1.205 | 1.941 | 3.245 |
| **complex128** | N/A | 0.514 | 0.693 | 1.325 | N/A |
| **turboquant** | 0.360 | N/A | 1.533 | N/A | N/A |

### 3,000 Vectors - Dense Search Pp95 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 1.747 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 1.149 |
| **int8** | N/A | N/A | N/A | N/A | 1.077 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 0.357 | N/A | 0.667 | N/A | N/A |

### 5,000 Vectors - Dense Search Pp95 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 1.745 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 1.107 |
| **int8** | N/A | N/A | N/A | N/A | 1.051 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 0.351 | N/A | 0.671 | N/A | N/A |

### 7,000 Vectors - Dense Search Pp95 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 1.781 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 1.061 |
| **int8** | N/A | N/A | N/A | N/A | 1.045 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 0.356 | N/A | 0.667 | N/A | N/A |

### 10,000 Vectors - Dense Search Pp95 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 1.797 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 1.152 |
| **int8** | N/A | N/A | N/A | N/A | 1.116 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 15,000 Vectors - Dense Search Pp95 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 2.206 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 1.099 |
| **int8** | N/A | N/A | N/A | N/A | 0.992 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

## CPU Build - Search Latency (P99 ms)

### 1,000 Vectors - Dense Search Pp99 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | 0.734 | 1.038 | 1.398 | 2.134 |
| **float64** | N/A | 0.575 | 0.734 | 0.879 | 1.406 |
| **float16** | N/A | 0.588 | 0.657 | 0.897 | 2.008 |
| **int8** | N/A | 0.513 | 0.636 | 0.824 | 2.149 |
| **int16** | N/A | 0.264 | 0.350 | 0.499 | 0.740 |
| **int32** | N/A | 0.558 | 0.760 | 0.983 | 1.263 |
| **int64** | N/A | 0.364 | 0.348 | 0.559 | 0.794 |
| **uint8** | N/A | 0.502 | 0.558 | 0.826 | 1.143 |
| **uint16** | N/A | 0.264 | 0.351 | 0.471 | 0.676 |
| **uint32** | N/A | 0.620 | 0.786 | 0.937 | 1.242 |
| **uint64** | N/A | 0.310 | 0.443 | 0.518 | 0.792 |
| **complex64** | N/A | 1.097 | 1.511 | 2.186 | 3.461 |
| **complex128** | N/A | 0.704 | 0.913 | 1.400 | N/A |
| **turboquant** | 0.415 | N/A | 2.366 | N/A | N/A |

### 3,000 Vectors - Dense Search Pp99 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 2.095 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 1.443 |
| **int8** | N/A | N/A | N/A | N/A | 1.344 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 0.426 | N/A | 0.745 | N/A | N/A |

### 5,000 Vectors - Dense Search Pp99 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 2.040 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 1.274 |
| **int8** | N/A | N/A | N/A | N/A | 1.672 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 0.402 | N/A | 0.723 | N/A | N/A |

### 7,000 Vectors - Dense Search Pp99 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 2.124 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 1.257 |
| **int8** | N/A | N/A | N/A | N/A | 1.238 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | 0.407 | N/A | 0.716 | N/A | N/A |

### 10,000 Vectors - Dense Search Pp99 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 2.100 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 1.483 |
| **int8** | N/A | N/A | N/A | N/A | 1.338 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 15,000 Vectors - Dense Search Pp99 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | 2.926 |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | 1.475 |
| **int8** | N/A | N/A | N/A | N/A | 1.239 |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

---

## Metal Build - Ingest Performance (Vectors/Second)

### 1,000 Vectors

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 3,000 Vectors

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 5,000 Vectors

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 7,000 Vectors

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 10,000 Vectors

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 15,000 Vectors

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

## Metal Build - Ingest Performance (MB/Second)

### 1,000 Vectors - MB/s (Ingest)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 3,000 Vectors - MB/s (Ingest)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 5,000 Vectors - MB/s (Ingest)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 7,000 Vectors - MB/s (Ingest)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 10,000 Vectors - MB/s (Ingest)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 15,000 Vectors - MB/s (Ingest)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

## Metal Build - Retrieve Performance (MB/Second)

### 1,000 Vectors - MB/s (Retrieve)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 3,000 Vectors - MB/s (Retrieve)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 5,000 Vectors - MB/s (Retrieve)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 7,000 Vectors - MB/s (Retrieve)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 10,000 Vectors - MB/s (Retrieve)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 15,000 Vectors - MB/s (Retrieve)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

## Metal Build - Search Performance (QPS)

### 1,000 Vectors - Dense Search QPS

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 3,000 Vectors - Dense Search QPS

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 5,000 Vectors - Dense Search QPS

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 7,000 Vectors - Dense Search QPS

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 10,000 Vectors - Dense Search QPS

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 15,000 Vectors - Dense Search QPS

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

## Metal Build - Search Latency (P50 ms)

### 1,000 Vectors - Dense Search Pp50 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 3,000 Vectors - Dense Search Pp50 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 5,000 Vectors - Dense Search Pp50 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 7,000 Vectors - Dense Search Pp50 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 10,000 Vectors - Dense Search Pp50 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 15,000 Vectors - Dense Search Pp50 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

## Metal Build - Search Latency (P95 ms)

### 1,000 Vectors - Dense Search Pp95 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 3,000 Vectors - Dense Search Pp95 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 5,000 Vectors - Dense Search Pp95 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 7,000 Vectors - Dense Search Pp95 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 10,000 Vectors - Dense Search Pp95 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 15,000 Vectors - Dense Search Pp95 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

## Metal Build - Search Latency (P99 ms)

### 1,000 Vectors - Dense Search Pp99 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 3,000 Vectors - Dense Search Pp99 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 5,000 Vectors - Dense Search Pp99 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 7,000 Vectors - Dense Search Pp99 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 10,000 Vectors - Dense Search Pp99 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

### 15,000 Vectors - Dense Search Pp99 Latency (ms)

| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |
|-------|---------|---------|---------|----------|----------|
| **float32** | N/A | N/A | N/A | N/A | N/A |
| **float64** | N/A | N/A | N/A | N/A | N/A |
| **float16** | N/A | N/A | N/A | N/A | N/A |
| **int8** | N/A | N/A | N/A | N/A | N/A |
| **int16** | N/A | N/A | N/A | N/A | N/A |
| **int32** | N/A | N/A | N/A | N/A | N/A |
| **int64** | N/A | N/A | N/A | N/A | N/A |
| **uint8** | N/A | N/A | N/A | N/A | N/A |
| **uint16** | N/A | N/A | N/A | N/A | N/A |
| **uint32** | N/A | N/A | N/A | N/A | N/A |
| **uint64** | N/A | N/A | N/A | N/A | N/A |
| **complex64** | N/A | N/A | N/A | N/A | N/A |
| **complex128** | N/A | N/A | N/A | N/A | N/A |
| **turboquant** | N/A | N/A | N/A | N/A | N/A |

---

## GraphRAG Performance (Graph Spreading)

| Alpha | K | QPS | P50 (ms) | P95 (ms) | P99 (ms) |
|-------|---|-----|----------|----------|----------|

## Deletion Performance (Tombstone Operations)

| Total Vectors | Deleted | Delete Time (ms) | Search Time (ms) |
|---------------|---------|------------------|------------------|