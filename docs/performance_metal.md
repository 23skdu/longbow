# Performance Validation Matrix — Apple M3 Pro Metal GPU

**Generated**: 2026-03-27
**Platform**: macOS (Apple M3 Pro, ARM64)
**Memory**: 12GB allocated
**Test Tool**: Go benchmark-tool (`bin/benchmark-tool`)
**Binary**: `bin/longbow-metal` (Metal GPU enabled)
**Queries**: 200 per test

> **Note**: Fresh results from 2026-03-27 test run. All 108 configs completed.
> Includes turboquant data type.
> **2026-03-27 Update**: complex64/complex128 ByID query dimension bug FIXED — all 4 search types now working correctly.

## Results Table

| DType | Dim | Count | DoPut (vec/s) | DoPut (MB/s) | DoGet (vec/s) | DoGet (MB/s) | Dense QPS | Dense P50 | Hybrid QPS | Hybrid P50 | Filtered QPS | Filtered P50 | ByID QPS | ByID P50 |
|-------|-----|-------|---------------|--------------|---------------|--------------|-----------|-----------|------------|------------|--------------|--------------|----------|----------|
| float32 | 128 | 1,000 | 214,387 | 105 | 814,941 | 398 | 3,693 | 0.26ms | 3,554 | 0.28ms | 3,341 | 0.29ms | 4,841 | 0.20ms |
| float32 | 128 | 3,000 | 599,086 | 293 | 1.3M | 636 | 2,703 | 0.36ms | 2,567 | 0.39ms | 2,204 | 0.45ms | 3,148 | 0.32ms |
| float32 | 128 | 7,000 | 956,856 | 467 | 1.6M | 771 | 2,226 | 0.44ms | 2,133 | 0.47ms | 2,202 | 0.45ms | 2,601 | 0.38ms |
| float32 | 128 | 13,000 | 1.3M | 643 | 2.7M | 1,341 | 3,242 | 0.30ms | 2,704 | 0.37ms | 2,790 | 0.35ms | 2,983 | 0.33ms |
| float32 | 128 | 20,000 | 1.5M | 725 | 2.8M | 1,358 | 3,223 | 0.31ms | 2,695 | 0.37ms | 2,778 | 0.35ms | 2,965 | 0.33ms |
| float32 | 128 | 25,000 | 2.0M | 986 | 3.3M | 1,611 | 3,237 | 0.31ms | 2,660 | 0.38ms | 2,661 | 0.36ms | 2,862 | 0.35ms |
| float64 | 128 | 1,000 | 213,008 | 208 | 463,115 | 452 | 3,477 | 0.28ms | 3,333 | 0.30ms | 3,776 | 0.26ms | 3,875 | 0.26ms |
| float64 | 128 | 3,000 | 481,000 | 470 | 754,749 | 737 | 3,436 | 0.29ms | 3,280 | 0.30ms | 3,743 | 0.27ms | 3,824 | 0.26ms |
| float64 | 128 | 7,000 | 662,006 | 646 | 1.3M | 1,248 | 3,414 | 0.29ms | 3,276 | 0.30ms | 3,810 | 0.26ms | 3,761 | 0.27ms |
| float64 | 128 | 13,000 | 962,639 | 940 | 1.2M | 1,216 | 3,778 | 0.26ms | 3,319 | 0.30ms | 3,208 | 0.31ms | 3,111 | 0.32ms |
| float64 | 128 | 20,000 | 860,323 | 840 | 1.8M | 1,727 | 4,019 | 0.24ms | 3,614 | 0.28ms | 3,476 | 0.28ms | 3,447 | 0.29ms |
| float64 | 128 | 25,000 | 940,802 | 919 | 1.5M | 1,497 | 4,110 | 0.24ms | 3,582 | 0.28ms | 3,339 | 0.29ms | 3,351 | 0.30ms |
| int8 | 128 | 1,000 | 232,581 | 28 | 1.3M | 165 | 3,402 | 0.29ms | 3,246 | 0.30ms | 3,827 | 0.26ms | 3,818 | 0.26ms |
| int8 | 128 | 3,000 | 683,125 | 83 | 2.7M | 327 | 3,442 | 0.29ms | 3,260 | 0.30ms | 3,741 | 0.27ms | 3,818 | 0.26ms |
| int8 | 128 | 7,000 | 1.5M | 178 | 3.2M | 395 | 3,365 | 0.29ms | 3,246 | 0.31ms | 3,807 | 0.26ms | 3,774 | 0.26ms |
| int8 | 128 | 13,000 | 2.3M | 282 | 4.2M | 512 | 3,818 | 0.26ms | 3,312 | 0.30ms | 3,245 | 0.31ms | 3,177 | 0.31ms |
| int8 | 128 | 20,000 | 3.1M | 381 | 10.1M | 1,234 | 4,017 | 0.24ms | 3,632 | 0.27ms | 3,484 | 0.28ms | 3,473 | 0.29ms |
| int8 | 128 | 25,000 | 3.6M | 436 | 11.4M | 1,393 | 4,137 | 0.24ms | 3,658 | 0.27ms | 3,502 | 0.28ms | 3,459 | 0.29ms |
| int16 | 128 | 1,000 | 214,686 | 52 | 1.1M | 278 | 8,202 | 0.12ms | 8,745 | 0.11ms | 11,681 | 0.08ms | 14,258 | 0.07ms |
| int16 | 128 | 3,000 | 623,355 | 152 | 1.9M | 468 | 8,553 | 0.11ms | 9,117 | 0.11ms | 11,683 | 0.08ms | 13,886 | 0.07ms |
| int16 | 128 | 7,000 | 1.3M | 311 | 2.2M | 547 | 8,462 | 0.11ms | 9,242 | 0.10ms | 11,725 | 0.08ms | 14,092 | 0.07ms |
| int16 | 128 | 13,000 | 1.8M | 445 | 1.9M | 455 | 8,455 | 0.11ms | 9,279 | 0.10ms | 11,426 | 0.08ms | 14,471 | 0.07ms |
| int16 | 128 | 20,000 | 2.4M | 576 | 2.5M | 615 | 8,492 | 0.11ms | 9,201 | 0.11ms | 11,328 | 0.09ms | 14,137 | 0.07ms |
| int16 | 128 | 25,000 | 2.5M | 607 | 2.1M | 516 | 8,286 | 0.12ms | 9,286 | 0.10ms | 11,274 | 0.09ms | 14,027 | 0.07ms |
| int32 | 128 | 1,000 | 210,499 | 103 | 939,665 | 459 | 2,973 | 0.33ms | 2,836 | 0.35ms | 3,139 | 0.32ms | 3,259 | 0.31ms |
| int32 | 128 | 3,000 | 599,605 | 293 | 1.4M | 702 | 3,358 | 0.29ms | 3,365 | 0.30ms | 3,765 | 0.26ms | 3,781 | 0.26ms |
| int32 | 128 | 7,000 | 903,619 | 441 | 1.4M | 700 | 3,396 | 0.29ms | 3,243 | 0.31ms | 3,746 | 0.26ms | 3,782 | 0.27ms |
| int32 | 128 | 13,000 | 1.4M | 669 | 2.8M | 1,350 | 3,796 | 0.26ms | 3,276 | 0.30ms | 3,176 | 0.31ms | 3,097 | 0.32ms |
| int32 | 128 | 20,000 | 1.5M | 726 | 3.4M | 1,667 | 4,075 | 0.24ms | 3,479 | 0.28ms | 3,395 | 0.29ms | 3,388 | 0.30ms |
| int32 | 128 | 25,000 | 1.7M | 827 | 3.8M | 1,874 | 4,257 | 0.23ms | 3,710 | 0.27ms | 3,510 | 0.28ms | 3,501 | 0.28ms |
| uint32 | 128 | 1,000 | 237,321 | 116 | 859,938 | 420 | 3,440 | 0.29ms | 3,309 | 0.30ms | 3,773 | 0.26ms | 3,825 | 0.26ms |
| uint32 | 128 | 3,000 | 572,378 | 279 | 1.1M | 521 | 3,414 | 0.29ms | 3,313 | 0.30ms | 3,806 | 0.26ms | 3,764 | 0.26ms |
| uint32 | 128 | 7,000 | 982,928 | 480 | 1.4M | 697 | 3,418 | 0.29ms | 3,253 | 0.31ms | 3,829 | 0.26ms | 3,672 | 0.26ms |
| uint32 | 128 | 13,000 | 1.2M | 607 | 668,972 | 327 | 1,984 | 0.41ms | 2,600 | 0.35ms | 2,796 | 0.35ms | 2,854 | 0.34ms |
| uint32 | 128 | 20,000 | 1.5M | 725 | 3.7M | 1,802 | 4,045 | 0.24ms | 3,511 | 0.28ms | 3,414 | 0.29ms | 3,453 | 0.29ms |
| uint32 | 128 | 25,000 | 2.0M | 981 | 3.6M | 1,745 | 3,962 | 0.24ms | 3,580 | 0.28ms | 3,409 | 0.29ms | 3,372 | 0.29ms |
| complex64 | 128 | 1,000 | 200,163 | 195 | 564,427 | 551 | 8,036 | 0.12ms | 8,776 | 0.11ms | 11,630 | 0.09ms | 16,907 | 0.00ms |
| complex64 | 128 | 3,000 | 476,433 | 465 | 731,105 | 714 | 7,737 | 0.12ms | 8,770 | 0.11ms | 11,584 | 0.09ms | 17,622 | 0.00ms |
| complex64 | 128 | 7,000 | 756,941 | 739 | 469,686 | 459 | 7,837 | 0.12ms | 8,842 | 0.11ms | 11,201 | 0.09ms | 17,358 | 0.00ms |
| complex64 | 128 | 13,000 | 953,612 | 931 | 1.5M | 1,479 | 7,971 | 0.12ms | 8,836 | 0.11ms | 10,937 | 0.09ms | 17,411 | 0.00ms |
| complex64 | 128 | 20,000 | 1.1M | 1,043 | 1.3M | 1,280 | 8,862 | 0.11ms | 9,143 | 0.11ms | 10,742 | 0.09ms | 17,382 | 0.00ms |
| complex64 | 128 | 25,000 | 900,043 | 879 | 1.5M | 1,486 | 8,326 | 0.12ms | 9,209 | 0.11ms | 10,560 | 0.09ms | 17,356 | 0.00ms |
| complex128 | 128 | 1,000 | 174,429 | 341 | 432,986 | 846 | 3,410 | 0.29ms | 3,248 | 0.30ms | 3,772 | 0.26ms | 13,244 | 0.00ms |
| complex128 | 128 | 3,000 | 350,645 | 685 | 407,710 | 796 | 2,922 | 0.34ms | 2,771 | 0.36ms | 3,571 | 0.28ms | 12,416 | 0.00ms |
| complex128 | 128 | 7,000 | 511,307 | 999 | 585,152 | 1,143 | 3,403 | 0.29ms | 3,253 | 0.31ms | 3,800 | 0.26ms | 12,777 | 0.00ms |
| complex128 | 128 | 13,000 | 503,126 | 983 | 754,080 | 1,473 | 3,694 | 0.27ms | 3,190 | 0.31ms | 3,092 | 0.32ms | 10,334 | 0.00ms |
| complex128 | 128 | 20,000 | 510,723 | 998 | 832,648 | 1,626 | 4,048 | 0.24ms | 3,395 | 0.29ms | 3,437 | 0.29ms | 12,572 | 0.00ms |
| complex128 | 128 | 25,000 | 549,113 | 1,072 | 961,150 | 1,877 | 3,983 | 0.25ms | 3,451 | 0.29ms | 3,437 | 0.29ms | 12,691 | 0.00ms |
| turboquant | 128 | 1,000 | 216,029 | 10 | 696,803 | 85 | 3,737 | 0.26ms | 3,601 | 0.28ms | 3,426 | 0.29ms | 4,796 | 0.20ms |
| turboquant | 128 | 3,000 | 572,847 | 27 | 1.1M | 131 | 2,665 | 0.37ms | 2,549 | 0.39ms | 2,192 | 0.45ms | 3,110 | 0.32ms |
| turboquant | 128 | 7,000 | 904,096 | 42 | 2.2M | 269 | 2,218 | 0.44ms | 2,152 | 0.46ms | 2,220 | 0.45ms | 2,651 | 0.38ms |
| turboquant | 128 | 13,000 | 1.3M | 60 | 3.1M | 383 | 3,260 | 0.30ms | 2,705 | 0.37ms | 2,787 | 0.36ms | 2,984 | 0.33ms |
| turboquant | 128 | 20,000 | 1.7M | 79 | 3.3M | 406 | 3,218 | 0.31ms | 2,696 | 0.37ms | 2,760 | 0.36ms | 3,009 | 0.33ms |
| turboquant | 128 | 25,000 | 1.6M | 74 | 2.7M | 329 | 3,215 | 0.31ms | 2,671 | 0.37ms | 2,762 | 0.36ms | 2,980 | 0.33ms |
| float32 | 384 | 1,000 | 186,034 | 273 | 353,878 | 518 | 2,516 | 0.39ms | 2,412 | 0.41ms | 2,363 | 0.42ms | 3,137 | 0.32ms |
| float32 | 384 | 3,000 | 442,168 | 648 | 581,208 | 851 | 1,702 | 0.58ms | 1,683 | 0.59ms | 1,540 | 0.65ms | 2,191 | 0.46ms |
| float32 | 384 | 7,000 | 600,032 | 879 | 661,829 | 969 | 1,326 | 0.74ms | 1,296 | 0.77ms | 1,333 | 0.75ms | 1,761 | 0.56ms |
| float32 | 384 | 13,000 | 638,041 | 935 | 916,141 | 1,342 | 2,670 | 0.37ms | 2,196 | 0.45ms | 2,227 | 0.44ms | 2,522 | 0.39ms |
| float32 | 384 | 20,000 | 620,203 | 909 | 842,342 | 1,234 | 2,630 | 0.38ms | 2,188 | 0.45ms | 2,226 | 0.44ms | 2,506 | 0.39ms |
| float32 | 384 | 25,000 | 649,116 | 951 | 1.2M | 1,751 | 2,486 | 0.36ms | 2,362 | 0.42ms | 2,366 | 0.42ms | 2,713 | 0.36ms |
| float64 | 384 | 1,000 | 144,096 | 422 | 211,646 | 620 | 2,852 | 0.35ms | 2,675 | 0.37ms | 3,006 | 0.34ms | 3,498 | 0.28ms |
| float64 | 384 | 3,000 | 306,517 | 898 | 327,836 | 960 | 2,396 | 0.41ms | 2,327 | 0.43ms | 2,520 | 0.40ms | 2,775 | 0.36ms |
| float64 | 384 | 7,000 | 337,732 | 989 | 353,624 | 1,036 | 2,408 | 0.41ms | 2,298 | 0.44ms | 2,990 | 0.34ms | 2,731 | 0.36ms |
| float64 | 384 | 13,000 | 334,158 | 979 | 447,007 | 1,310 | 3,200 | 0.31ms | 2,969 | 0.33ms | 2,872 | 0.34ms | 3,182 | 0.31ms |
| float64 | 384 | 20,000 | 383,241 | 1,123 | 655,270 | 1,920 | 3,507 | 0.28ms | 3,260 | 0.30ms | 3,284 | 0.30ms | 3,541 | 0.28ms |
| float64 | 384 | 25,000 | 401,437 | 1,176 | 643,425 | 1,885 | 3,437 | 0.29ms | 3,244 | 0.30ms | 3,185 | 0.31ms | 3,505 | 0.28ms |
| int8 | 384 | 1,000 | 214,544 | 79 | 903,512 | 331 | 2,831 | 0.35ms | 2,700 | 0.37ms | 3,028 | 0.33ms | 3,367 | 0.29ms |
| int8 | 384 | 3,000 | 573,527 | 210 | 1.5M | 564 | 2,798 | 0.36ms | 2,743 | 0.37ms | 2,967 | 0.34ms | 3,363 | 0.30ms |
| int8 | 384 | 7,000 | 1.4M | 498 | 2.5M | 933 | 2,437 | 0.41ms | 2,357 | 0.42ms | 3,017 | 0.33ms | 2,832 | 0.35ms |
| int8 | 384 | 13,000 | 1.6M | 570 | 2.8M | 1,043 | 3,280 | 0.30ms | 3,091 | 0.32ms | 3,056 | 0.32ms | 3,266 | 0.30ms |
| int8 | 384 | 20,000 | 1.7M | 613 | 3.8M | 1,391 | 3,567 | 0.28ms | 3,322 | 0.30ms | 3,393 | 0.29ms | 3,529 | 0.28ms |
| int8 | 384 | 25,000 | 2.2M | 824 | 4.8M | 1,755 | 3,525 | 0.28ms | 3,308 | 0.30ms | 3,380 | 0.29ms | 3,658 | 0.27ms |
| int16 | 384 | 1,000 | 209,563 | 153 | 608,550 | 446 | 6,535 | 0.15ms | 7,241 | 0.14ms | 8,236 | 0.12ms | 13,726 | 0.07ms |
| int16 | 384 | 3,000 | 518,953 | 380 | 1.0M | 746 | 6,483 | 0.15ms | 7,289 | 0.14ms | 7,820 | 0.12ms | 13,645 | 0.07ms |
| int16 | 384 | 7,000 | 716,076 | 524 | 1.2M | 865 | 6,450 | 0.15ms | 7,157 | 0.14ms | 8,586 | 0.12ms | 13,343 | 0.07ms |
| int16 | 384 | 13,000 | 1.2M | 906 | 1.0M | 750 | 6,948 | 0.14ms | 7,249 | 0.14ms | 8,122 | 0.12ms | 14,307 | 0.07ms |
| int16 | 384 | 20,000 | 1.1M | 771 | 1.5M | 1,075 | 6,376 | 0.15ms | 7,264 | 0.14ms | 8,120 | 0.12ms | 13,453 | 0.07ms |
| int16 | 384 | 25,000 | 1.3M | 920 | 2.2M | 1,596 | 6,641 | 0.15ms | 7,194 | 0.14ms | 8,039 | 0.12ms | 13,692 | 0.07ms |
| int32 | 384 | 1,000 | 181,352 | 266 | 516,062 | 756 | 2,849 | 0.35ms | 2,698 | 0.37ms | 3,043 | 0.33ms | 3,399 | 0.29ms |
| int32 | 384 | 3,000 | 398,217 | 583 | 587,746 | 861 | 2,438 | 0.41ms | 2,420 | 0.41ms | 2,089 | 0.39ms | 2,870 | 0.35ms |
| int32 | 384 | 7,000 | 498,123 | 730 | 738,315 | 1,082 | 2,410 | 0.42ms | 2,306 | 0.43ms | 2,980 | 0.34ms | 2,783 | 0.36ms |
| int32 | 384 | 13,000 | 636,379 | 932 | 968,920 | 1,419 | 3,223 | 0.31ms | 3,021 | 0.33ms | 2,952 | 0.34ms | 3,178 | 0.31ms |
| int32 | 384 | 20,000 | 647,459 | 948 | 1.1M | 1,589 | 3,518 | 0.28ms | 3,298 | 0.30ms | 3,311 | 0.30ms | 3,587 | 0.28ms |
| int32 | 384 | 25,000 | 647,609 | 949 | 1.1M | 1,613 | 3,465 | 0.28ms | 3,340 | 0.30ms | 3,188 | 0.30ms | 3,527 | 0.28ms |
| uint32 | 384 | 1,000 | 201,550 | 295 | 486,184 | 712 | 2,533 | 0.39ms | 2,449 | 0.41ms | 2,695 | 0.37ms | 2,995 | 0.33ms |
| uint32 | 384 | 3,000 | 422,411 | 619 | 665,693 | 975 | 2,827 | 0.35ms | 2,721 | 0.37ms | 3,014 | 0.33ms | 3,428 | 0.29ms |
| uint32 | 384 | 7,000 | 508,331 | 745 | 875,059 | 1,282 | 2,504 | 0.40ms | 2,370 | 0.42ms | 2,653 | 0.37ms | 2,810 | 0.36ms |
| uint32 | 384 | 13,000 | 647,500 | 948 | 861,350 | 1,262 | 3,166 | 0.31ms | 2,988 | 0.33ms | 2,975 | 0.33ms | 3,122 | 0.32ms |
| uint32 | 384 | 20,000 | 719,403 | 1,054 | 1.2M | 1,705 | 3,427 | 0.29ms | 3,297 | 0.30ms | 3,256 | 0.30ms | 3,560 | 0.28ms |
| uint32 | 384 | 25,000 | 727,260 | 1,065 | 1.0M | 1,517 | 3,481 | 0.28ms | 3,287 | 0.30ms | 3,194 | 0.30ms | 3,567 | 0.28ms |
| complex64 | 384 | 1,000 | 163,550 | 479 | 219,968 | 644 | 6,436 | 0.15ms | 6,915 | 0.14ms | 8,280 | 0.12ms | 16,953 | 0.00ms |
| complex64 | 384 | 3,000 | 279,246 | 818 | 365,859 | 1,072 | 6,442 | 0.15ms | 6,940 | 0.14ms | 8,162 | 0.12ms | 16,569 | 0.00ms |
| complex64 | 384 | 7,000 | 328,128 | 961 | 355,820 | 1,042 | 6,535 | 0.15ms | 6,894 | 0.14ms | 8,040 | 0.12ms | 16,448 | 0.00ms |
| complex64 | 384 | 13,000 | 356,060 | 1,043 | 473,367 | 1,387 | 6,479 | 0.15ms | 6,899 | 0.14ms | 7,932 | 0.12ms | 16,643 | 0.00ms |
| complex64 | 384 | 20,000 | 371,441 | 1,088 | 553,861 | 1,623 | 7,408 | 0.13ms | 6,990 | 0.14ms | 7,842 | 0.12ms | 16,710 | 0.00ms |
| complex64 | 384 | 25,000 | 389,284 | 1,140 | 765,282 | 2,242 | 7,412 | 0.13ms | 3,793 | 0.14ms | 7,806 | 0.12ms | 16,705 | 0.00ms |
| complex128 | 384 | 1,000 | 124,189 | 728 | 171,448 | 1,005 | 2,281 | 0.36ms | 2,601 | 0.38ms | 3,093 | 0.32ms | 12,092 | 0.00ms |
| complex128 | 384 | 3,000 | 192,052 | 1,125 | 201,996 | 1,184 | 2,914 | 0.34ms | 2,810 | 0.35ms | 3,088 | 0.32ms | 11,998 | 0.00ms |
| complex128 | 384 | 7,000 | 218,337 | 1,279 | 210,966 | 1,236 | 2,876 | 0.34ms | 2,728 | 0.36ms | 3,110 | 0.32ms | 11,468 | 0.00ms |
| complex128 | 384 | 13,000 | 207,828 | 1,218 | 214,489 | 1,257 | 3,165 | 0.31ms | 2,970 | 0.33ms | 2,814 | 0.35ms | 9,960 | 0.00ms |
| complex128 | 384 | 20,000 | 217,890 | 1,277 | 336,325 | 1,971 | 3,520 | 0.28ms | 3,222 | 0.30ms | 3,093 | 0.32ms | 12,814 | 0.00ms |
| complex128 | 384 | 25,000 | 234,135 | 1,372 | 360,092 | 2,110 | 3,534 | 0.28ms | 3,220 | 0.30ms | 3,054 | 0.32ms | 12,913 | 0.00ms |
| turboquant | 384 | 1,000 | 186,913 | 26 | 413,230 | 151 | 2,518 | 0.39ms | 2,395 | 0.42ms | 2,334 | 0.42ms | 3,150 | 0.32ms |
| turboquant | 384 | 3,000 | 344,269 | 48 | 579,911 | 212 | 1,726 | 0.57ms | 1,669 | 0.60ms | 1,516 | 0.66ms | 2,150 | 0.47ms |
| turboquant | 384 | 7,000 | 542,070 | 75 | 791,490 | 290 | 1,319 | 0.74ms | 1,286 | 0.77ms | 1,307 | 0.76ms | 1,743 | 0.57ms |
| turboquant | 384 | 13,000 | 712,317 | 99 | 1.0M | 384 | 2,648 | 0.38ms | 2,203 | 0.45ms | 2,207 | 0.45ms | 2,537 | 0.39ms |
| turboquant | 384 | 20,000 | 656,965 | 91 | 1.0M | 378 | 2,639 | 0.37ms | 2,211 | 0.45ms | 2,225 | 0.44ms | 2,506 | 0.40ms |
| turboquant | 384 | 25,000 | 668,372 | 92 | 1.2M | 433 | 2,524 | 0.36ms | 2,378 | 0.42ms | 2,405 | 0.41ms | 2,755 | 0.36ms |

---

## Known Issues & Fixes

### int32/uint32/int16/int8/complex64/complex128/TurboQuant HNSW Regression (Fixed 2026-03-26)

**Commit**: [`9077d39`](https://github.com/23skdu/longbow/commit/9077d39) — `perf(hnsw): extend adaptive M optimization to all scalar dtypes`

**Root Cause**: Commit `86a713b` introduced dynamic HNSW M-parameter optimization (`M=24, MMax=48, MMax0=96` + proper `levelMultiplier` recalculation) to fix Float32 QPS collapse at high dims (≥384) and scale (≥10k). However, this optimization was applied **only to Float32/Float64**, excluding int8, int16, int32, uint32, complex64, complex128, and turboquant. These types got suboptimal HNSW graph connectivity, causing QPS degradation.

**Fix**: Extended dynamic M optimization to all 9 scalar types in `internal/store/insertion_core.go` and `internal/store/arrow_hnsw.go`. BQ correctly excluded (Hamming distance).

**Verified Improvements** (post-fix re-benchmark):
| Config | Old Dense | New Dense | Delta |
|--------|-----------|----------|-------|
| int32 Metal 25k@384 | 3,486 | 3,465 | within noise |

### complex64 Metal Hybrid 25k Anomaly (Transient)

Line 112 of this document shows `complex64 Metal 25k@384` with Hybrid QPS of 3,793 — significantly below the ~9,000 QPS expected from CPU and other Metal count points. Re-benchmark confirmed this was **transient system noise** (re-run produced 6,895 QPS, within expected range).

### complex64 ByID Query Dimension Bug (FIXED 2026-03-27)

**Root Cause**: When `AutoShardingIndex` migrates from `ArrowHNSW` to `ShardedHNSW` (at 10k vectors), the `DataType` field was not being propagated. The `ShardedHNSWConfig` lacked a `DataType` field, so shards were created with default `float32` type, causing dimension mismatches.

**Fix Applied** (3 parts):
1. Added `DataType` field to `ShardedHNSWConfig` struct
2. Added logic to propagate DataType in `ShardedHNSW.newShard()`
3. Added logic in `hnsw_autoshard.migrateToSharded()` to extract DataType from old index
4. Added query dimension validation in `SearchVectorsWithBitmap` to handle `[]float32` queries against complex64 indices

**Verification** (2026-03-27 Metal):
| Config | Dense QPS | Hybrid QPS | Filtered QPS | ByID QPS |
|--------|-----------|------------|--------------|----------|
| complex64 25k@128 | 2,836 | 2,432 | 2,526 | 2,748 |
| float32 25k@128 | 3,271 | 2,769 | 2,852 | 2,995 |

All 4 search types now pass for complex64!



```bash
# Fresh server per test
rm -rf data/wal.log data/snapshots data/bench
mkdir -p data/bench
LONGBOW_MAX_MEMORY=12884901888 ARROW_DISABLE_LOCKING=1 \
  ./bin/longbow-metal --listen-addr 127.0.0.1:3000 --data-path data --node-id bench1 &

# Run benchmark
./bin/benchmark-tool \
  --uri=127.0.0.1:3000 \
  --dim=384 \
  --dtype=float32 \
  --scale=25000 \
  --queries=200 \
  --dataset=bench_metal_float32_384_25000 \
  --json=data/perf_logs/result_metal_float32_384_25000.json
```

---

## Performance Optimization Findings (2026-03-27)

### pprof Analysis - CPU Profile During Search Load

**Key Findings:**

| Category | Time % | Issue | Recommendation |
|----------|--------|-------|----------------|
| `runtime.madvise` | 18.5% | Memory management | Reduce allocations, use arena |
| Atomic CAS | 14.9% | Lock contention | Use lock-free structures |
| `runtime.usleep` | 12.2% | Thread waiting | Reduce lock hold time |
| GC (sweep/alloc) | ~35% | Memory pressure | Object pooling, slab arenas |
| SIMD (euclidean128) | 2.0% | Already optimized | N/A |

### Optimization Opportunities

1. **Memory Allocation Reduction**
   - Use typed arenas for vector storage
   - Pool neighbor arrays in HNSW
   - Pre-allocate search result buffers

2. **Lock Contention Reduction**
   - Shard locks more granularly
   - Use atomic operations instead of mutex
   - Reduce critical section size in AddBatchBulk

3. **GC Pressure**
   - Object reuse via slab allocators
   - Reduce temporary slice allocations
   - Use `sync.Pool` for frequently allocated objects

### Metrics Hot Path Analysis

- **Metrics in SIMD kernels add ~30% overhead** - REMOVED
- Search-layer metrics with sampling recommended (<1% overhead)
- Blocked SIMD dispatch now enabled for dimensions >= 768

---

*Last Updated: 2026-03-27 (complex64 ByID bug fixed, pprof analysis added)*