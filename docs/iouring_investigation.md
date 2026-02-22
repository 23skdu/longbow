# io_uring Kernel Interaction Investigation Plan

## Problem Statement

The custom io_uring implementation in `internal/iouring/` passes basic ring setup tests but fails on I/O operation completion:

### Symptom Analysis

| Test | Expected | Actual | Analysis |
|------|----------|--------|----------|
| `TestSubmitAndComplete` | UserData: 0x2a, Res: 14 | UserData: 0x0, Res: 0 | CQE is all zeros |
| `TestSubmitRead` | UserData: 0x64, Res: 21 | UserData: 0x0, Res: 0 | CQE is all zeros |
| `TestSubmitWrite` | Res: 17 | Res: 0 | CQE is all zeros |
| `TestCqReady` | 1 | 0xffffffff (-1) | Head/Tail arithmetic overflow |

### Key Observations

1. **Ring setup succeeds** - `io_uring_setup` returns valid fd, mmap succeeds
2. **SQ submission succeeds** - `Submit()` returns nil, SQ tail increments
3. **Flush succeeds** - `io_uring_enter` returns without error
4. **CQE returns zeros** - Kernel appears to process but CQE fields are zero
5. **CqReady overflows** - Suggests head > tail after operations

---

## Investigation Phases

### Phase 1: Verify Params Struct Alignment ⚠️ HIGH PRIORITY

**Hypothesis**: The `Params`, `SqRingOffsets`, or `CqRingOffsets` structs don't match kernel ABI.

**Kernel Definition** (include/uapi/linux/io_uring.h):
```c
struct io_uring_params {
    __u32 sq_entries;
    __u32 cq_entries;
    __u32 flags;
    __u32 sq_thread_cpu;
    __u32 sq_thread_idle;
    __u32 features;
    __u32 wq_fd;
    __u32 resv[3];
    struct io_sqring_offsets sq_off;
    struct io_cqring_offsets cq_off;
};

struct io_sqring_offsets {
    __u32 head;
    __u32 tail;
    __u32 ring_mask;
    __u32 ring_entries;
    __u32 flags;
    __u32 dropped;
    __u32 array;
    __u32 resv[3];   // <-- 3 reserved fields!
};

struct io_cqring_offsets {
    __u32 head;
    __u32 tail;
    __u32 ring_mask;
    __u32 ring_entries;
    __u32 overflow;
    __u32 cqes;
    __u32 flags;
    __u32 resv[2];
};
```

**Current Go Definition** (`internal/iouring/types.go`):
```go
type SqRingOffsets struct {
    Head        uint32
    Tail        uint32
    RingMask    uint32
    RingEntries uint32
    Flags       uint32
    Dropped     uint32
    Array       uint32
    Resv1       uint32   // <-- Only 2 reserved fields!
    Resv2       uint32   // <-- Should be Resv[3]!
}
```

**Action Items**:
1. [ ] Fix `SqRingOffsets` to have 3 reserved fields (`Resv [3]uint32`)
2. [ ] Add struct size verification test
3. [ ] Add offset verification using `unsafe.Offsetof`

**Test Code**:
```go
func TestSqRingOffsetsSize(t *testing.T) {
    // Kernel expects 40 bytes (10 x uint32)
    expected := uint32(40)
    actual := uint32(unsafe.Sizeof(SqRingOffsets{}))
    assert.Equal(t, expected, actual, "SqRingOffsets size mismatch")
}
```

---

### Phase 2: Verify Memory Mapping Offsets ⚠️ HIGH PRIORITY

**Hypothesis**: The mmap offsets for SQ/CQ rings are incorrect.

**Current Implementation** (`ring.go`):
```go
const (
    IORING_OFF_SQ_RING uint64 = 0
    IORING_OFF_CQ_RING uint64 = 0x8000000   // 128 MB
    IORING_OFF_SQES    uint64 = 0x10000000   // 256 MB
)
```

**Verification Needed**:
1. [ ] Check if kernel uses different offsets based on `IORING_FEAT_SINGLE_MMAP`
2. [ ] Verify offset calculations match kernel expectations
3. [ ] Check if `IORING_SETUP_NO_MMAP` flag affects this

**Debug Test**:
```go
func TestMmapOffsets(t *testing.T) {
    ring, _ := NewRing(64, 0)
    defer ring.Close()
    
    t.Logf("Features: 0x%x", ring.params.Features)
    t.Logf("SQ offsets: head=%d, tail=%d, array=%d",
        ring.params.SqOffsets.Head,
        ring.params.SqOffsets.Tail,
        ring.params.SqOffsets.Array)
    t.Logf("CQ offsets: head=%d, tail=%d, cqes=%d",
        ring.params.CqOffsets.Head,
        ring.params.CqOffsets.Tail,
        ring.params.CqOffsets.Cqes)
}
```

---

### Phase 3: Debug CQE Read Path ⚠️ HIGH PRIORITY

**Hypothesis**: CQE pointer arithmetic is wrong, reading from incorrect memory.

**Current Implementation** (`cq.go`):
```go
func (r *Ring) Peek() *CQE {
    tail := atomic.LoadUint32(r.cqTail)
    head := atomic.LoadUint32(r.cqHead)
    
    if head == tail {
        return nil
    }
    
    index := head & r.cqRingMaskCached
    cqes := unsafe.Slice(r.cqes, r.cqEntriesCached)
    return &cqes[index]
}
```

**Potential Issues**:
1. `cqRingMaskCached` may be wrong
2. `cqes` pointer may be calculated incorrectly
3. Memory barrier missing between kernel write and user read

**Debug Test**:
```go
func TestCQEMemoryLayout(t *testing.T) {
    ring, _ := NewRing(64, 0)
    defer ring.Close()
    
    // Submit NOP
    ring.Submit(&SQE{Opcode: IORING_OP_NOP, Fd: -1, UserData: 0xDEADBEEF})
    ring.FlushAndWait(1, time.Second)
    
    // Raw memory dump
    cqBase := (*[1 << 20]byte)(unsafe.Pointer(&ring.cqRingArea[0]))
    
    head := atomic.LoadUint32(ring.cqHead)
    tail := atomic.LoadUint32(ring.cqTail)
    
    t.Logf("CQ head=%d, tail=%d, mask=%d", head, tail, ring.cqRingMaskCached)
    t.Logf("CQ offsets: head=%d, tail=%d, cqes=%d",
        ring.params.CqOffsets.Head,
        ring.params.CqOffsets.Tail,
        ring.params.CqOffsets.Cqes)
    
    // Dump raw CQE area
    cqesOffset := ring.params.CqOffsets.Cqes
    for i := 0; i < int(ring.cqEntriesCached); i++ {
        cqeOffset := cqesOffset + uint32(i*16) // CQE is 16 bytes
        if cqeOffset+16 <= uint32(len(ring.cqRingArea)) {
            cqe := (*CQE)(unsafe.Pointer(&cqBase[cqeOffset]))
            if cqe.UserData != 0 || cqe.Res != 0 {
                t.Logf("CQE[%d]: UserData=0x%x, Res=%d, Flags=0x%x",
                    i, cqe.UserData, cqe.Res, cqe.Flags)
            }
        }
    }
}
```

---

### Phase 4: Add Comprehensive Debug Logging

**Action Items**:
1. [ ] Add tracing to `ioUringSetup` - log all returned params
2. [ ] Add tracing to `ioUringEnter` - log submit count, return value
3. [ ] Add memory dump capability for SQ/CQ ring state
4. [ ] Add SQE submission verification

**Implementation**:
```go
// Add to syscall.go
var debugIOUring = os.Getenv("DEBUG_IOURING") != ""

func debugLog(format string, args ...any) {
    if debugIOUring {
        log.Printf("[iouring] "+format, args...)
    }
}
```

---

### Phase 5: Compare with Reference Implementation

**Reference Implementations to Study**:

1. **liburing** (C reference)
   - https://github.com/axboe/liburing
   - Focus on `setup.c`, `queue.c`, `register.c`

2. **go-linux-syscall**
   - Check if `golang.org/x/sys/unix` has io_uring support now
   - Compare struct definitions

3. **Other Go io_uring libraries**:
   - `github.com/iceber/iouring-go`
   - `github.com/stretchr/iouring-go` (if exists)

**Key Comparison Points**:
- [ ] Struct sizes match exactly?
- [ ] Mmap offsets match?
- [ ] SQE preparation matches?
- [ ] CQE reading matches?

---

### Phase 6: Kernel Version Compatibility

**Issue**: io_uring ABI has changed across kernel versions.

**Kernel Version Requirements**:
- 5.1: Initial io_uring support
- 5.3: IORING_OP_READ/WRITE (not READV/WRITEV)
- 5.5: Feature flags, CQ flags
- 5.6: Buffer selection, linked SQEs
- 5.7+: Various enhancements

**Action Items**:
1. [ ] Add kernel version detection
2. [ ] Add feature probing via `IORING_REGISTER_PROBE`
3. [ ] Conditionally disable features on older kernels

```go
func detectKernelVersion() (major, minor int, err error) {
    var uname unix.Utsname
    if err := unix.Uname(&uname); err != nil {
        return 0, 0, err
    }
    // Parse release string like "6.1.0-18-generic"
    // ...
}
```

---

## Quick Diagnostic Script

```bash
#!/bin/bash
# Run this to gather diagnostic info

echo "=== Kernel Version ==="
uname -r

echo -e "\n=== io_uring Support ==="
cat /proc/sys/kernel/io_uring_disabled 2>/dev/null || echo "io_uring_disabled not found"
grep -E "CONFIG_IO_URING|CONFIG_BLOCK" /boot/config-$(uname -r) 2>/dev/null || echo "kernel config not found"

echo -e "\n=== Test Results ==="
cd /home/rsd/REPOS/longbow
go test -v -timeout 30s ./internal/iouring/ 2>&1 | grep -E "PASS|FAIL|Error"

echo -e "\n=== Test with Debug ==="
DEBUG_IOURING=1 go test -v -run TestSubmitAndComplete ./internal/iouring/ 2>&1
```

---

## Most Likely Root Causes (Ranked)

1. **Struct alignment mismatch** (80% confidence)
   - `SqRingOffsets` has wrong number of reserved fields
   - This corrupts offset calculations for CQ ring

2. **Memory barrier issue** (15% confidence)
   - Missing `atomic` operations when reading CQE
   - Kernel writes not visible to Go code

3. **Wrong mmap offset/size** (5% confidence)
   - CQ ring mapped at wrong address
   - Reading from unallocated memory

---

## Next Steps

1. Run diagnostic script to gather baseline info
2. Fix `SqRingOffsets.Resv` to be `[3]uint32`
3. Add size verification tests
4. Re-run tests with debug logging enabled
5. If still failing, add memory dump analysis

---

*Created: February 22, 2026*
*Status: Investigation planned, awaiting implementation*
