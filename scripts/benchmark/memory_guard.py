"""Host memory detection, ceiling resolution, memory estimation, and soak runner."""

import os
import re
import time
from datetime import datetime

try:
    import numpy as np
except ImportError:
    np = None

try:
    from longbow import LongbowClient  # noqa: F401

    HAS_LONGBOW_SDK = True
except ImportError:
    HAS_LONGBOW_SDK = False

from .process import DTYPE_BYTES, ResourceExhaustedException


class MemoryExceededException(Exception):
    """Raised when estimated memory exceeds the configured limit."""


# Memory ceiling resolution (roadmap R14, H2/H3/H4, R14a).
#
# Three things were wrong with the ceiling before this. --memory was documented as
# "10GB" but typed as int bytes, so it could only be passed as 10737418240.
# LONGBOW_MAX_MEMORY is documented everywhere else as a size string, and
# int("18GB") raised ValueError, so --estimate-memory crashed whenever the
# variable was set. And start_server ignored both, reading a hardcoded 18 GiB, so
# the documented knob did nothing at all: on a 22 GiB host the default sat above
# the safe ceiling and the client was OOM-killed mid-indexing with no diagnostic.
#
# One parser, one resolution order, used everywhere.

_SIZE_UNITS = {
    "": 1,
    "B": 1,
    "K": 1024, "KB": 1024, "KIB": 1024,
    "M": 1024 ** 2, "MB": 1024 ** 2, "MIB": 1024 ** 2,
    "G": 1024 ** 3, "GB": 1024 ** 3, "GIB": 1024 ** 3,
    "T": 1024 ** 4, "TB": 1024 ** 4, "TIB": 1024 ** 4,
}

# Fraction of physical RAM used when neither --memory nor LONGBOW_MAX_MEMORY is
# given. The previous 18 GiB literal was an absolute constant that happened to be
# too high on a 22 GiB host and absurdly too low on a 512 GiB one.
DEFAULT_MEMORY_FRACTION = 0.60


def parse_size_bytes(value, default=None):
    """Parse a byte count or a size string into bytes.

    Accepts "10737418240", "10GB", "10GiB", " 10 gb ", "512M". Returns `default`
    when value is None, empty or unparseable, so a typo in an environment variable
    degrades to the default rather than aborting a multi-hour benchmark run.
    """
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return int(value)

    text = str(value).strip()
    if not text:
        return default

    # Split the numeric prefix from the unit suffix.
    i = 0
    while i < len(text) and (text[i].isdigit() or text[i] in ".+-"):
        i += 1
    number, unit = text[:i].strip(), text[i:].strip().upper()

    if not number:
        return default
    try:
        magnitude = float(number)
    except ValueError:
        return default

    multiplier = _SIZE_UNITS.get(unit)
    if multiplier is None:
        return default
    return int(magnitude * multiplier)


def format_size_gb(num_bytes) -> str:
    """Render a byte count as a human-readable GiB figure for logs."""
    if num_bytes is None:
        return "unset"
    return f"{num_bytes / (1024 ** 3):.1f} GB"


def host_total_memory_bytes():
    """Physical RAM, or None if it cannot be determined."""
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, OSError, AttributeError):
        pass
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    return None


def host_available_memory_bytes():
    """Memory available right now, or None.

    A ceiling above what is actually free is what produced the silent SIGKILLs, so
    the tier is checked against free memory as well as the configured ceiling.
    """
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    return None


def resolve_memory_limit_bytes(args):
    """Resolve the server memory ceiling in bytes.

    Precedence: --memory, then LONGBOW_MAX_MEMORY, then a fraction of host RAM.
    --memory is authoritative (R14) so that an explicit flag is never silently
    overridden by an inherited environment variable.
    """
    explicit = getattr(args, "memory", None)
    if explicit is not None and str(explicit).strip() != "":
        parsed = parse_size_bytes(explicit, default=None)
        if parsed is not None:
            return parsed

    from_env = parse_size_bytes(os.environ.get("LONGBOW_MAX_MEMORY"), default=None)
    if from_env is not None:
        return from_env

    total = host_total_memory_bytes()
    if total is None:
        # Nothing to go on; keep the previous default rather than inventing one.
        return 18 * 1024 ** 3
    fraction = getattr(args, "memory_default_fraction", None) or DEFAULT_MEMORY_FRACTION
    return int(total * fraction)



class MemoryGuardMixin:
    """Memory ceiling checks, disk usage checks, and memory soak execution."""

    def _measure_disk_usage(self, label):
        """Measure disk usage (MB) of vector store files for a benchmark run.

        When auto-spill is active (LONGBOW_AUTO_SPILL_DISK=true), the server may
        write spilled vectors and graph files to:
          <data_dir>/<label>/<dataset>_vectors.bin
          <data_dir>/<label>/<dataset>_graph.bin

        Returns total MB used, or 0 if no disk files found.
        """
        data_root = os.path.join(self.data_dir, label)
        if not os.path.isdir(data_root):
            return 0.0

        total_bytes = 0
        for root, dirs, files in os.walk(data_root):
            for f in files:
                if f.endswith("_vectors.bin") or f.endswith(".bin"):
                    fp = os.path.join(root, f)
                    try:
                        total_bytes += os.path.getsize(fp)
                    except OSError:
                        pass
        if total_bytes == 0:
            return 0.0

        mb = total_bytes / (1024.0 * 1024.0)
        print(f"  Disk usage: {mb:.1f} MB ({total_bytes} bytes)")
        return round(mb, 1)
    def _estimate_memory_usage(self, dtype, dim, count):
        """Estimate peak memory (MB) for a given config.

        Uses a conservative formula based on raw data size plus HNSW overhead.
        Skips configs where estimated memory exceeds the configured max.
        """
        bytes_per = DTYPE_BYTES.get(dtype, 4)
        raw_mb = dim * count * bytes_per / (1024.0 * 1024.0)
        hnsw_edges_mb = count * 2048 / (1024.0 * 1024.0)
        arrow_overhead = raw_mb * 2
        temp_scratch_mb = raw_mb * 4
        headroom_mb = 512.0
        total_mb = raw_mb + arrow_overhead + temp_scratch_mb + hnsw_edges_mb + headroom_mb
        return round(total_mb, 1)
    def _check_memory_limit(self, dtype, dim, count):
        """Check if estimated memory exceeds the configured limit.

        Returns True if the config fits within memory, False if it should be skipped.
        Prints a warning when skipping.
        """
        if not getattr(self.args, 'estimate_memory', False):
            return True
        limit_bytes = resolve_memory_limit_bytes(self.args)
        limit_gb = limit_bytes / (1024 ** 3)
        estimated_mb = self._estimate_memory_usage(dtype, dim, count)
        estimated_gb = estimated_mb / 1024.0
        if estimated_gb > limit_gb:
            print(f"  [MEMORY] Estimated memory {estimated_gb:.1f} GB exceeds limit {limit_gb:.1f} GB, skipping")
            return False
        print(f"  [MEMORY] Estimated {estimated_gb:.1f} GB / {limit_gb:.1f} GB limit — OK")
        return True
    def execute_memory_soak(self):
        """Memory soak test: insert vectors and monitor RSS over time.
        
        Monitors memory usage for the specified duration, reporting RSS at
        regular intervals to detect memory leaks or excessive growth.
        """
        if not HAS_LONGBOW_SDK:
            print("ERROR: longbow SDK not installed. Install with: pip install longbow")
            return

        print("=" * 80)
        print("MEMORY SOAK TEST")
        print("Started:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print("=" * 80)

        dims = [int(d) for d in self.args.dims.split(",")]
        dtypes = self.args.dtypes.split(",")
        counts = [int(c) for c in self.args.counts.split(",")]
        count = max(counts) if counts else 500000
        duration = self.args.soak_duration
        interval = max(30, duration // 120)  # Sample ~120 times

        dtype_map = {
            "float32": np.float32, "float64": np.float64, "float16": np.float16,
            "int8": np.int8, "int16": np.int16, "int32": np.int32, "int64": np.int64,
            "uint8": np.uint8, "uint16": np.uint16, "uint32": np.uint32, "uint64": np.uint64,
            "complex64": np.complex64, "complex128": np.complex128, "turboquant": np.float32,
        }

        for dtype in dtypes:
            for dim in dims:
                print(f"\n{'=' * 80}")
                print(f"Memory Soak: {dtype} dim={dim} count={count} duration={duration}s")
                print("=" * 80)

                label = f"soak_{dtype}_{dim}"
                env_overrides = {"LONGBOW_MAX_MEMORY": str(self.args.memory)}
                if not self.start_server(label, env_overrides=env_overrides):
                    print(f"  Failed to start server for {dtype} dim={dim}")
                    continue

                np_dtype = dtype_map.get(dtype, np.float32)
                vec_size = dim
                if "complex" in dtype:
                    vec_size = dim * 2

                # Insert vectors in batches
                batch_size = min(10000, count)
                inserted = 0
                try:
                    while inserted < count:
                        to_insert = min(batch_size, count - inserted)
                        vectors = np.random.randn(to_insert, vec_size).astype(np_dtype)
                        ids = list(range(inserted, inserted + to_insert))
                        self.client.insert_vectors(ids, vectors.tolist())
                        inserted += to_insert
                        print(f"  Inserted {inserted}/{count} vectors", end="\r")

                    print(f"\n  All {count} vectors inserted. Monitoring memory for {duration}s...")
                except Exception as e:
                    print(f"  Error during insertion: {e}")
                    self.stop_server()
                    continue

                # Monitor memory over time
                memory_samples = []
                start_time = time.time()
                peak_rss = 0.0

                while time.time() - start_time < duration:
                    elapsed = time.time() - start_time
                    rss_mb = 0.0
                    if self.server_pid:
                        try:
                            with open(f"/proc/{self.server_pid}/status", "r") as f:
                                for line in f:
                                    if line.startswith("VmRSS:"):
                                        rss_mb = float(line.split()[1]) / 1024.0
                                        break
                        except Exception:
                            pass

                    if rss_mb > peak_rss:
                        peak_rss = rss_mb

                    memory_samples.append({
                        "elapsed_s": round(elapsed, 1),
                        "rss_mb": round(rss_mb, 2),
                    })

                    if int(elapsed) % 60 == 0 and elapsed > 0:
                        print(f"  [{elapsed:.0f}s] RSS: {rss_mb:.1f} MB (peak: {peak_rss:.1f} MB)")

                    time.sleep(interval)

                self.stop_server()

                # Calculate statistics
                rss_values = [s["rss_mb"] for s in memory_samples]
                if len(rss_values) >= 2:
                    rss_mean = sum(rss_values) / len(rss_values)
                    rss_min = min(rss_values)
                    rss_max = max(rss_values)
                    rss_growth = rss_values[-1] - rss_values[0]

                    print(f"\n  Memory Soak Results ({dtype} dim={dim}):")
                    print(f"    Samples:    {len(memory_samples)}")
                    print(f"    RSS Mean:   {rss_mean:.1f} MB")
                    print(f"    RSS Min:    {rss_min:.1f} MB")
                    print(f"    RSS Max:    {rss_max:.1f} MB")
                    print(f"    RSS Growth: {rss_growth:+.1f} MB ({rss_growth/rss_values[0]*100 if rss_values[0] > 0 else 0:+.1f}%)")
                    print(f"    Peak RSS:   {peak_rss:.1f} MB")

                    self.results.append({
                        "dim": dim,
                        "dtype": dtype,
                        "count": count,
                        "mode": "memory_soak",
                        "duration_s": duration,
                        "samples": len(memory_samples),
                        "rss_mean_mb": round(rss_mean, 2),
                        "rss_min_mb": round(rss_min, 2),
                        "rss_max_mb": round(rss_max, 2),
                        "rss_growth_mb": round(rss_growth, 2),
                        "peak_rss_mb": round(peak_rss, 2),
                        "samples_detail": memory_samples,
                        "timestamp": datetime.now().isoformat(),
                    })
                else:
                    print("  Insufficient memory samples collected")
