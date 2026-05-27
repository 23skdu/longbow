# Next Steps & Production Validation

This document outlines the critical active optimizations, validation roadmap, and priority tasks for the Longbow vector search engine, following the successful resolution of all initial production blockers.

---

## 🎯 Active Priorities & Future Roadmap

### P1 — Hardware-Specific Remote Validation & Tuning

#### 1. NVIDIA CUDA Baseline & Performance Sweeps
* **Status**: In Progress `[/]`
* **Task**: Execute the unified benchmark suite on the remote `ancalagon` system to collect complete CUDA GPU performance baselines and throughput curves across dimensions, quantization, and scale configurations.

#### 2. Kernel Overhead Analysis & Latency Profiling
* **Status**: Not Started `[ ]`
* **Task**: Profile CUDA GPU thread-group sizes and launch latency under maximum QPS on the target NVIDIA architecture to isolate performance gaps and optimize kernel execution efficiency.

---

### P2 — Production Scale Optimization

#### 1. Buffer Eviction & VRAM Management
* **Status**: Not Started `[ ]`
* **Task**: Optimize dynamic buffer eviction policies for continuous stream workloads where index scale exceeds physical GPU VRAM, ensuring seamless paging between main memory and GPU memory.
