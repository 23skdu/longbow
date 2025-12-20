---
description: how to run and track benchmarks for performance regression detection
---

To ensure no performance regressions are introduced with new features, follow these steps:

1. **Run Current Benchmarks**
   Run the benchmark suite and save the results to `fresh_benchmarks.txt`.
   ```bash
   go test -bench=. -benchmem -count=5 ./internal/store | tee fresh_benchmarks.txt
   ```

2. **Compare with Baseline**
   Use `benchstat` to compare the new results against the `baseline_benchmarks.txt`.
   ```bash
   benchstat baseline_benchmarks.txt fresh_benchmarks.txt
   ```

3. **Analyze Results**
   - Look for any `+N.N%` changes in the `sec/op` column, which indicate a slowdown.
   - If a regression > 10% is detected, investigate the cause before merging.

4. **Update Baseline (If regression is expected/accepted)**
   If the performance change is understood and accepted, promote the fresh benchmarks to the new baseline.
   ```bash
   cp fresh_benchmarks.txt baseline_benchmarks.txt
   ```
