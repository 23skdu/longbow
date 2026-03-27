# Golang Benchmark Tool

The `benchmark_tool` is a standalone client driver written in Go using the `apache/arrow-go/v18` library and the `SmartClient` SDK to perform performance evaluations without Python interpretation friction.

## Key Goals

- Remove Python allocation and dynamic typing overheads for small-batch throughput accuracy.
- Support strong scalar types matrix (`float32`, `int32`, `int16`, `int8`, etc) with exact memory layout verification.

---

## Building the Tool

Compile via standard Go toolchain into `bin/`:

```bash
go build -o bin/benchmark-tool ./benchmark_tool/
```

---

## Manual Execution

Flags are supported to configure scale, dimensions, and typed dimensions directly:

```bash
./bin/benchmark-tool \
    --uri grpc://127.0.0.1:3000 \
    --scale 5000 \
    --dim 128 \
    --dtype float32 \
    --dataset float_bench \
    --queries 100 \
    --json reports_go/res_f32_5000.json
```

### Supported Flags

| Flag | Description | Default |
|---|---|---|
| `--uri` | flight data node address | `grpc://127.0.0.1:3000` |
| `--scale` | total random vectors count to insert | `1000` |
| `--dim` | dimension length | `128` |
| `--dtype` | types: `float32`, `int32`, `int16`, `int8` | `float32` |
| `--dataset` | name of the uploaded dataset | `bench_go` |
| `--queries` | search query intervals to test after put | `1000` |
| `--json` | filepath to save results aggregate | `""` |

---

## Batch Orchestration (Incremental isolated)

Use `scripts/benchmark_tool_incremental.sh` to run fully isolated incremental cycles through type lists:

```bash
bash scripts/benchmark_tool_incremental.sh
```

Reports will populate under `reports_go/` mirroring individual setups.
