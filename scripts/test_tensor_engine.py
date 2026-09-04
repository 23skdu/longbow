#!/usr/bin/env python3
"""
scripts/test_tensor_engine.py
=============================
Comprehensive Test Suite & Verification Harness for Longbow Tensor Engine.

Tests all features of the tensor engine across 9 core domains:
  1. Core Tensors & Data Types (Float32, Float64, Complex64/128, Int64/32/8, Uint8, Strides, Slicing, Clones)
  2. Elementwise Operations & Special Functions (Add, Sub, Mul, Div, Pow, Sin, Cos, Tan, Exp, Log, Sqrt, Sinh, Cosh, Tanh, Erf, Neg)
  3. Linear Algebra & Contractions (MatMul 2D, Dot Product, Outer Product, TensorContract)
  4. Einstein Summation Engine (Einsum: matmul, dot, outer, transpose, diagonal, trace, multi-tensor chain, path optimizer)
  5. Computational DAG & Optimizer (IR Graph, CSE, Constant Folding, Algebraic Rewrites: A*0->0, A+0->A, -(-A)->A, T(T(A))->A)
  6. Relativistic & Differential Geometry Calculus (3D/4D Levi-Civita with parity, Metric Inversion, Index Raising/Lowering, Invariant 4-Momentum, Christoffel Symbols, Riemann Curvature, Ricci Tensor, Ricci Scalar, Differential Forms Wedge Product)
  7. Multi-Dtype Contractions (Float64, Complex128, Int64)
  8. Hardware Acceleration & Math Dispatch (AVX2 SIMD GEMM, Fast Math Dispatch vs Pure Go Fallback)
  9. Fuzzing & Microbenchmarks (Einsum parser, broadcast fuzzers, GEMM microbenchmarks)

Usage:
  python3 scripts/test_tensor_engine.py [options]

Options:
  --category <name>     Run only a specific category: all, core, ops, linalg, einsum, optimizer, calculus, multidtype, hardware, fuzz
  --bench               Run tensor microbenchmarks
  --fuzz                Run Go fuzz targets
  --unit-only           Run only package unit tests
  --verify-only         Run only deep verification tests
  --json-report <path>  Write full JSON execution report to specified file
  --no-color            Disable terminal ANSI color formatting
  --verbose, -v         Enable detailed output for every test
"""

import argparse
import datetime
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TENSOR_PKG = "./internal/tensor/..."
VERIFY_CMD = "./cmd/tensor-verify"

# ANSI Colors
class Colors:
    def __init__(self, enabled: bool = True):
        self.enabled = enabled

    def _c(self, code: str, text: str) -> str:
        return f"\033[{code}m{text}\033[0m" if self.enabled else text

    def bold(self, text: str) -> str:
        return self._c("1", text)

    def dim(self, text: str) -> str:
        return self._c("2", text)

    def green(self, text: str) -> str:
        return self._c("32", text)

    def red(self, text: str) -> str:
        return self._c("31", text)

    def yellow(self, text: str) -> str:
        return self._c("33", text)

    def blue(self, text: str) -> str:
        return self._c("34", text)

    def cyan(self, text: str) -> str:
        return self._c("36", text)

    def magenta(self, text: str) -> str:
        return self._c("35", text)


def get_go_version() -> str:
    try:
        res = subprocess.run(["go", "version"], capture_output=True, text=True, check=True)
        return res.stdout.strip()
    except Exception:
        return "Unknown Go"


def get_cpu_info() -> str:
    try:
        if sys.platform.startswith("linux"):
            with open("/proc/cpuinfo", "r") as f:
                for line in f:
                    if "model name" in line:
                        return line.split(":", 1)[1].strip()
        elif sys.platform == "darwin":
            res = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True)
            return res.stdout.strip()
    except Exception:
        pass
    return platform.processor() or platform.machine()


def run_command(cmd: List[str], cwd: str = REPO_ROOT) -> Tuple[int, str, str, float]:
    start = time.perf_counter()
    p = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    elapsed = time.perf_counter() - start
    return p.returncode, p.stdout, p.stderr, elapsed


def run_unit_tests(category_filter: str, verbose: bool, c: Colors) -> Dict[str, Any]:
    print(f"\n{c.bold(c.cyan('==> 1. Running Go Package Unit Tests (internal/tensor)...'))}")
    cmd = ["go", "test", "-json", TENSOR_PKG]
    code, stdout, stderr, elapsed = run_command(cmd)

    unit_results = []
    category_map = {
        "TestNewTensor": "core", "TestNewFromData": "core", "TestCloneIndependence": "core",
        "TestReshape": "core", "TestReshapeFunction": "core", "TestAt": "core", "TestLabels": "core",
        "TestDtypeSize": "core", "TestDtypePromote": "core", "TestDtypeClassification": "core",
        "TestSetLabelsMismatchPanic": "core", "TestAdd": "ops", "TestBroadcast": "ops",
        "TestNeg": "ops", "TestSinCos": "ops", "TestExpLogSqrtPow": "ops",
        "TestSinhCoshTanhErf": "ops", "TestReduceSum": "ops", "TestReduceSumNonFloat32": "ops",
        "TestBroadcastShapeNonBroadcastable": "ops", "TestAsinGo": "ops", "TestSinGo": "ops",
        "TestCosGo": "ops", "TestLogGo": "ops", "TestPowGo": "ops", "TestSqrtGo": "ops",
        "TestExpGoEdgeCase": "ops", "TestSincosTaylorRangeReduction": "ops",
        "TestTensorContract": "linalg", "TestMatMul": "linalg", "TestMatMulError": "linalg",
        "TestMatMulRankError": "linalg", "TestTranspose": "linalg", "TestTransposePermLengthError": "linalg",
        "TestBinaryShapeError": "linalg", "TestParseEinsum": "einsum", "TestParseEinsumErrors": "einsum",
        "TestEinsumValidate": "einsum", "TestInferOutputShape": "einsum", "TestOptimizePath": "einsum",
        "TestOptimizePathTwoInputs": "einsum", "TestEinsumDiagonalAndTrace": "einsum",
        "TestEinsumMultiTensorChain": "einsum", "TestIRGraph": "optimizer", "TestIRGraphDedup": "optimizer",
        "TestIRNodeTypes": "optimizer", "TestIRNumElements": "optimizer", "TestIRString": "optimizer",
        "TestContractShapeInference": "optimizer", "TestOptimizerCSE": "optimizer",
        "TestOptimizerConstantFolding": "optimizer", "TestOptimizeMulByZero": "optimizer",
        "TestOptimizeAddZero": "optimizer", "TestOptimizeDoubleNeg": "optimizer",
        "TestOptimizeTransposeIdentity": "optimizer", "TestFindCommonSubexpressions": "optimizer",
        "TestNoCSE": "optimizer", "TestIsIdentityPerm": "optimizer", "TestComposePerm": "optimizer",
        "TestOptimizeNilGraph": "optimizer", "TestApplyDefaultRule": "optimizer",
        "TestNodeKeyAllTypes": "optimizer", "TestNodeKeyUnknown": "optimizer", "TestCopyNode": "optimizer",
        "TestRewriteMulByZeroNonMul": "optimizer", "TestRewriteAddZeroNonAdd": "optimizer",
        "TestIsZeroTensorEdgeCases": "optimizer", "TestComposePermMismatched": "optimizer",
        "TestNewElementwiseMultiArg": "optimizer", "TestFloat64Contraction": "multidtype",
        "TestComplexContraction": "multidtype", "TestIntContraction": "multidtype",
        "TestContractGenericNonFloat32": "multidtype", "TestElementwiseUnaryNonFloat32": "multidtype",
        "TestElementwiseBinaryNonFloat32": "multidtype", "TestTransposeNonFloat32": "multidtype",
        "TestGemmCorrectness": "hardware", "TestInitMathDispatch": "hardware",
        "FuzzParseEinsum": "fuzz", "FuzzBroadcastShapes": "fuzz", "FuzzEinsumExecution": "fuzz",
    }

    test_events: Dict[str, Dict[str, Any]] = {}
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue

        test_name = ev.get("Test")
        if not test_name or "/" in test_name:
            continue

        action = ev.get("Action")
        if action == "run":
            test_events[test_name] = {"name": test_name, "start": time.perf_counter(), "output": []}
        elif action == "output" and test_name in test_events:
            test_events[test_name]["output"].append(ev.get("Output", ""))
        elif action in ("pass", "fail", "skip") and test_name in test_events:
            cat = category_map.get(test_name, "other")
            duration_s = ev.get("Elapsed", 0.0)
            passed = action == "pass"
            test_events[test_name]["passed"] = passed
            test_events[test_name]["duration_s"] = duration_s
            test_events[test_name]["category"] = cat
            test_events[test_name]["action"] = action
            unit_results.append(test_events[test_name])

    passed_count = 0
    failed_count = 0

    for res in unit_results:
        cat = res["category"]
        if category_filter != "all" and category_filter != cat:
            continue

        t_name = res["name"]
        passed = res["passed"]
        duration_ms = res["duration_s"] * 1000.0

        if passed:
            passed_count += 1
            status_str = c.green("PASS")
            time_str = c.dim(f"{duration_ms:.2f}ms")
            if verbose:
                print(f"  [{status_str}] ({c.yellow(cat):<10}) {t_name:<45} {time_str}")
        else:
            failed_count += 1
            status_str = c.red("FAIL")
            print(f"  [{status_str}] ({c.yellow(cat):<10}) {t_name:<45}")
            for out in res.get("output", []):
                print(f"      {c.red(out.strip())}")

    if not verbose:
        print(f"  {c.green('✔')} Executed {len(unit_results)} unit tests across internal/tensor ({passed_count} passed, {failed_count} failed) in {elapsed:.2f}s")

    return {
        "total": len(unit_results),
        "passed": passed_count,
        "failed": failed_count,
        "elapsed_s": elapsed,
        "tests": unit_results,
    }


def run_verification_suite(category_filter: str, verbose: bool, c: Colors) -> Dict[str, Any]:
    print(f"\n{c.bold(c.cyan('==> 2. Running Deep Feature Verification Suite (cmd/tensor-verify)...'))}")
    cmd = ["go", "run", VERIFY_CMD, "-json"]
    if category_filter != "all":
        cmd.extend(["-category", category_filter])

    code, stdout, stderr, elapsed = run_command(cmd)

    if code != 0 and not stdout.strip().startswith("{"):
        print(f"  {c.red('❌ Compilation/Execution error in cmd/tensor-verify:')}\n{stderr}")
        return {"total": 0, "passed": 0, "failed": 1, "error": stderr, "results": []}

    try:
        report = json.loads(stdout)
    except json.JSONDecodeError:
        print(f"  {c.red('❌ Failed to parse JSON report from cmd/tensor-verify:')}\n{stdout}\n{stderr}")
        return {"total": 0, "passed": 0, "failed": 1, "error": "Invalid JSON", "results": []}

    results = report.get("results", [])
    curr_category = None

    for item in results:
        cat = item.get("category", "general")
        name = item.get("name", "")
        passed = item.get("passed", False)
        duration_us = item.get("duration_ns", 0) / 1000.0

        if cat != curr_category:
            curr_category = cat
            print(f"\n  {c.bold(c.yellow(f'[{cat.upper()} FEATURES]'))}")

        if passed:
            mark = c.green("✔ PASS")
            t_str = c.dim(f"{duration_us:.1f}µs")
            print(f"    {mark} {name:<62} {t_str}")
        else:
            mark = c.red("✖ FAIL")
            err = item.get("error", "unknown error")
            print(f"    {mark} {name:<62}\n        {c.red(f'Error: {err}')}")

    return report


def run_benchmarks(c: Colors) -> Dict[str, Any]:
    print(f"\n{c.bold(c.cyan('==> 3. Running Tensor Microbenchmarks...'))}")
    cmd = ["go", "test", "-bench=BenchmarkMatMul", "-benchtime=100ms", "./internal/tensor"]
    code, stdout, stderr, elapsed = run_command(cmd)

    bench_lines = []
    bench_data = []
    for line in stdout.splitlines():
        if line.startswith("Benchmark"):
            bench_lines.append(line)
            parts = line.split()
            if len(parts) >= 4:
                b_name = parts[0]
                iters = parts[1]
                ns_op = parts[2]
                bench_data.append({"name": b_name, "iterations": iters, "ns_per_op": ns_op})
                print(f"  {c.magenta('⚡')} {b_name:<40} {iters:>10} iters {c.bold(ns_op):>15} ns/op")

    if not bench_lines:
        print(f"  {c.yellow('No benchmarks completed or output empty.')}")
        if stderr:
            print(f"  {c.red(stderr)}")

    return {"elapsed_s": elapsed, "benchmarks": bench_data, "raw": stdout}


def run_fuzz_targets(c: Colors) -> Dict[str, Any]:
    print(f"\n{c.bold(c.cyan('==> 4. Running Fuzzing Targets (seed & corpus validation)...'))}")
    fuzz_targets = ["FuzzParseEinsum", "FuzzBroadcastShapes", "FuzzEinsumExecution"]
    results = []

    for target in fuzz_targets:
        cmd = ["go", "test", "-run", f"^{target}$", "./internal/tensor"]
        code, stdout, stderr, elapsed = run_command(cmd)
        passed = code == 0
        status = c.green("✔ PASS") if passed else c.red("✖ FAIL")
        print(f"  {status} {target:<30} seed corpus verification ({elapsed:.2f}s)")
        results.append({"target": target, "passed": passed, "elapsed_s": elapsed, "output": stdout})

    return {"targets": results}


def print_banner(c: Colors):
    print(c.bold(c.blue("================================================================================")))
    print(c.bold(c.blue("            LONGBOW TENSOR ENGINE COMPREHENSIVE TEST HARNESS                   ")))
    print(c.bold(c.blue("================================================================================")))
    print(f"  {c.bold('System:')}      {platform.system()} {platform.release()} ({platform.machine()})")
    print(f"  {c.bold('CPU:')}         {get_cpu_info()}")
    print(f"  {c.bold('Go Toolchain:')} {get_go_version()}")
    print(f"  {c.bold('Timestamp:')}    {datetime.datetime.now().isoformat()}")
    print(c.bold(c.blue("--------------------------------------------------------------------------------")))


def print_summary(unit_rep: Dict[str, Any], verify_rep: Dict[str, Any], total_elapsed: float, c: Colors) -> int:
    print(f"\n{c.bold(c.blue('================================================================================'))}")
    print(c.bold(c.blue("                             TEST EXECUTION SUMMARY                             ")))
    print(c.bold(c.blue("================================================================================")))

    unit_total = unit_rep.get("total", 0)
    unit_passed = unit_rep.get("passed", 0)
    unit_failed = unit_rep.get("failed", 0)

    verify_total = verify_rep.get("total", 0)
    verify_passed = verify_rep.get("passed", 0)
    verify_failed = verify_rep.get("failed", 0)

    total_tests = unit_total + verify_total
    total_passed = unit_passed + verify_passed
    total_failed = unit_failed + verify_failed

    print(f"  Unit Tests (internal/tensor):  {unit_passed}/{unit_total} passed" + (f" ({c.red(str(unit_failed) + ' failed')})" if unit_failed else ""))
    print(f"  Deep Feature Verifications:    {verify_passed}/{verify_total} passed" + (f" ({c.red(str(verify_failed) + ' failed')})" if verify_failed else ""))
    print(c.bold(c.blue("--------------------------------------------------------------------------------")))
    print(f"  {c.bold('GRAND TOTAL:')}                  {total_passed}/{total_tests} passed in {total_elapsed:.2f}s")

    if total_failed == 0 and total_tests > 0:
        print(f"\n  {c.bold(c.green('🎉 ALL TENSOR ENGINE FEATURES VERIFIED & PASSING SUCCESSFULLY!'))}\n")
        return 0
    else:
        print(f"\n  {c.bold(c.red(f'❌ TEST FAILURES DETECTED: {total_failed} tests failed!'))}\n")
        return 1


def main():
    parser = argparse.ArgumentParser(description="Test all features of the Longbow Tensor Engine.")
    parser.add_argument("--category", default="all", choices=["all", "core", "ops", "linalg", "einsum", "optimizer", "calculus", "multidtype", "hardware", "fuzz"], help="Filter tests by feature category")
    parser.add_argument("--bench", action="store_true", help="Run performance microbenchmarks")
    parser.add_argument("--fuzz", action="store_true", help="Run fuzz targets")
    parser.add_argument("--unit-only", action="store_true", help="Run only package unit tests")
    parser.add_argument("--verify-only", action="store_true", help="Run only deep verification suite")
    parser.add_argument("--json-report", default=None, help="Save report to JSON file")
    parser.add_argument("--no-color", action="store_true", help="Disable colored output")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose test logs")
    args = parser.parse_args()

    c = Colors(enabled=not args.no_color and sys.stdout.isatty())
    print_banner(c)

    start_total = time.perf_counter()

    unit_rep = {"total": 0, "passed": 0, "failed": 0}
    verify_rep = {"total": 0, "passed": 0, "failed": 0}
    bench_rep = None
    fuzz_rep = None

    if not args.verify_only:
        unit_rep = run_unit_tests(args.category, args.verbose, c)

    if not args.unit_only:
        verify_rep = run_verification_suite(args.category, args.verbose, c)

    if args.bench:
        bench_rep = run_benchmarks(c)

    if args.fuzz:
        fuzz_rep = run_fuzz_targets(c)

    total_elapsed = time.perf_counter() - start_total
    exit_code = print_summary(unit_rep, verify_rep, total_elapsed, c)

    if args.json_report:
        full_report = {
            "timestamp": datetime.datetime.now().isoformat(),
            "system": {
                "os": platform.system(),
                "release": platform.release(),
                "arch": platform.machine(),
                "cpu": get_cpu_info(),
                "go_version": get_go_version(),
            },
            "summary": {
                "total_tests": unit_rep.get("total", 0) + verify_rep.get("total", 0),
                "total_passed": unit_rep.get("passed", 0) + verify_rep.get("passed", 0),
                "total_failed": unit_rep.get("failed", 0) + verify_rep.get("failed", 0),
                "duration_s": total_elapsed,
                "success": exit_code == 0,
            },
            "unit_tests": unit_rep,
            "verification_tests": verify_rep,
            "benchmarks": bench_rep,
            "fuzz_tests": fuzz_rep,
        }
        report_path = os.path.abspath(args.json_report)
        with open(report_path, "w") as f:
            json.dump(full_report, f, indent=2)
        print(f"  {c.cyan('JSON Report written to:')} {report_path}\n")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
