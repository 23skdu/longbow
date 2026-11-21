"""Cross-layer search mode coverage, benchmark harness, SDK, and docs consistency tests."""

import importlib.util
import os
import re
import sys
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS = os.path.dirname(_HERE)
_ROOT = os.path.dirname(_SCRIPTS)

if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)


def _read(relpath):
    with open(os.path.join(_ROOT, relpath), "r", encoding="utf-8") as f:
        return f.read()


def _read_tree(rel_dir, ext, exclude_suffix="_test.go"):
    base = os.path.join(_ROOT, rel_dir)
    chunks = []
    for root, _, files in sorted(os.walk(base)):
        for name in sorted(files):
            if name.endswith(ext) and not (exclude_suffix and name.endswith(exclude_suffix)):
                with open(os.path.join(root, name), "r", encoding="utf-8") as f:
                    chunks.append(f.read())
    return "\n".join(chunks)


def _load_module(name, relpath):
    spec = importlib.util.spec_from_file_location(name, os.path.join(_ROOT, relpath))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {relpath}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


class TestBenchToolsSearchModes(unittest.TestCase):
    def test_bench_tool_all_search_modes_complete(self):
        src = _read_tree("cmd/bench-tool", ".go")
        m = re.search(r"var allSearchModes = \[\]string\{(.*?)\}", src, re.S)
        self.assertIsNotNone(m, "allSearchModes slice not found in cmd/bench-tool/")
        modes = set(re.findall(r'"([^"]+)"', m.group(1)))
        expected = {
            "Dense",
            "Hybrid",
            "Filtered",
            "FilteredBool",
            "FilteredString",
            "Sparse",
            "ByID",
            "Range",
            "AdvancedText",
            "GraphRAG",
            "GlobalGraphRAG",
            "Recommend",
            "Geo",
            "GeoBox",
            "GeoHybrid",
            "Temporal",
            "TemporalRange",
            "TemporalWindow",
            "MultiModal",
            "Federated",
            "LearnedIndex",
        }
        missing = expected - modes
        self.assertEqual(missing, set(), f"Missing modes in bench-tool allSearchModes: {missing}")

    def test_io_bench_covers_extended_ticket_modes(self):
        src = _read("cmd/io-bench/main.go")
        for ticket_key in (
            '"search"',
            '"search_by_id"',
            '"range_search"',
            '"federated_search"',
            '"recommend"',
            '"geo_search"',
            '"temporal_search"',
        ):
            self.assertIn(ticket_key, src)

    def test_cli_exposes_all_search_commands(self):
        main_src = _read("cmd/cli/main.go")
        for cmd in (
            "search",
            "range-search",
            "federated-search",
            "text-search",
            "geo-search",
            "temporal-search",
            "multimodal-search",
            "recommend",
            "explain-search",
            "rerank",
            "rewrite-query",
        ):
            self.assertIn(f'case "{cmd}"', main_src, f"Missing CLI command: {cmd}")


class TestUnifiedBenchmarkHarnessModes(unittest.TestCase):
    def test_enables_hybrid_text_columns_for_bm25_modes(self):
        src = _read("scripts/unified_benchmark.py") + "\n" + _read_tree("scripts/benchmark", ".py", None)
        self.assertIn('env["LONGBOW_HYBRID_SEARCH_ENABLED"] = "true"', src)
        self.assertIn('env["LONGBOW_HYBRID_TEXT_COLUMNS"]', src)

    def test_scenario_mixin_exposes_new_search_scenarios(self):
        from benchmark.scenarios import ScenarioMixin
        from benchmark.scenarios._mode_runner import summarize_latencies

        for method_name in (
            "execute_range",
            "execute_federated",
            "execute_text",
            "execute_multimodal",
            "execute_geo",
            "execute_temporal",
            "execute_recommend",
            "execute_graphrag",
            "execute_learned_index",
        ):
            self.assertTrue(
                hasattr(ScenarioMixin, method_name),
                f"ScenarioMixin missing {method_name}",
            )

        summary = summarize_latencies([1.0, 2.0, 4.0], 0.5)
        self.assertAlmostEqual(summary["qps"], 6.0)
        self.assertGreater(summary["p99_ms"], 0.0)

    def test_unified_benchmark_dispatches_and_documents_new_scenarios(self):
        src = _read("scripts/unified_benchmark.py") + "\n" + _read_tree("scripts/benchmark", ".py", None)
        for scenario in ("range", "federated", "text", "multimodal"):
            self.assertIn(f'if mode == "{scenario}":', src)
            self.assertIn(f"self.execute_{scenario}()", src)
            self.assertIn(scenario, src[src.index('"--mode"') : src.index('"--dims"')])


class TestPythonSDKAndDocsParity(unittest.TestCase):
    def test_python_sdk_defines_all_search_methods(self):
        src = _read_tree("longbowclientsdk/src/longbow", ".py", None)
        expected_methods = [
            "search",
            "search_by_id",
            "search_by_id_df",
            "get_vector",
            "range_search",
            "federated_search",
            "register_federated_collection",
            "list_federated_collections",
            "advanced_text_search",
            "search_facets",
            "search_analytics",
            "tune_bm25",
            "geo_search",
            "temporal_search",
            "multimodal_search",
            "recommend",
            "traverse",
            "rerank",
            "rewrite_query",
            "explain_search",
            "rest_hybrid_search",
            "rest_search_by_id",
            "rest_range_search",
            "rest_text_search",
            "rest_geo_search",
            "rest_temporal_search",
            "rest_multimodal_search",
            "rest_federated_search",
        ]
        for method in expected_methods:
            self.assertRegex(src, rf"def {method}\(", f"Python SDK missing {method}()")

    def test_docs_reflect_all_search_modes_and_valid_flags(self):
        docs_readme = _read("docs/README.md")
        vectorsearch = _read("docs/vectorsearch.md")
        api_doc = _read("docs/api.md")
        features_doc = _read("docs/features.md")
        dev_doc = _read("docs/development.md")
        root_readme = _read("README.md")

        for term in (
            "ByID",
            "Sparse",
            "Advanced Full-Text",
            "Hybrid",
            "Range Search",
            "Temporal",
            "Geo-Spatial",
            "Multi-Modal",
            "Recommend",
            "GraphRAG",
            "Federated",
            "TurboQuant",
            "Learned Index",
        ):
            self.assertIn(term, docs_readme, f"docs/README.md missing {term}")
            self.assertIn(term, vectorsearch, f"docs/vectorsearch.md missing {term}")

        for endpoint in (
            "/search/by-id",
            "/hybrid",
            "/text-search",
            "/geo-search",
            "/temporal-search",
            "/multimodal-search",
            "/range-search",
            "/recommend",
            "/api/v1/search/federated",
        ):
            self.assertIn(endpoint, api_doc, f"docs/api.md missing {endpoint}")
            self.assertIn(endpoint.split("/")[-1], features_doc, f"docs/features.md missing {endpoint}")

        self.assertNotIn("--modes ", root_readme, "README.md should use --mode, not invalid --modes")
        self.assertIn("--search-modes all", vectorsearch)
        self.assertIn("-search-modes", dev_doc)

    def test_benchmark_scenarios_and_sdk_imports_are_valid(self):
        """All scenario modules import cleanly and avoid stale SDK/runner method names."""
        import ast
        from unittest.mock import MagicMock, patch

        # unified_benchmark must expose local longbowclientsdk/src fallback on sys.path.
        _load_module("unified_benchmark_test_mod", "scripts/unified_benchmark.py")
        sdk_src = os.path.join(_ROOT, "longbowclientsdk", "src")
        self.assertIn(sdk_src, sys.path)

        # Every file in scripts/benchmark/ must define LongbowClient before referencing it
        # and avoid stale runner/client method names.
        for name in sorted(os.listdir(os.path.join(_SCRIPTS, "benchmark", "scenarios"))):
            if not name.endswith(".py"):
                continue
            rel = f"scripts/benchmark/scenarios/{name}"
            src = _read(rel)
            compile(src, rel, "exec")
            if "LongbowClient" in src:
                self.assertIn(
                    "from longbow import LongbowClient",
                    src,
                    f"{rel} references LongbowClient without importing it",
                )
            self.assertNotIn("delete_dataset(", src, f"{rel} still uses non-existent delete_dataset")
            self.assertNotIn("get_bench_tool_path(", src, f"{rel} still uses non-existent get_bench_tool_path")
            self.assertNotIn("insert_vectors(", src, f"{rel} still uses non-existent insert_vectors")

        mg_src = _read("scripts/benchmark/memory_guard.py")
        compile(mg_src, "scripts/benchmark/memory_guard.py", "exec")
        self.assertNotIn("insert_vectors(", mg_src)
        self.assertNotIn("self.client.", mg_src)

        geo_src = _read("scripts/benchmark/scenarios/geo.py")
        self.assertIn("client.geo_search(", geo_src)
        self.assertNotIn("geo_center=", geo_src)
        self.assertNotIn("geo_box=", geo_src)

        # Verify _mode_runner fallback invokes orchestrator.get_bench_tool() cleanly.
        from benchmark.scenarios import _mode_runner

        orch = MagicMock()
        orch.get_bench_tool.return_value = "/tmp/nonexistent-bench-tool"
        with patch.object(_mode_runner, "run_command"):
            res = _mode_runner.run_bench_tool_mode_fallback(orch, "lbl", "federated", 128, 100)
        self.assertIsNone(res)
        orch.get_bench_tool.assert_called_once_with()

        # Verify ScenarioMixin binds all execute_* entrypoints cleanly.
        from benchmark.scenarios import ScenarioMixin

        tree = ast.parse(_read("scripts/benchmark/scenarios/__init__.py"))
        self.assertIsNotNone(tree)
        for attr in (
            "execute_churn",
            "execute_cluster",
            "execute_deletion",
            "execute_exchange",
            "execute_federated",
            "execute_geo",
            "execute_graphrag",
            "execute_learned_index",
            "execute_multimodal",
            "execute_onnx",
            "execute_range",
            "execute_recommend",
            "execute_temporal",
            "execute_text",
        ):
            self.assertTrue(hasattr(ScenarioMixin, attr), f"ScenarioMixin missing {attr}")


if __name__ == "__main__":
    unittest.main()

