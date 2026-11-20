"""Tests for release version consistency guard and bump helper.

Covers:
- Consistent binary/ADBC and Helm versions pass the guard
- Drift between pkg/version and Helm chart version/appVersion is rejected
- Malformed versions are rejected
- bump_version.sh atomically updates every release artifact
- bump_version.sh rejects invalid version strings
- Missing files produce a setup error (exit 2)
"""

import os
import shutil
import subprocess
import tempfile
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(os.path.dirname(_HERE))
_SCRIPTS = os.path.dirname(_HERE)
_CHECK = os.path.join(_SCRIPTS, "check_version_consistency.sh")
_BUMP = os.path.join(_SCRIPTS, "bump_version.sh")

GO_REL = "pkg/version/version.go"
CHART_REL = "helm/longbow/Chart.yaml"


def _run(script, *args, cwd=None):
    return subprocess.run(
        [script, *args], cwd=cwd or _REPO, capture_output=True, text=True
    )


def _fresh_repo(tmp):
    """Create a minimal repo-shaped tree with current version files."""
    root = os.path.join(tmp, "repo")
    os.makedirs(os.path.join(root, "pkg", "version"), exist_ok=True)
    os.makedirs(os.path.join(root, "helm", "longbow"), exist_ok=True)
    shutil.copy(os.path.join(_REPO, GO_REL), os.path.join(root, GO_REL))
    shutil.copy(os.path.join(_REPO, CHART_REL), os.path.join(root, CHART_REL))
    return root


class TestCheckVersionConsistency(unittest.TestCase):
    def test_current_repo_is_consistent(self):
        res = _run(_CHECK)
        self.assertEqual(res.returncode, 0, res.stderr + res.stdout)
        self.assertIn("PASS", res.stdout)
        self.assertIn("pkg/version/version.go", res.stdout)

    def test_go_vs_chart_drift_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = _fresh_repo(tmp)
            p = os.path.join(root, GO_REL)
            with open(p, "r") as f:
                s = f.read()
            with open(p, "w") as f:
                f.write(s.replace('Version = "0.2.6"', 'Version = "0.2.7"'))
            res = _run(_CHECK, root)
            self.assertEqual(res.returncode, 1)
            self.assertIn("version drift", res.stderr)

    def test_chart_version_vs_appversion_drift_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = _fresh_repo(tmp)
            p = os.path.join(root, CHART_REL)
            with open(p, "r") as f:
                s = f.read()
            with open(p, "w") as f:
                f.write(s.replace('appVersion: "0.2.6"', 'appVersion: "0.2.5"'))
            res = _run(_CHECK, root)
            self.assertEqual(res.returncode, 1)
            self.assertIn("version drift", res.stderr)

    def test_malformed_version_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = _fresh_repo(tmp)
            p = os.path.join(root, GO_REL)
            with open(p, "r") as f:
                s = f.read()
            with open(p, "w") as f:
                f.write(s.replace('Version = "0.2.6"', 'Version = "banana"'))
            res = _run(_CHECK, root)
            self.assertEqual(res.returncode, 1)
            self.assertIn("malformed version", res.stderr)

    def test_missing_file_is_setup_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            res = _run(_CHECK, tmp)
            self.assertEqual(res.returncode, 2)
            self.assertIn("missing", res.stderr)


class TestBumpVersion(unittest.TestCase):
    def test_bump_updates_all_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = _fresh_repo(tmp)
            res = _run(_BUMP, "0.3.0", root)
            self.assertEqual(res.returncode, 0, res.stderr + res.stdout)
            with open(os.path.join(root, GO_REL)) as f:
                self.assertIn('Version = "0.3.0"', f.read())
            with open(os.path.join(root, CHART_REL)) as f:
                chart = f.read()
            self.assertIn("version: 0.3.0", chart)
            self.assertIn('appVersion: "0.3.0"', chart)
            check = _run(_CHECK, root)
            self.assertEqual(check.returncode, 0, check.stderr)

    def test_bump_rejects_invalid_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = _fresh_repo(tmp)
            res = _run(_BUMP, "not-semver", root)
            self.assertEqual(res.returncode, 2)
            self.assertIn("Invalid version", res.stderr)

    def test_bump_requires_version_argument(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = _fresh_repo(tmp)
            res = _run(_BUMP, cwd=root)
            self.assertEqual(res.returncode, 2)
            self.assertIn("Usage", res.stderr)


if __name__ == "__main__":
    unittest.main()
