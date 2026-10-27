#!/usr/bin/env python3
"""Unit tests for Longbow Dockerfiles.

Validates multi-stage build structure, DevSecOps compliance (non-root USER,
APT hygiene), port exposure, volume mounting, entrypoints, build flags,
and BuildKit syntax conformance across all Dockerfile flavors.
"""

import os
import re
import subprocess
import unittest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

DOCKERFILES = [
    {
        "name": "Dockerfile",
        "is_symlink": True,
        "symlink_target": "Dockerfile.cpu",
    },
    {
        "name": "Dockerfile.cpu",
        "stages": ["golang:1.27.2-bookworm", "scratch"],
        "runtime_user": "65534:65534",
        "ports": ["3000", "3001", "9090"],
        "volume_data": True,
        "env_vars": {"LONGBOW_GPU_ENABLED": "false", "GOGC": "75"},
        "entrypoint": '["/longbow"]',
    },
    {
        "name": "Dockerfile.emlgo-cpu",
        "stages": ["golang:1.27.2-bookworm", "scratch"],
        "runtime_user": "65534:65534",
        "ports": ["3000", "3001", "9090"],
        "volume_data": True,
        "env_vars": {"LONGBOW_GPU_ENABLED": "false", "LONGBOW_MATH_DISPATCH": "emlgo", "GOGC": "75"},
        "entrypoint": '["/longbow"]',
    },
    {
        "name": "Dockerfile.emlgo-gpu",
        "stages": ["nvidia/cuda:13.4.3-devel-ubuntu24.04", "nvidia/cuda:13.4.3-runtime-ubuntu24.04"],
        "runtime_user": "longbow:longbow",
        "ports": ["3000", "3001", "9090"],
        "volume_data": True,
        "env_vars": {"LONGBOW_GPU_ENABLED": "true", "LONGBOW_MATH_DISPATCH": "emlgo", "GOGC": "75"},
        "has_healthcheck": True,
        "entrypoint": '["/app/longbow"]',
    },
    {
        "name": "Dockerfile.nvidia",
        "stages": ["nvidia/cuda:13.4.3-devel-ubuntu24.04", "nvidia/cuda:13.4.3-runtime-ubuntu24.04"],
        "runtime_user": "longbow:longbow",
        "ports": ["3000", "3001", "9090"],
        "volume_data": True,
        "env_vars": {"LONGBOW_GPU_ENABLED": "true", "GOGC": "75"},
        "has_healthcheck": True,
        "entrypoint": '["/app/longbow"]',
    },
    {
        "name": "Dockerfile.tpu",
        "stages": ["golang:1.27.2-bookworm", "debian:bookworm-slim"],
        "runtime_user": "65534:65534",
        "ports": ["3000", "3001", "9090"],
        "volume_data": True,
        "env_vars": {"LONGBOW_TPU_ENABLED": "true", "LONGBOW_GPU_ENABLED": "false", "GOGC": "75"},
        "entrypoint": '["/longbow"]',
    },
    {
        "name": "Dockerfile.metal",
        "stages": ["debian:bookworm-slim"],
        "runtime_user": "longbow:longbow",
        "ports": ["3000", "3001", "9090"],
        "volume_data": False,
        "env_vars": {"LONGBOW_GPU_ENABLED": "true", "GOGC": "75"},
        "entrypoint": '["/usr/local/bin/longbow"]',
    },
]


class TestDockerfiles(unittest.TestCase):
    def test_dockerfile_existence_and_symlinks(self):
        """All expected Dockerfiles must exist and symlink must resolve."""
        for df in DOCKERFILES:
            path = os.path.join(REPO_ROOT, df["name"])
            self.assertTrue(os.path.exists(path), f"File {df['name']} does not exist")
            if df.get("is_symlink"):
                self.assertTrue(os.path.islink(path), f"{df['name']} should be a symlink")
                target = os.readlink(path)
                self.assertEqual(target, df["symlink_target"])
                self.assertTrue(os.path.exists(os.path.join(REPO_ROOT, target)))

    def test_multi_stage_structure(self):
        """Check expected FROM stages and base images."""
        for df in DOCKERFILES:
            if df.get("is_symlink"):
                continue
            path = os.path.join(REPO_ROOT, df["name"])
            with open(path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            from_lines = [l.strip() for l in lines if l.strip().upper().startswith("FROM ")]
            expected_stages = df["stages"]
            self.assertEqual(
                len(from_lines),
                len(expected_stages),
                f"{df['name']} expected {len(expected_stages)} stages, found {len(from_lines)}: {from_lines}",
            )
            for i, expected_img in enumerate(expected_stages):
                self.assertIn(expected_img, from_lines[i], f"{df['name']} stage {i+1} base image mismatch")

    def test_non_root_user(self):
        """DevSecOps: every runtime stage must specify a non-root USER."""
        for df in DOCKERFILES:
            if df.get("is_symlink"):
                continue
            path = os.path.join(REPO_ROOT, df["name"])
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            user_matches = re.findall(r"^USER\s+([^\s]+)", content, re.MULTILINE)
            self.assertTrue(len(user_matches) > 0, f"{df['name']} must contain a USER instruction")
            runtime_user = user_matches[-1]
            self.assertEqual(runtime_user, df["runtime_user"], f"{df['name']} runtime USER mismatch")
            self.assertNotIn(runtime_user.lower(), ["root", "0"], f"{df['name']} cannot run as root")

    def test_apt_hygiene(self):
        """apt-get install calls must use --no-install-recommends and clean cache."""
        for df in DOCKERFILES:
            if df.get("is_symlink"):
                continue
            path = os.path.join(REPO_ROOT, df["name"])
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            if "apt-get install" in content:
                self.assertIn("--no-install-recommends", content, f"{df['name']} apt-get must use --no-install-recommends")
                self.assertIn("rm -rf /var/lib/apt/lists/*", content, f"{df['name']} must clean apt cache")

    def test_ports_exposed(self):
        """Standard Longbow ports (3000, 3001, 9090) must be exposed."""
        for df in DOCKERFILES:
            if df.get("is_symlink"):
                continue
            path = os.path.join(REPO_ROOT, df["name"])
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            expose_matches = re.findall(r"^EXPOSE\s+(.+)$", content, re.MULTILINE)
            all_ports = []
            for match in expose_matches:
                all_ports.extend(match.split())
            for expected_port in df["ports"]:
                self.assertIn(expected_port, all_ports, f"{df['name']} must expose port {expected_port}")

    def test_volume_data(self):
        """VOLUME /data must be declared when persistence is required."""
        for df in DOCKERFILES:
            if df.get("is_symlink"):
                continue
            path = os.path.join(REPO_ROOT, df["name"])
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            has_volume = "VOLUME /data" in content
            self.assertEqual(has_volume, df["volume_data"], f"{df['name']} VOLUME /data mismatch")

    def test_reproducible_build_flags(self):
        """Go build invocations must include -trimpath, -buildvcs=false, and -mod=vendor."""
        for df in DOCKERFILES:
            if df.get("is_symlink") or df["name"] == "Dockerfile.metal":
                continue
            path = os.path.join(REPO_ROOT, df["name"])
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            self.assertIn("-trimpath", content, f"{df['name']} missing -trimpath")
            self.assertIn("-buildvcs=false", content, f"{df['name']} missing -buildvcs=false")
            self.assertIn("-mod=vendor", content, f"{df['name']} missing -mod=vendor")
            self.assertIn('-ldflags="-s -w', content, f"{df['name']} missing -ldflags='-s -w'")

    def test_environment_variables(self):
        """Runtime environment variables must be accurately configured."""
        for df in DOCKERFILES:
            if df.get("is_symlink"):
                continue
            path = os.path.join(REPO_ROOT, df["name"])
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            for key, val in df["env_vars"].items():
                expected = f"{key}={val}"
                self.assertIn(expected, content, f"{df['name']} missing ENV {expected}")

    def test_entrypoints(self):
        """ENTRYPOINT must be defined using JSON array exec format."""
        for df in DOCKERFILES:
            if df.get("is_symlink"):
                continue
            path = os.path.join(REPO_ROOT, df["name"])
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            ep_match = re.search(r"^ENTRYPOINT\s+(\[.+\])", content, re.MULTILINE)
            self.assertIsNotNone(ep_match, f"{df['name']} must declare JSON array ENTRYPOINT")
            self.assertEqual(ep_match.group(1).strip(), df["entrypoint"], f"{df['name']} entrypoint mismatch")

    def test_healthchecks(self):
        """GPU images must configure a container HEALTHCHECK probing port 9090."""
        for df in DOCKERFILES:
            if df.get("is_symlink"):
                continue
            path = os.path.join(REPO_ROOT, df["name"])
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            has_health = "HEALTHCHECK" in content
            self.assertEqual(has_health, df.get("has_healthcheck", False), f"{df['name']} HEALTHCHECK mismatch")
            if has_health:
                self.assertIn("http://localhost:9090/health", content)

    def test_docker_build_check(self):
        """docker build --check should exit with 0 warnings when docker is installed."""
        try:
            res = subprocess.run(["docker", "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if res.returncode != 0:
                self.skipTest("docker CLI not available")
        except FileNotFoundError:
            self.skipTest("docker CLI not found")

        for df in DOCKERFILES:
            if df.get("is_symlink"):
                continue
            cmd = ["docker", "build", "--check", "-f", df["name"], "."]
            res = subprocess.run(cmd, cwd=REPO_ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            self.assertEqual(res.returncode, 0, f"docker build --check failed on {df['name']}: {res.stderr}\n{res.stdout}")
            self.assertIn("Check complete, no warnings found", res.stdout + res.stderr)


if __name__ == "__main__":
    unittest.main()
