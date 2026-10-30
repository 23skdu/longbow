package docker

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dockerfileMeta defines expectations for each Dockerfile flavor.
type dockerfileMeta struct {
	filename       string
	isSymlink      bool
	symlinkTarget  string
	stages         []string
	runtimeUser    string
	exposedPorts   []string
	hasVolumeData  bool
	expectedTags   []string
	envVars        map[string]string
	hasHealthCheck bool
	entrypoint     string
}

var allDockerfiles = []dockerfileMeta{
	{
		filename:      "Dockerfile",
		isSymlink:     true,
		symlinkTarget: "Dockerfile.cpu",
	},
	{
		filename:      "Dockerfile.cpu",
		stages:        []string{"golang:1.27.3-bookworm", "scratch"},
		runtimeUser:   "65534:65534",
		exposedPorts:  []string{"3000", "3001", "9090"},
		hasVolumeData: true,
		expectedTags:  []string{"${BUILD_TAGS}", "iouring"},
		envVars: map[string]string{
			"LONGBOW_GPU_ENABLED":         "false",
			"LONGBOW_STORAGE_USE_IOURING": "false",
			"GOGC":                        "75",
		},
		entrypoint: `["/longbow"]`,
	},
	{
		filename:      "Dockerfile.emlgo-cpu",
		stages:        []string{"golang:1.27.3-bookworm", "scratch"},
		runtimeUser:   "65534:65534",
		exposedPorts:  []string{"3000", "3001", "9090"},
		hasVolumeData: true,
		expectedTags:  []string{"emlgo", "iouring"},
		envVars: map[string]string{
			"LONGBOW_GPU_ENABLED":         "false",
			"LONGBOW_STORAGE_USE_IOURING": "false",
			"LONGBOW_MATH_DISPATCH":       "emlgo",
			"GOGC":                        "75",
		},
		entrypoint: `["/longbow"]`,
	},
	{
		filename:      "Dockerfile.emlgo-gpu",
		stages:        []string{"nvidia/cuda:13.4.3-devel-ubuntu24.04", "nvidia/cuda:13.4.3-runtime-ubuntu24.04"},
		runtimeUser:   "longbow:longbow",
		exposedPorts:  []string{"3000", "3001", "9090"},
		hasVolumeData: true,
		expectedTags:  []string{"gpu", "emlgo"},
		envVars: map[string]string{
			"LONGBOW_GPU_ENABLED":         "true",
			"LONGBOW_STORAGE_USE_IOURING": "false",
			"LONGBOW_MATH_DISPATCH":       "emlgo",
			"GOGC":                        "75",
		},
		hasHealthCheck: true,
		entrypoint:     `["/app/longbow"]`,
	},
	{
		filename:      "Dockerfile.nvidia",
		stages:        []string{"nvidia/cuda:13.4.3-devel-ubuntu24.04", "nvidia/cuda:13.4.3-runtime-ubuntu24.04"},
		runtimeUser:   "longbow:longbow",
		exposedPorts:  []string{"3000", "3001", "9090"},
		hasVolumeData: true,
		expectedTags:  []string{"gpu"},
		envVars: map[string]string{
			"LONGBOW_GPU_ENABLED":         "true",
			"LONGBOW_STORAGE_USE_IOURING": "false",
			"GOGC":                        "75",
		},
		hasHealthCheck: true,
		entrypoint:     `["/app/longbow"]`,
	},
	{
		filename:      "Dockerfile.tpu",
		stages:        []string{"golang:1.27.3-bookworm", "debian:bookworm-slim"},
		runtimeUser:   "65534:65534",
		exposedPorts:  []string{"3000", "3001", "9090"},
		hasVolumeData: true,
		expectedTags:  []string{"tpu"},
		envVars: map[string]string{
			"LONGBOW_TPU_ENABLED": "true",
			"LONGBOW_GPU_ENABLED": "false",
			"GOGC":                "75",
		},
		entrypoint: `["/longbow"]`,
	},
	{
		filename:      "Dockerfile.metal",
		stages:        []string{"debian:bookworm-slim"},
		runtimeUser:   "longbow:longbow",
		exposedPorts:  []string{"3000", "3001", "9090"},
		hasVolumeData: false,
		envVars: map[string]string{
			"LONGBOW_GPU_ENABLED":         "true",
			"LONGBOW_STORAGE_USE_IOURING": "false",
			"GOGC":                        "75",
		},
		entrypoint: `["/usr/local/bin/longbow"]`,
	},
}

func getRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find repository root containing go.mod")
		}
		dir = parent
	}
}

// readDockerfileLines reads non-comment, trimmed lines or entire file lines.
func readDockerfileLines(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err, "failed to open %s", path)
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	require.NoError(t, scanner.Err())
	return lines
}

// TestDockerfiles_ExistAndValidSymlink verifies all Dockerfiles exist and Dockerfile symlink is intact.
func TestDockerfiles_ExistAndValidSymlink(t *testing.T) {
	root := getRepoRoot(t)

	for _, df := range allDockerfiles {
		path := filepath.Join(root, df.filename)
		fi, err := os.Lstat(path)
		require.NoError(t, err, "Dockerfile %s must exist", df.filename)

		if df.isSymlink {
			assert.True(t, fi.Mode()&os.ModeSymlink != 0, "%s must be a symlink", df.filename)
			target, err := os.Readlink(path)
			require.NoError(t, err)
			assert.Equal(t, df.symlinkTarget, target, "%s should target %s", df.filename, df.symlinkTarget)

			// Verify resolved target exists
			targetPath := filepath.Join(root, target)
			_, err = os.Stat(targetPath)
			assert.NoError(t, err, "symlink target %s must exist", target)
		} else {
			assert.False(t, fi.Mode()&os.ModeSymlink != 0, "%s must be a regular file", df.filename)
		}
	}
}

// TestDockerfiles_MultiStageStructure checks stage names and base images.
func TestDockerfiles_MultiStageStructure(t *testing.T) {
	root := getRepoRoot(t)

	for _, df := range allDockerfiles {
		if df.isSymlink {
			continue
		}
		t.Run(df.filename, func(t *testing.T) {
			lines := readDockerfileLines(t, filepath.Join(root, df.filename))
			var fromStages []string
			for _, line := range lines {
				trimmed := strings.TrimSpace(line)
				if strings.HasPrefix(strings.ToUpper(trimmed), "FROM ") {
					fromStages = append(fromStages, trimmed)
				}
			}

			require.Equal(t, len(df.stages), len(fromStages),
				"expected %d FROM stages in %s, found %d: %v", len(df.stages), df.filename, len(fromStages), fromStages)

			for i, expectedImg := range df.stages {
				assert.Contains(t, fromStages[i], expectedImg,
					"stage %d in %s should use image %s", i+1, df.filename, expectedImg)
			}
		})
	}
}

// TestDockerfiles_SecurityNonRootUser enforces DevSecOps rule: final runtime stage MUST run as non-root user.
func TestDockerfiles_SecurityNonRootUser(t *testing.T) {
	root := getRepoRoot(t)

	for _, df := range allDockerfiles {
		if df.isSymlink {
			continue
		}
		t.Run(df.filename, func(t *testing.T) {
			lines := readDockerfileLines(t, filepath.Join(root, df.filename))
			var lastUser string
			for _, line := range lines {
				trimmed := strings.TrimSpace(line)
				if strings.HasPrefix(strings.ToUpper(trimmed), "USER ") {
					lastUser = strings.TrimSpace(trimmed[5:])
				}
			}

			require.NotEmpty(t, lastUser, "%s must specify a USER instruction", df.filename)
			assert.Equal(t, df.runtimeUser, lastUser, "%s runtime USER mismatch", df.filename)
			assert.NotEqual(t, "root", strings.ToLower(lastUser), "%s cannot run as root", df.filename)
			assert.NotEqual(t, "0", lastUser, "%s cannot run as UID 0", df.filename)
		})
	}
}

// TestDockerfiles_SecurityAptHygiene verifies apt-get calls use --no-install-recommends and clean up /var/lib/apt/lists/*.
func TestDockerfiles_SecurityAptHygiene(t *testing.T) {
	root := getRepoRoot(t)

	for _, df := range allDockerfiles {
		if df.isSymlink {
			continue
		}
		t.Run(df.filename, func(t *testing.T) {
			content, err := os.ReadFile(filepath.Join(root, df.filename))
			require.NoError(t, err)
			sContent := string(content)

			if strings.Contains(sContent, "apt-get install") {
				assert.Contains(t, sContent, "--no-install-recommends",
					"%s: apt-get install must include --no-install-recommends", df.filename)
				assert.Contains(t, sContent, "rm -rf /var/lib/apt/lists/*",
					"%s: apt-get install must clean up /var/lib/apt/lists/*", df.filename)
			}
		})
	}
}

// TestDockerfiles_SecurityNoHardcodedSecrets verifies no API keys or plaintext credentials exist.
func TestDockerfiles_SecurityNoHardcodedSecrets(t *testing.T) {
	root := getRepoRoot(t)
	forbiddenPatterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)(api[_-]?key|secret|password|token)\s*=\s*['"][a-zA-Z0-9_\-]{8,}['"]`),
		regexp.MustCompile(`(?i)BEGIN\s+(RSA|OPENSSH|EC|DSA)?\s*PRIVATE\s+KEY`),
		regexp.MustCompile(`(?i)ghp_[a-zA-Z0-9]{36}`),
		regexp.MustCompile(`(?i)xox[baprs]-[0-9a-zA-Z]{10,}`),
	}

	for _, df := range allDockerfiles {
		if df.isSymlink {
			continue
		}
		t.Run(df.filename, func(t *testing.T) {
			content, err := os.ReadFile(filepath.Join(root, df.filename))
			require.NoError(t, err)
			for _, pattern := range forbiddenPatterns {
				assert.False(t, pattern.Match(content),
					"%s matched suspicious secret pattern: %s", df.filename, pattern.String())
			}
		})
	}
}

// TestDockerfiles_ExposeStandardPorts ensures standard Longbow ports (3000, 3001, 9090) are exposed.
func TestDockerfiles_ExposeStandardPorts(t *testing.T) {
	root := getRepoRoot(t)

	for _, df := range allDockerfiles {
		if df.isSymlink {
			continue
		}
		t.Run(df.filename, func(t *testing.T) {
			lines := readDockerfileLines(t, filepath.Join(root, df.filename))
			var exposed []string
			for _, line := range lines {
				trimmed := strings.TrimSpace(line)
				if strings.HasPrefix(strings.ToUpper(trimmed), "EXPOSE ") {
					ports := strings.Fields(trimmed[7:])
					exposed = append(exposed, ports...)
				}
			}

			for _, expectedPort := range df.exposedPorts {
				assert.Contains(t, exposed, expectedPort,
					"%s should expose port %s", df.filename, expectedPort)
			}
		})
	}
}

// TestDockerfiles_VolumeMountData ensures VOLUME /data is declared for persistent vector storage.
func TestDockerfiles_VolumeMountData(t *testing.T) {
	root := getRepoRoot(t)

	for _, df := range allDockerfiles {
		if df.isSymlink {
			continue
		}
		t.Run(df.filename, func(t *testing.T) {
			content, err := os.ReadFile(filepath.Join(root, df.filename))
			require.NoError(t, err)
			hasVolume := strings.Contains(string(content), "VOLUME /data")
			assert.Equal(t, df.hasVolumeData, hasVolume,
				"%s volume /data declaration mismatch", df.filename)
		})
	}
}

// TestDockerfiles_BuildFlagsAndTags verifies reproducible Go build flags (-trimpath, -buildvcs=false, -mod=vendor).
func TestDockerfiles_BuildFlagsAndTags(t *testing.T) {
	root := getRepoRoot(t)

	for _, df := range allDockerfiles {
		if df.isSymlink || df.filename == "Dockerfile.metal" {
			continue // Dockerfile.metal uses pre-built binaries
		}
		t.Run(df.filename, func(t *testing.T) {
			content, err := os.ReadFile(filepath.Join(root, df.filename))
			require.NoError(t, err)
			sContent := string(content)

			assert.Contains(t, sContent, "-trimpath", "%s should use -trimpath for reproducible builds", df.filename)
			assert.Contains(t, sContent, "-buildvcs=false", "%s should use -buildvcs=false", df.filename)
			assert.Contains(t, sContent, "-mod=vendor", "%s should use -mod=vendor", df.filename)
			assert.Contains(t, sContent, "-ldflags=\"-s -w", "%s should strip debug symbols with -s -w", df.filename)

			for _, tag := range df.expectedTags {
				assert.Contains(t, sContent, tag, "%s should specify expected tag %s", df.filename, tag)
			}
		})
	}
}

// TestDockerfiles_RuntimeEnvironmentVariables verifies memory and dispatch env settings.
func TestDockerfiles_RuntimeEnvironmentVariables(t *testing.T) {
	root := getRepoRoot(t)

	for _, df := range allDockerfiles {
		if df.isSymlink {
			continue
		}
		t.Run(df.filename, func(t *testing.T) {
			content, err := os.ReadFile(filepath.Join(root, df.filename))
			require.NoError(t, err)
			sContent := string(content)

			for k, v := range df.envVars {
				expectedEnv := k + "=" + v
				assert.Contains(t, sContent, expectedEnv,
					"%s should configure runtime environment variable %s", df.filename, expectedEnv)
			}
		})
	}
}

// TestDockerfiles_Entrypoint verifies valid JSON array entrypoint.
func TestDockerfiles_Entrypoint(t *testing.T) {
	root := getRepoRoot(t)

	for _, df := range allDockerfiles {
		if df.isSymlink {
			continue
		}
		t.Run(df.filename, func(t *testing.T) {
			lines := readDockerfileLines(t, filepath.Join(root, df.filename))
			var entrypointLine string
			for _, line := range lines {
				trimmed := strings.TrimSpace(line)
				if strings.HasPrefix(strings.ToUpper(trimmed), "ENTRYPOINT ") {
					entrypointLine = strings.TrimSpace(trimmed[11:])
				}
			}

			require.NotEmpty(t, entrypointLine, "%s must declare ENTRYPOINT", df.filename)
			assert.Equal(t, df.entrypoint, entrypointLine, "%s entrypoint mismatch", df.filename)
			assert.True(t, strings.HasPrefix(entrypointLine, "[") && strings.HasSuffix(entrypointLine, "]"),
				"%s ENTRYPOINT should use JSON exec array format: %s", df.filename, entrypointLine)
		})
	}
}

// TestDockerfiles_Healthchecks ensures GPU images have healthchecks.
func TestDockerfiles_Healthchecks(t *testing.T) {
	root := getRepoRoot(t)

	for _, df := range allDockerfiles {
		if df.isSymlink {
			continue
		}
		t.Run(df.filename, func(t *testing.T) {
			content, err := os.ReadFile(filepath.Join(root, df.filename))
			require.NoError(t, err)
			sContent := string(content)

			hasHealth := strings.Contains(sContent, "HEALTHCHECK")
			assert.Equal(t, df.hasHealthCheck, hasHealth,
				"%s HEALTHCHECK declaration mismatch", df.filename)
			if df.hasHealthCheck {
				assert.Contains(t, sContent, "http://localhost:9090/health",
					"%s HEALTHCHECK should probe metrics/health port 9090", df.filename)
			}
		})
	}
}

// TestDockerfiles_BuildKitCheck runs `docker build --check -f <df> .` if docker is installed.
func TestDockerfiles_BuildKitCheck(t *testing.T) {
	dockerPath, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("docker CLI not available in PATH, skipping BuildKit syntax check")
	}

	root := getRepoRoot(t)

	for _, df := range allDockerfiles {
		if df.isSymlink {
			continue
		}
		t.Run(df.filename, func(t *testing.T) {
			cmd := exec.Command(dockerPath, "build", "--check", "-f", df.filename, ".")
			cmd.Dir = root
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "docker build --check failed for %s: %s", df.filename, string(out))
			assert.Contains(t, string(out), "Check complete, no warnings found",
				"docker build --check for %s should have zero warnings: %s", df.filename, string(out))
		})
	}
}
