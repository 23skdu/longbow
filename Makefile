# Longbow Makefile
# Build, security, memory, static analysis, testing, and observability targets.

SHELL := /bin/bash

# Toolchain configuration (prefer Homebrew Go 1.27.3 and Homebrew / local tools)
GO ?= $(shell which /home/linuxbrew/.linuxbrew/bin/go 2>/dev/null || which go)
GOPATH ?= $(shell $(GO) env GOPATH)
GOBIN ?= $(GOPATH)/bin
export PATH := /home/linuxbrew/.linuxbrew/bin:$(GOBIN):$(PATH)

GOLANGCI_LINT ?= $(shell which golangci-lint 2>/dev/null || echo /home/linuxbrew/.linuxbrew/bin/golangci-lint)
GOSEC         ?= $(shell which gosec 2>/dev/null || echo /home/linuxbrew/.linuxbrew/bin/gosec)
GOVULNCHECK   ?= $(shell which govulncheck 2>/dev/null || echo $(GOBIN)/govulncheck)
DEADCODE      ?= $(shell which deadcode 2>/dev/null || echo $(GOBIN)/deadcode)
GOIMPORTS     ?= $(shell which goimports 2>/dev/null || echo $(GOBIN)/goimports)
VALGRIND      ?= $(shell which valgrind 2>/dev/null || echo /usr/bin/valgrind)
CLANG         ?= $(shell which clang 2>/dev/null || echo /home/linuxbrew/.linuxbrew/bin/clang)
HELM          ?= $(shell which helm 2>/dev/null || echo /home/linuxbrew/.linuxbrew/bin/helm)
MARKDOWNLINT  ?= $(shell which markdownlint 2>/dev/null || echo /home/linuxbrew/.linuxbrew/bin/markdownlint)
PYTHON        ?= $(shell which python3 2>/dev/null || echo /home/linuxbrew/.linuxbrew/bin/python3)

.PHONY: all help ci build build-release build-debug build-race build-adbc build-bench build-verify clean \
	deps deps-upgrade lint lint-fast lint-fix vet staticcheck deadcode fmt fmt-check \
	security sec-gosec sec-vulncheck sec-audit \
	mem-bench mem-escape mem-profile mem-asan mem-valgrind \
	test test-short test-all race test-coverage coverage-html test-fuzz test-docker \
	test-adbc test-adbc-python test-adbc-so \
	cross-compile check-simd test-bench-harness helm-lint markdown-lint bench-ci bench-ab \
	accuracy-gate oracle-gate observability-inventory observability-check observability-rules \
	install-commit-hook install-hooks pre-commit

# Default target
all: build build-adbc

## help: Print available targets with descriptions
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Combined Local CI Pipeline:"
	@echo "  ci                   Run the complete combined CI verification pipeline locally"
	@echo ""
	@echo "Build Targets:"
	@echo "  build                Compile all packages"
	@echo "  build-release        Build release binaries with stripped debug symbols (-s -w)"
	@echo "  build-debug          Build with optimizations and inlining disabled for debugging (-N -l)"
	@echo "  build-race           Compile all packages with race detector enabled"
	@echo "  build-adbc           Build ADBC C-shared library (liblongbow_adbc.so)"
	@echo "  build-bench          Build bench-tool CLI binary"
	@echo "  build-verify         Build tensor-verify CLI binary"
	@echo "  cross-compile        Verify ARM64 cross-compilation (build & vet)"
	@echo "  clean                Remove compiled binaries, C-shared headers, and test artifacts"
	@echo "  deps                 Download and tidy Go modules and sync vendor directory"
	@echo "  deps-upgrade         Upgrade all Go modules, tidy go.mod, and sync vendor"
	@echo ""
	@echo "Static Analysis & Linting:"
	@echo "  lint                 Run golangci-lint across all packages"
	@echo "  lint-fast            Run fast linters only"
	@echo "  lint-fix             Run golangci-lint with automated fixes"
	@echo "  vet                  Run go vet standard analyzer"
	@echo "  staticcheck          Run staticcheck and unused analyzers"
	@echo "  deadcode             Run deadcode analyzer for unreachable functions"
	@echo "  fmt                  Format code with gofmt and goimports"
	@echo "  fmt-check            Verify formatting without modifying files"
	@echo "  markdown-lint        Run markdown linting on documentation"
	@echo ""
	@echo "Security Analysis:"
	@echo "  security             Run full security audit (gosec + govulncheck)"
	@echo "  sec-gosec            Run gosec AST security analysis"
	@echo "  sec-vulncheck        Run govulncheck against Go vulnerability database"
	@echo "  sec-audit            Audit dependencies for known CVEs and vulnerabilities"
	@echo ""
	@echo "Memory & Profiling Analysis:"
	@echo "  mem-bench            Run memory allocation benchmarks (-benchmem)"
	@echo "  mem-escape           Analyze compiler escape analysis and inlining decisions"
	@echo "  mem-profile          Run tests with memory and CPU profiling enabled"
	@echo "  mem-asan             Run memory sanitizer tests with Clang AddressSanitizer"
	@echo "  mem-valgrind         Run ADBC shared library tests under Valgrind"
	@echo ""
	@echo "Testing & Quality Gates:"
	@echo "  test                 Run unit tests in short mode (fast, bounded memory)"
	@echo "  test-short           Alias for test"
	@echo "  test-all             Run full test suite across all packages"
	@echo "  race                 Run tests under Go race detector in short mode"
	@echo "  test-coverage        Verify 100% package test presence and collect statement coverage"
	@echo "  coverage-html        Generate interactive HTML coverage report (coverage.html)"
	@echo "  test-fuzz            Run fuzz test harnesses"
	@echo "  test-docker          Run Dockerfile unit tests (Go and Python)"
	@echo "  test-bench-harness   Run benchmark harness test suite (114 Python tests)"
	@echo "  test-adbc            Run ADBC driver Go test suite"
	@echo "  test-adbc-python     Run ADBC Python driver verification script"
	@echo "  test-adbc-so         Run ADBC C-shared library Python tests"
	@echo "  accuracy-gate        Verify numerical precision tolerances across 15 vector dtypes"
	@echo "  oracle-gate          Verify ground-truth mathematical parity vs reference oracles"
	@echo "  observability-check  Verify metric declarations and alert rule bindings"
	@echo "  helm-lint            Lint and dry-run template the Helm chart"
	@echo "  bench-ci             Run fast Unified Benchmark in CI mode"
	@echo "  check-simd           Verify generated SIMD kernels are byte-for-byte reproducible"
	@echo "  pre-commit           Run full pre-commit gate suite"
	@echo "  install-hooks        Install Git pre-commit hooks"

# ==============================================================================
# Combined Local CI Pipeline
# ==============================================================================

ci: fmt-check lint vet build build-adbc cross-compile check-simd test-coverage test-docker test-bench-harness accuracy-gate oracle-gate observability-check observability-rules security helm-lint
	@echo "=========================================================="
	@echo " ✓ Complete local CI verification pipeline passed 100%!"
	@echo "=========================================================="

# ==============================================================================
# Build Targets
# ==============================================================================

build:
	@echo "Building all packages..."
	$(GO) build ./...

build-release:
	@echo "Building release artifacts..."
	$(GO) build -trimpath -ldflags="-s -w" ./...

build-debug:
	@echo "Building debug artifacts (no inlining, debug symbols)..."
	$(GO) build -gcflags="all=-N -l" ./...

build-race:
	@echo "Building all packages with race detector enabled..."
	$(GO) build -race ./...

build-adbc:
	@echo "Building ADBC driver as C-shared library..."
	$(GO) build -buildmode=c-shared -o liblongbow_adbc.so ./cmd/adbc

build-bench:
	@echo "Building bench-tool..."
	@mkdir -p bin
	$(GO) build -o bin/bench-tool ./cmd/bench-tool

build-verify:
	@echo "Building tensor-verify..."
	@mkdir -p bin
	$(GO) build -o bin/tensor-verify ./cmd/tensor-verify

cross-compile:
	@echo "Verifying ARM64 cross-compilation..."
	GOARCH=arm64 $(GO) build ./...
	GOARCH=arm64 $(GO) vet ./...
	@echo "ARM64 cross-compiles cleanly."

check-simd:
	@echo "Verifying SIMD generation is byte-for-byte reproducible..."
	./scripts/check_simd_generation.sh

deps:
	@echo "Resolving dependencies..."
	$(GO) mod download
	$(GO) mod tidy
	$(GO) mod vendor

deps-upgrade:
	@echo "Upgrading Go dependencies..."
	$(GO) get -u ./...
	$(GO) mod tidy
	$(GO) mod vendor

clean:
	@echo "Cleaning build and test artifacts..."
	rm -f liblongbow_adbc.so liblongbow_adbc.h coverage.out coverage.html mem.out cpu.out
	rm -rf bin/
	$(GO) clean -testcache

# ==============================================================================
# Static Analysis & Linters
# ==============================================================================

lint:
	@echo "Running golangci-lint..."
	$(GOLANGCI_LINT) run ./...

lint-fast:
	@echo "Running fast linters..."
	$(GOLANGCI_LINT) run --fast ./...

lint-fix:
	@echo "Running golangci-lint with automated fixes..."
	$(GOLANGCI_LINT) run --fix ./...

vet:
	@echo "Running go vet..."
	$(GO) vet ./...

staticcheck:
	@echo "Running staticcheck and unused analyzers..."
	$(GOLANGCI_LINT) run -E staticcheck,unused ./...

deadcode:
	@echo "Running deadcode reachability analysis..."
	$(DEADCODE) ./cmd/... ./internal/...

fmt:
	@echo "Formatting Go source files..."
	gofmt -s -w .
	$(GOIMPORTS) -w .

fmt-check:
	@echo "Checking code formatting..."
	@DIFF=$$(gofmt -l .); \
	if [ -n "$$DIFF" ]; then \
		echo "Unformatted files found:"; \
		echo "$$DIFF"; \
		exit 1; \
	fi
	@echo "All files formatted cleanly."

markdown-lint:
	@echo "Running markdown linting on documentation..."
	-$(MARKDOWNLINT) -c .markdownlint-cli2.jsonc docs/**/*.md README.md CONTRIBUTING.md

# ==============================================================================
# Security Analysis
# ==============================================================================

security: sec-gosec sec-vulncheck
	@echo "Security analysis passed cleanly."

GOSEC_FLAGS ?= -quiet

sec-gosec:
	@echo "Running gosec AST security analysis..."
	$(GOSEC) $(GOSEC_FLAGS) ./...

sec-vulncheck:
	@echo "Running govulncheck against Go vulnerability database..."
	$(GOVULNCHECK) ./...

sec-audit: sec-vulncheck
	@echo "Auditing module dependencies..."
	$(GO) list -m all > /dev/null

# ==============================================================================
# Memory & Profiling Analysis
# ==============================================================================

mem-bench:
	@echo "Running memory allocation benchmarks..."
	$(GO) test -short -run='^$$' -bench=. -benchmem ./internal/store/... ./internal/simd/... ./internal/memory/...

mem-escape:
	@echo "Analyzing compiler escape analysis for memory package..."
	$(GO) build -gcflags="-m -m" ./internal/memory/...

mem-profile:
	@echo "Running profiled tests..."
	$(GO) test -short -run='^Test' -memprofile=mem.out -cpuprofile=cpu.out ./internal/store/index/
	@echo "Profiles written to mem.out and cpu.out. Use '$(GO) tool pprof mem.out' to inspect."

mem-asan:
	@echo "Running memory sanitizer (AddressSanitizer) with Clang..."
	CC=$(CLANG) $(GO) test -asan -short ./internal/memory/...

mem-valgrind: build-adbc
	@echo "Running ADBC Python integration tests under Valgrind..."
	$(VALGRIND) --leak-check=full --error-exitcode=1 python3 scripts/verify_driver.py

# ==============================================================================
# Testing & Quality Gates
# ==============================================================================

test: test-short

test-short:
	@echo "Running unit tests in short mode (bounded memory)..."
	$(GO) test -short ./...

test-all:
	@echo "Running all unit tests..."
	$(GO) test -v ./...

race:
	@echo "Running race detector in short mode (prevent OOM)..."
	$(GO) test -race -short ./...

test-coverage:
	@echo "Verifying 100% Package Test Coverage Gate..."
	@UNTESTED=$$($(GO) test -list '.*' ./... 2>&1 | grep '\[no test files\]' || true); \
	if [ -n "$$UNTESTED" ]; then \
		echo "Found packages without test files:"; \
		echo "$$UNTESTED"; \
		exit 1; \
	fi
	@echo "All packages have active test coverage."
	$(GO) test -short -cover ./...

coverage-html:
	@echo "Generating interactive HTML coverage report..."
	$(GO) test -short -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report saved to coverage.html"

test-fuzz:
	@echo "Running fuzz tests in short mode..."
	$(GO) test -short -run='^Fuzz' ./...

test-docker:
	@echo "Running Dockerfile unit tests (Go & Python)..."
	$(GO) test -v ./tests/docker/...
	$(PYTHON) tests/docker/test_dockerfiles.py

test-bench-harness:
	@echo "Running benchmark harness test suite..."
	$(PYTHON) scripts/tests/test_benchmark_validation.py
	$(PYTHON) scripts/tests/test_benchmark_memory.py
	$(PYTHON) scripts/tests/test_benchmark_temporal.py
	$(PYTHON) scripts/tests/test_benchmark_attribution.py
	$(PYTHON) scripts/tests/test_ab_stats.py

test-adbc:
	$(GO) test -v -cover ./internal/adbc/...

test-adbc-python: build-adbc
	$(PYTHON) scripts/verify_driver.py

test-adbc-so: build-adbc
	$(PYTHON) tests/adbc/test_driver_so.py

accuracy-gate:
	@echo "Running Accuracy Gate (numerical precision across 15 vector dtypes)..."
	$(GO) test -v ./internal/simd/ -run='^TestSIMDAccuracy'

oracle-gate:
	@echo "Running Oracle Gate (ground-truth parity vs reference oracles)..."
	$(GO) test -v ./internal/simd/... ./internal/tensor/... ./internal/memory/... ./internal/store/index/ -run='Parity'

# Observability conformance (docs/roadmap.md Items 18-20, 24)
observability-inventory:
	$(GO) run ./tools/metrics-inventory > internal/observability/testdata/registered_metrics.tsv

observability-check: observability-inventory
	$(GO) test ./internal/observability/ -count=1

observability-rules:
	grafana/tests/test-rules.sh

helm-lint:
	@echo "Linting Helm chart..."
	$(HELM) lint helm/longbow
	$(HELM) template longbow helm/longbow --debug > /dev/null
	@echo "Helm chart valid."

bench-ci: build
	@echo "Running fast Unified Benchmark in CI mode..."
	$(PYTHON) scripts/unified_benchmark.py --ci --runs 3 --compare-baseline benchmarks/baseline_cpu.json --threshold 10

bench-ab:
	@echo "Running interleaved A/B benchmark verification..."
	$(PYTHON) scripts/check_index_regression.py --help > /dev/null

# Git pre-commit hook installation and quality gates
install-commit-hook:
	@echo "Installing pre-commit hook..."
	@mkdir -p .git/hooks
	@ln -sf ../../scripts/pre-commit.sh .git/hooks/pre-commit || cp scripts/pre-commit.sh .git/hooks/pre-commit
	@chmod +x scripts/pre-commit.sh .git/hooks/pre-commit
	@echo "Pre-commit hook installed successfully into .git/hooks/pre-commit."

install-hooks: install-commit-hook

pre-commit:
	@./scripts/pre-commit.sh
